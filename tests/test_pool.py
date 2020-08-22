import os
import time
from fiber import Pool, SimpleQueue
import fiber
import pytest
import docker
import threading
import random

import fiber.config as fiber_config


@pytest.fixture(scope="module")
def client():
    return docker.from_env()


def f(x):
    return x * x

def f2(x, y):
    return x * y

def fy(x, y=1):
    return x * x * y


def get_proc_name(x):
    import multiprocessing as mp
    proc = mp.current_process()
    return proc.name


def double_queue_worker(args):
    i, (instructions, returns) = args
    print('[worker]LOOPING', i)
    # instructions, returns = SUBPROC_QUEUES[i]
    assert instructions is not None
    assert returns is not None
    returns.put('READY {}'.format(i))
    print('[worker]PUT READY', i)

    while True:
        # quit after got one instruction. This makes sure each worker gets at
        # least one instruction.
        ins = instructions.get()
        print('[worker]GOT', ins, i)
        if ins == "QUIT":
            #print('[worker]got QUIT exiting', i)
            break

        returns.put("ACK {}".format(i))
        print('[worker]PUT ACK', i)


def sleep_worker(duration):
    time.sleep(duration)


def random_error_worker(i):
    random.seed(time.time())
    time.sleep(0.01)

    p = random.random()
    if p < 0.05:
        raise ValueError

    return i

def is_inside(p):
    x, y = random.random(), random.random()
    return x * x + y * y < 1

class TestPool():
    @pytest.fixture(autouse=True)
    def check_leak(self, request, client):
        assert fiber.active_children() == []
        if fiber.config.default_backend != "docker":
            yield
            return
        pre_workers = client.containers.list()
        yield
        post_workers = client.containers.list()
        assert pre_workers == post_workers

    def test_pool_basic(self):
        pool = Pool(2)
        res = pool.map(f, [1, 2, 3])
        pool.terminate()
        pool.join()
        assert res == [1, 4, 9]

    def test_pool_more(self):
        pool = Pool(4)

        # explicitly start workers instead of lazy start
        pool.start_workers()
        res = pool.map(f, [i for i in range(1000)])

        pool.wait_until_workers_up()

        pool.terminate()
        pool.join()
        assert res == [i**2 for i in range(1000)]

    def test_pool_apply(self):
        pool = Pool(4)
        res_async = pool.apply_async(f, (42,))
        r = res_async.get()
        assert r == (42 * 42)

        res = pool.apply(f, (36,))
        assert res == (36 * 36)

        res = pool.apply(fy, (36,), {"y": 2})
        assert res == (36 * 36 * 2)

        pool.terminate()
        pool.join()

    def test_pool_imap(self):
        pool = Pool(4)
        res_iter = pool.imap(f, [x for x in range(100)], 1)
        res = list(res_iter)
        assert res == [x * x for x in range(100)]

        res_iter = pool.imap_unordered(f, [x for x in range(100)], 1)
        res = list(res_iter)
        assert len(res) == 100
        res.sort()
        assert res == [x * x for x in range(100)]

        pool.terminate()
        pool.join()

    def test_pool_starmap(self):
        pool = Pool(4)
        res = pool.starmap(f, [(x,) for x in range(100)], 1)
        assert res == [x * x for x in range(100)]

        async_res = pool.starmap_async(f, [(x,) for x in range(100)], 1)
        res = async_res.get()
        assert res == [x * x for x in range(100)]

        pool.terminate()
        pool.join()

    def test_pool_starmap2(self):
        pool = Pool(4)
        res = pool.starmap(f2, [(x, x) for x in range(100)], 10)
        assert res == [x * x for x in range(100)]

        async_res = pool.starmap_async(f, [(x,) for x in range(100)], 10)
        res = async_res.get()
        assert res == [x * x for x in range(100)]

        pool.terminate()
        pool.join()

    def test_pool_multiple_workers_inside_one_job(self):
        old_val = fiber_config.cpu_per_job
        try:
            fiber_config.cpu_per_job = 2
            pool = Pool(4)
            # explicitly start workers instead of lazy start
            pool.start_workers()
            # wait for all the workers to start
            pool.wait_until_workers_up()

            res = pool.map(get_proc_name, [i for i in range(4)], chunksize=1)
            pool.terminate()
            pool.join()
            res.sort()
            # Two `ForkProcess-1` from 2 jobs, two `ForkProcess-2` from the first job
            assert res == ['ForkProcess-1', 'ForkProcess-1', 'ForkProcess-2', 'ForkProcess-2'], res
        finally:
            fiber_config.cpu_per_job = old_val

    def test_chunk_size(self):
        log_file = os.path.join(os.path.dirname(__file__), "..", "logs", "chunk.log")
        try:
            fiber.reset()
            log_file = fiber_config.log_file
            log_level = fiber_config.log_level

            fiber.init(cpu_per_job=8, log_file=log_file, log_level="debug")

            n_envs = 9
            queues = [(SimpleQueue(), SimpleQueue()) for _ in range(n_envs)]
            pool = Pool(n_envs)

            # explicitly start workers instead of lazy start
            pool.start_workers()
            print("waiting for all workers to be up")
            # wait some time for workers to start
            pool.wait_until_workers_up()
            #time.sleep(20)
            print("all workers are up")

            def run_map():
                print('[master]RUN MAP')
                # Not setting chunk size 1 here, if chunk size is calculated
                # wrong,  map will get stuck
                pool.map(double_queue_worker, enumerate(queues), chunksize=1)
                print('[master]RUN MAP DONE')

            td = threading.Thread(target=run_map, daemon=True)
            td.start()

            print('Checking...')
            for i, (_, returns) in enumerate(queues):
                print('[master]Checking queue', i, n_envs)
                assert 'READY' in returns.get()
                print(f'[master]Checking queue {i} done')

            print('All workers are ready, put HELLOs')
            for i, (instruction, _) in enumerate(queues):
                instruction.put("HELLO")
                print(f'[master]PUT HELLO {i}')

            print('All HELLOs sent, waiting for ACKs')
            for i, (_, returns) in enumerate(queues):
                assert 'ACK' in returns.get()
                print(f'[master]GOT ACK {i}')

            print('All ACKs sent, send QUIT to workers')
            for i, (instruction, _) in enumerate(queues):
                instruction.put("QUIT")
                print(f'[master]PUT QUIT {i}, {n_envs}')
            pool.terminate()
            pool.join()

        finally:
            fiber.init(log_file=log_file, log_level=log_level)

    def test_pool_close(self):
        pool = Pool(2)
        res = pool.map(f, [1, 2, 3])
        assert res == [1, 4, 9]
        pool.close()

        with pytest.raises(ValueError):
            pool.map(f, [1, 2, 3])

        pool.join()

    def test_many_jobs(self):
        """
        This is to test a race condition in handling data in pending table
        """
        workers = 5
        pool = fiber.Pool(workers)
        tasks = 5000
        duration = 0.001

        # explicitly start workers instead of lazy start
        pool.start_workers()

        pool.wait_until_workers_up()

        res = [None] * workers
        for i in range(tasks // workers):
            for j in range(workers):
                handle = pool.apply_async(sleep_worker, (duration,))
                res[j] = handle
            for j in range(workers):
                res[j].get()

        pool.terminate()
        pool.join()

    def test_pi_estimation(self):
        pool = Pool(processes=4)
        NUM_SAMPLES = int(1e6)
        pi = 4.0 * sum(pool.map(is_inside, range(0, NUM_SAMPLES))) / NUM_SAMPLES
        assert 3 < pi and pi < 4
        print("Pi is roughly {}".format(pi))

        pool.terminate()
        pool.join()

    def test_error_handling(self):
        try:
            pool = fiber.Pool(3, error_handling=True)
            # explicitly start workers instead of lazy start
            pool.start_workers()
            pool.wait_until_workers_up()
            res = pool.map(
                random_error_worker, [i for i in range(300)], chunksize=1
            )

            assert res == [i for i in range(300)]

        finally:
            pool.terminate()
            pool.join()

    def test_error_handling_unordered(self):
        try:
            pool = fiber.Pool(3, error_handling=True)
            # explicitly start workers instead of lazy start
            pool.start_workers()
            pool.wait_until_workers_up()
            res_iter = pool.imap_unordered(
                random_error_worker, [i for i in range(300)], chunksize=1
            )

            res = list(res_iter)
            res.sort()

            assert res == [i for i in range(300)]

        finally:
            pool.terminate()
            pool.join()

    def test_pool_with_no_argument(self):
        # Make sure no exception is raised
        p = fiber.Pool()
        p.map(print, [1, 2, 3, 4])
        p.terminate()
        p.join()

        assert 1 == 1
