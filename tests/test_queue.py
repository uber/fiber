import fiber
import pickle
import time
import pytest
import docker
import collections
import multiprocessing as mp
from fiber.queues import Pipe, SimpleQueue
from fiber.socket import Socket


@pytest.fixture(scope="module")
def client():
    return docker.from_env()


def write_pipe(pipe, msg):
    pipe.send(msg)


def put_queue(q, data):
    print("put_queue started")
    if type(data) is list:
        for d in data:
            q.put(d)
    else:
        q.put(data)
    # make sure all data are flushed out
    time.sleep(1)
    print("put_queue done")


def get_queue(q_in, q_out, n):
    print("get_queue started", q_in, q_in.reader, q_in.writer)
    for i in range(n):
        d = q_in.get()
        q_out.put(d)
    time.sleep(1)
    print("get_queue finished")


def worker(q_in, q_out, ident):
    data = None
    while True:
        data = q_in.get()
        if data == "quit":
            break
        q_out.put(ident)


def pipe_worker(conn):
    d = conn.recv()
    print("[pipe_worker] got command", d)
    conn.send(b"ack")
    print("[pipe_worker] ack")


class TestZconnection():

    def test_zconn_with_mp(self):
        mp.Process()
        pass

def pipe_mp_worker(writer):
    import multiprocessing as mp
    ctx = mp.get_context('spawn')
    p = ctx.Process(target=write_pipe, args=(writer, b"hello"), name="WRITE_PIPE")
    p.start()

class TestQueue():

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

    def test_pipe(self):
        reader, writer = Pipe()

        writer.send(b"hello")
        data = reader.recv()
        assert data == b"hello"

    def test_sharing_pipe_with_multiprocessing(self):
        reader, writer = Pipe()
        # The contaxt is 'spawn' because fork based mp won't trigger ZConnection's
        # initialization.
        ctx = mp.get_context('spawn')
        p = ctx.Process(target=write_pipe, args=(writer, b"hello"))
        p.start()
        msg = reader.recv()
        p.join()
        assert msg == b"hello"

    def test_subprocess_with_pipe(self):
        reader, writer = Pipe()
        p = fiber.Process(target=write_pipe, args=(writer, b"fiber pipe"))
        p.start()
        msg = reader.recv()
        p.join()
        assert msg == b"fiber pipe"

    def test_pipe_duplex1(self):
        conn1, conn2 = Pipe(duplex=True)

        conn1.send(b"hello")
        data = conn2.recv()
        assert data == b"hello"

    def test_pipe_duplex2(self):
        conn1, conn2 = Pipe(duplex=True)

        conn2.send(b"hi")
        data = conn1.recv()
        assert data == b"hi"

    def test_pipe_duplex_over_fiber_process(self):
        conn1, conn2 = Pipe(duplex=True)
        p = fiber.Process(target=pipe_worker, args=(conn2,))
        p.start()
        conn1.send(b"hello")
        data = conn1.recv()
        p.join()
        assert data == b"ack"

    def test_sharing_pipe_with_fiber_and_multiprocessing(self):
        reader, writer = Pipe()

        p = fiber.Process(target=pipe_mp_worker, args=(writer,))
        p.start()
        msg = reader.recv()
        p.join()
        assert msg == b"hello"

    def test_simple_queue(self):
        q = SimpleQueue()
        ctx = mp.get_context('spawn')
        p = ctx.Process(target=put_queue, args=(q, 10))
        p.start()
        p.join()
        data = q.get()
        assert data == 10

    def test_simple_queue_fiber(self):
        q = SimpleQueue()
        p = fiber.Process(target=put_queue, args=(q, 10))
        p.start()
        p.join()
        data = q.get()
        assert data == 10

    def test_simple_queue_fiber2(self):
        q = SimpleQueue()
        n = 3
        procs = []
        try:
            for i in range(n):
                procs.append(fiber.Process(target=put_queue, args=(q, 10)))
            for p in procs:
                p.start()
            for p in procs:
                p.join()
            for p in procs:
                data = q.get()
                assert data == 10
        finally:
            for p in procs:
                p.join()

    def test_simple_queue_fiber_multi(self):
        # Read and write multiple objects
        n = 10
        q = SimpleQueue()
        p = fiber.Process(target=put_queue, args=(q, [i for i in range(n)]))
        p.start()
        p.join()
        for i in range(n):
            data = q.get()
            assert data == i

    def test_simple_queue_read_write_from_different_proc(self):
        # One process writew, another process reads and then writes to q_out.
        # Then master process reads from q_out.
        n = 10
        q = SimpleQueue()
        q_out = SimpleQueue()
        p1 = fiber.Process(target=put_queue, args=(q, [i for i in range(n)]))
        p2 = fiber.Process(target=get_queue, args=(q, q_out, n))
        p1.start()
        p2.start()
        for i in range(n):
            data = q_out.get()
            assert data == i
        p1.join()
        p2.join()

    @pytest.mark.skip(reason="both reader and writer are initialized to LazyZConnection")
    def test_queue_reader_is_none_by_default(self):
        # set reader to None so that de-serizlied Queue doesn't connect to
        # reader automatically. Fiber socket will faily queue all the messages
        # to all the readers. We want to prevent this behavior.

        # This is only needed by SimpleQueuePush
        q = fiber.queues.SimpleQueuePush()
        q.put(1)
        q.get()
        d = pickle.dumps(q)
        q2 = pickle.loads(d)
        assert q2.reader is None
        assert q2.writer is not None

    def test_queue_balance(self):
        # We only test SimpleQueuePush because SimpleQueuePull doesn't gurantee
        # balance.
        inqueue = fiber.queues.SimpleQueuePush()
        outqueue = fiber.queues.SimpleQueuePush()
        num_workers = 4
        multiplier = 600
        workers = []
        results = []
        for i in range(num_workers):
            print("create worker", i)
            p = fiber.Process(target=worker, args=(inqueue, outqueue, i), daemon=True)
            workers.append(p)
        for i in range(num_workers):
            workers[i].start()

        # wait for all workers to connect
        time.sleep(1)
        for i in range(num_workers * multiplier):
            inqueue.put("work")
        for i in range(num_workers * multiplier):
            results.append(outqueue.get())
        stats = collections.Counter(results)
        total = num_workers * multiplier
        # send singals to all workers
        for i in range(num_workers * multiplier):
            inqueue.put("quit")
        for i in range(num_workers):
            workers[i].join()
        for i in range(num_workers):
            #print("{}: {} {:.2f}".format(i, stats[i], stats[i] / float(total)))
            # data should be fairly queued
            assert stats[i] == 600
