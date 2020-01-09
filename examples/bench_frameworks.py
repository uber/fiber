# Benchmark Multiprocessing / Fiber / Pyspark / Ray with dummy tasks
#

import time
import fiber
import multiprocessing as mp
import logging
import subprocess
import argparse
from pprint import pprint


logger = logging.getLogger()


def sleep_worker(duration):
    import time
    time.sleep(duration)


def timeit(func, *args, **kwargs):
    start = time.time()
    res = func(*args, **kwargs)
    elapsed = time.time() - start

    return res, elapsed


def spawn_workers(n_workers):
    from ipyparallel.apps.ipengineapp import launch_new_instance
    pids = []
    import os
    for _ in range(n_workers):
        pid = os.fork()
        if pid == 0:
            launch_new_instance()
        else:
            pids.append(pid)

    #launch_new_instance()
    return pids


def bench_fiber(tasks, workers, task_duration, warmup=True, pool=None):
    if warmup:
        if not pool:
            pool = fiber.Pool(workers)
        pool.map(sleep_worker, [task_duration for x in range(tasks)],
                 chunksize=1)
        logger.debug("warm up finished")

    res, elapsed = timeit(
        pool.map, sleep_worker, [task_duration for x in range(tasks)],
        chunksize=1,
    )

    return elapsed


def bench_fiber_seq(tasks, workers, task_duration, warmup=True, pool=None):
    def run(pool, duration):
        res = [None] * workers
        for i in range(tasks // workers):
            for j in range(workers):
                handle = pool.apply_async(sleep_worker, (duration,))
                res[j] = handle
            for j in range(workers):
                res[j].get()

    if warmup:
        if not pool:
            pool = mp.Pool(workers)
        pool.map(sleep_worker, [task_duration for x in range(tasks)],
                 chunksize=1)
        logger.debug("warm up finished")

    res, elapsed = timeit(run, pool, task_duration)

    return elapsed


def bench_mp(tasks, workers, task_duration, warmup=True):
    logger.debug("benchmarking multiprocessing")
    pool = None
    if warmup:
        logger.debug("warming up")
        pool = mp.Pool(workers)
        pool.map(sleep_worker, [task_duration for x in range(tasks)],
                 chunksize=1)
        logger.debug("warm up finished")

    res, elapsed = timeit(
        pool.map, sleep_worker, [task_duration for x in range(tasks)],
        chunksize=1
    )

    return elapsed


def bench_mp_seq(tasks, workers, task_duration, warmup=True, pool=None):
    def run(pool, duration):
        res = [None] * workers
        for i in range(tasks // workers):
            for j in range(workers):
                handle = pool.apply_async(sleep_worker, (duration,))
                res[j] = handle
            for j in range(workers):
                res[j].get()

    if warmup:
        if not pool:
            pool = mp.Pool(workers)
        pool.map(sleep_worker, [task_duration for x in range(tasks)],
                 chunksize=1)
        logger.debug("warm up finished")

    res, elapsed = timeit(run, pool, task_duration)

    return elapsed


def pyspark_parallel(sc, tasks, task_duration):
    nums = sc.parallelize([task_duration for i in range(tasks)])
    nums.map(sleep_worker).collect()


def pyspark_parallel_seq(sc, tasks, task_duration, workers):
    for i in range(tasks // workers):
        nums = sc.parallelize([task_duration for i in range(workers)])
        nums.map(sleep_worker).collect()


def bench_spark(tasks, workers, task_duration, warmup=True, sc=None):

    if warmup:
        pyspark_parallel(sc, tasks, task_duration)

    res, elapsed = timeit(pyspark_parallel, sc, tasks, task_duration)

    return elapsed


def bench_spark_seq(tasks, workers, task_duration, warmup=True, sc=None):
    if warmup:
        pyspark_parallel(sc, tasks, task_duration)

    res, elapsed = timeit(pyspark_parallel_seq, sc, tasks,
                          task_duration, workers)

    return elapsed


def bench_ray(tasks, workers, task_duration, warmup=True):
    import ray

    @ray.remote
    def ray_sleep(duration):
        time.sleep(duration)

    if warmup:
        ray.get([ray_sleep.remote(task_duration) for x in range(tasks)])

    res, elapsed = timeit(
        ray.get, [ray_sleep.remote(task_duration) for x in range(tasks)]
    )
    return elapsed


def bench_ray_seq(tasks, workers, task_duration, warmup=True):
    import ray

    @ray.remote
    def ray_sleep(duration):
        time.sleep(duration)

    def ray_parallel_seq(tasks, workers):
        for i in range(tasks // workers):
            ray.get([ray_sleep.remote(task_duration) for x in range(workers)])

    if warmup:
        ray.get([ray_sleep.remote(task_duration) for x in range(tasks)])

    res, elapsed = timeit(
        ray_parallel_seq, tasks, workers
    )
    return elapsed


def bench_ipp_seq(tasks, workers, task_duration, warmup=True):
    from ipyparallel import Client

    rc = Client()
    #dview = rc[:]
    dview = rc.load_balanced_view()

    if warmup:
        dview.map_sync(sleep_worker, [task_duration for i in range(tasks)])

    dview.block = True

    def run(tasks):
        objs = [dview.apply_async(sleep_worker, task_duration) for i in range(tasks)]
        for task in objs:
            task.get()

    res, elapsed = timeit(
        run, tasks
    )
    return elapsed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'frameworks', nargs='+',
        choices=['mp', 'fiber', 'pyspark', 'ray', 'ipyparallel'],
        help='frameworks to benchmark'
    )
    parser.add_argument('-t', '--total-duration', type=int, default=1,
                        help='total running time')

    parser.add_argument('-d', '--task-duration', type=float, default=None,
                        choices=[0.001, 0.01, 0.1, 1],
                        help='task duration in ms')

    args = parser.parse_args()

    workers = 5

    max_duration = args.total_duration

    results = {}
    frameworks = args.frameworks

    for framework in frameworks:
        results[framework] = []
        results[framework + "_seq"] = []

    if "pyspark" in frameworks:
        from pyspark import SparkContext
        import pyspark

        sc = SparkContext()
        conf = pyspark.SparkConf().setAll([("spark.cores.max", 5)])
        sc.stop()
        sc = pyspark.SparkContext(conf=conf)

    if "ray" in frameworks:
        import ray

        ray.init()

    if "fiber" in frameworks:
        import fiber.pool
        fiber_pool = fiber.Pool(workers)

    if "ipyparallel" in frameworks:
        print("before popen")
        #ipp_controller = subprocess.Popen(["ipcontroller", "--ip", "*"])
        print("after popen")
        import atexit
        import signal
        import os
        #atexit.register(ipp_controller.kill)
        pids = spawn_workers(workers)
        for pid in pids:
            atexit.register(os.kill, pid, signal.SIGKILL)
        time.sleep(4)

    for i in range(4):
        factor = 10 ** i
        duration = 1 / factor
        if args.task_duration is not None:
            print(args.task_duration, duration, type(args.task_duration), type(duration))
            if args.task_duration != duration:
                continue
        tasks = int(max_duration * workers / duration)

        print(
            "Benchmarking {} workers with {} tasks each takes {} "
            "seconds".format(
                workers, tasks, duration
            )
        )

        # sequential tests (simulating RL)
        if "mp" in frameworks:
            elapsed = bench_mp_seq(tasks, workers, duration, True)
            results["mp_seq"].append({"task_duration": duration,
                                      "elapsed": elapsed})
            print("mp_seq", elapsed)

        if "fiber" in frameworks:
            elapsed = bench_fiber_seq(tasks, workers, duration, True,
                                      pool=fiber_pool)
            results["fiber_seq"].append({"task_duration": duration,
                                         "elapsed": elapsed})
            print("fiber_seq", elapsed)

        if "pyspark" in frameworks:
            elapsed = bench_spark_seq(tasks, workers, duration, warmup=True,
                                      sc=sc)
            results["pyspark_seq"].append({"task_duration": duration,
                                           "elapsed": elapsed})
            print("pyspark_seq", elapsed)

        if "ray" in frameworks:
            elapsed = bench_ray_seq(tasks, workers, duration, warmup=True)
            results["ray_seq"].append({"task_duration": duration,
                                       "elapsed": elapsed})
            print("ray_seq", elapsed)

        if "ipyparallel" in frameworks:
            elapsed = bench_ipp_seq(tasks, workers, duration, warmup=True)
            results["ipyparallel_seq"].append({"task_duration": duration,
                                               "elapsed": elapsed})
            print("ipyparallel_seq", elapsed)

        # batch tests (simulating ES)
        """
        if "mp" in frameworks:
            elapsed = bench_mp(tasks, workers, duration, True)
            results["mp"].append({"task_duration": duration,
                                  "elapsed": elapsed})
            print("mp", elapsed)

        if "fiber" in frameworks:
            elapsed = bench_fiber(tasks, workers, duration, True,
                                  pool=fiber_pool)
            results["fiber"].append({"task_duration": duration,
                                     "elapsed": elapsed})
            print("fiber", elapsed)

        if "pyspark" in frameworks:
            elapsed = bench_spark(tasks, workers, duration, warmup=True, sc=sc)
            results["pyspark"].append({"task_duration": duration,
                                       "elapsed": elapsed})
            print("pyspark", elapsed)

        if "ray" in frameworks:
            elapsed = bench_ray(tasks, workers, duration, warmup=True)
            results["ray"].append({"task_duration": duration,
                                   "elapsed": elapsed})
            print("ray", elapsed)
        """

    pprint(results)


if __name__ == "__main__":
    main()
