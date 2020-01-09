import sys
import time
import fiber
import multiprocessing as mp
from threading import Thread
from fiber import SimpleQueue as Queue
from datetime import datetime
import logging
import time
import random

logger = logging.getLogger('fiber')
logger.setLevel(logging.DEBUG)
 
NUM = int(1e6)
MSG = "MESSAGE_"# * 1024
def worker(recv_q, send_q):
    every = NUM / 10
    for task_nbr in range(NUM):
        send_q.put(MSG + str(task_nbr))
        if task_nbr % every == 0:
            print("worker put", task_nbr, MSG)
    print("before worker got")
    msg = recv_q.get()
    print("worker got", msg)
    sys.exit(1)
  
def single_queue_mp_write(mplib):
    """Single queue, get in master process and put in worker process."""
    every = NUM / 10
    send_q = mplib.SimpleQueue()
    recv_q = mplib.SimpleQueue()
    mplib.Process(target=worker, daemon=True, args=(send_q, recv_q)).start()
    for num in range(NUM):
        msg = recv_q.get()
        if num % every == 0:
            print("master got", num, msg)
    send_q.put(None)
    print("master put", None)
 
def single_queue_sync_rw(mplib):
    """Synchronize write and read."""
    send_q = mplib.SimpleQueue()
    put = False
    for num in range(NUM):
        if num % 1000 == 0:
            put = not put
            print(datetime.now().strftime("%H:%M:%S.%f"), "put" if put else "get", num)
        if put:
            send_q.put(MSG)
        else:
            send_q.get()


def bench(func, mplib=fiber):
    """Benchmark func with a multiprocessing lib: fiber or multiprocessiong."""
    start_time = time.time()
    func(mplib)
    end_time = time.time()
    duration = end_time - start_time
    msg_per_sec = NUM / duration
 
    doc = func.__doc__.strip() if func.__doc__ is not None else ""
    print("Benchmark result - {} - {}\n{}".format(func.__name__, mplib.__name__, doc))
    print("Duration: {}".format(duration))
    print("Messages Per Second: {}".format(msg_per_sec))
    print("Effective Data Rate: {} Mbps".format(msg_per_sec * len(MSG) * 8 / 1e6))


def pi_inside(p):
    x, y = random.random(), random.random()
    return x * x + y * y < 1

def pi_estimation(mp_lib):
    """Benchmark pi estimation with random number sampling."""
    NUM_SAMPLES = int(2e4)

    pool = mp_lib.Pool(processes=4)
    count = sum(pool.map(pi_inside, range(0, NUM_SAMPLES)))
    print("Pi is roughly {}".format(4.0 * count / NUM_SAMPLES))

def compare(func):
    """Run func with both multiprocessing and fiber."""
    start_time = time.time()
    func(mp)
    end_time = time.time()
    duration1 = end_time - start_time
 
    doc = func.__doc__.strip() if func.__doc__ is not None else ""
    print("Compare result - {}\n{}".format(func.__name__, doc))
    print("multiprocessing duration: {}".format(duration1))

    start_time = time.time()
    func(fiber)
    end_time = time.time()
    duration2 = end_time - start_time
 
    print("fiber duration: {}".format(duration2))

    print("fiber vs. multiprocessing: {:.2%}".format(duration2 / duration1))

if __name__ == "__main__":
    #bench(single_queue_sync_rw, mplib=fiber)
    #bench(single_queue_mp_write, mplib=fiber)
    #compare(pi_estimation)
    compare(single_queue_sync_rw)
    #compare(single_queue_mp_write)
