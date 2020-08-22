#
# Module providing the `Pool` class for managing a process pool
#
# multiprocessing/pool.py
#
# Copyright (c) 2006-2008, R Oudkerk
# Licensed to PSF under a Contributor Agreement.
#
# Modifications Copyright (c) 2020 Uber Technologies

"""
*Pools* are supported by Fiber. They allow the user to manage a pool of
worker processes. Fiber extend pools with *job-backed processes* so that it can
manage thousands of (remote) workers per pool. Users can also create multiple
pools at the same time.

Fiber implements 2 different version of `Pool`: `ZPool` and `ResilientZPool`.
Both has the same API as `multiprocessing.Pool`. `ZPool` is pool based on
"r"/"w" socket pairs.
`ResilientZPool` is `ZPool` + [error handling](advanced.md#error-handling).
Failed tasks will be resubmitted to the Pool and worked on by other pool
workers.

By default, `ResilientZPool` is exposed as `fiber.Pool`.

Example:

```python
pool = fiber.Pool(processes=4)
pool.map(math.sqrt, range(10))
```
"""

import logging
import multiprocessing as mp
import multiprocessing.pool as mp_pool
import multiprocessing.util as mp_util
import math
import queue
import random
import struct
import threading
import sys
import time
import secrets
import traceback
from multiprocessing.pool import (CLOSE, RUN, TERMINATE,
                                  ExceptionWithTraceback, MaybeEncodingError,
                                  ThreadPool, _helper_reraises_exception)

import fiber.queues
import fiber.config as config
from fiber.backend import get_backend
from fiber.queues import LazyZConnection
from fiber.socket import Socket
from fiber.process import current_process
import signal


if fiber.util.is_in_interactive_console():
    import cloudpickle as pickle
else:
    import pickle

logger = logging.getLogger('fiber')

MIN_PORT = 40000
MAX_PORT = 65535


def safe_join_worker(proc):
    p = proc
    if p.is_alive():
        # worker has not yet exited
        logger.debug('cleaning up worker %s' % p.pid)

        p.join(5)


def safe_terminate_worker(proc):
    delay = random.random()

    # Randomize start time to prevent overloading the server
    logger.debug(
        "start multiprocessing.pool.worker terminator thread for proc %s "
        "with delay %s", proc.name, delay)
    time.sleep(delay)

    p = proc
    if p.exitcode is None:
        p.terminate()

    logger.debug("safe_terminate_worker() finished")


def safe_start(proc):
    try:
        proc.start()
        proc._start_failed = False
    except Exception:
        msg = traceback.format_exc()
        logging.warning("failed to start process %s: %s", proc.name, msg)
        # Set this so that this process can be cleaned up later
        proc._start_failed = True


def mp_worker_core(inqueue, outqueue, maxtasks=None, wrap_exception=False):
    logger.debug('mp_worker_core running')
    put = outqueue.put
    get = inqueue.get

    completed = 0
    while maxtasks is None or (maxtasks and completed < maxtasks):
        try:
            task = get()
        except (EOFError, OSError):
            logger.debug('worker got EOFError or OSError -- exiting')
            break

        if task is None:
            logger.debug('worker got sentinel -- exiting')
            break

        job, i, func, args, kwds = task
        try:
            result = (True, func(*args, **kwds))
        except Exception as e:
            if wrap_exception and func is not _helper_reraises_exception:
                e = ExceptionWithTraceback(e, e.__traceback__)
            result = (False, e)
        try:
            put((job, i, result))
        except Exception as e:
            wrapped = MaybeEncodingError(e, result[1])
            logger.debug("Possible encoding error while sending result: %s" % (
                wrapped))
            put((job, i, (False, wrapped)))

        task = job = result = func = args = kwds = None
        completed += 1
    logger.debug('worker exiting after %s tasks' % completed)


def mp_worker(inqueue, outqueue, initializer=None, initargs=(), maxtasks=None,
              wrap_exception=False, num_workers=1):
    """This is mostly the same as multiprocessing.pool.worker, the difference
    is that it will start multiple workers (specified by `num_workers` argument)
    via multiproccessing and allow the Fiber pool worker to take multiple CPU
    cores.
    """

    assert maxtasks is None or (type(maxtasks) == int and maxtasks > 0)
    if hasattr(inqueue, '_writer'):
        inqueue._writer.close()
        outqueue._reader.close()

    if initializer is not None:
        initializer(*initargs)

    workers = []
    for i in range(num_workers):
        # Use fork context to make sure that imported modules and other things
        # are shared. This is useful when modules share objects between
        # processes.
        ctx = mp.get_context('fork')
        p = ctx.Process(target=mp_worker_core,
                        args=(inqueue, outqueue, maxtasks, wrap_exception))
        p.start()
        workers.append(p)

    for w in workers:
        w.join()


class ClassicPool(mp_pool.Pool):

    @staticmethod
    def Process(ctx, *args, **kwds):
        return fiber.process.Process(*args, **kwds)

    def __init__(self, processes=None, initializer=None, initargs=(),
                 maxtasksperchild=None, cluster=None):

        self._ctx = None
        self._setup_queues()
        self._taskqueue = queue.Queue()
        self._cache = {}
        self._state = RUN
        self._maxtasksperchild = maxtasksperchild
        self._initializer = initializer
        self._initargs = initargs

        if processes is None:
            processes = 1
        if processes < 1:
            raise ValueError("Number of processes must be at least 1")

        if initializer is not None and not callable(initializer):
            raise TypeError("initializer must be a callable")

        self._processes = processes
        self._pool = []
        self._threads = []
        self._repopulate_pool()

        # Worker handler
        self._worker_handler = threading.Thread(
            target=ClassicPool._handle_workers,
            args=(self._cache, self._taskqueue, self._ctx, self.Process,
                  self._processes, self._pool, self._threads, self._inqueue,
                  self._outqueue, self._initializer, self._initargs,
                  self._maxtasksperchild, self._wrap_exception)
        )
        self._worker_handler.daemon = True
        self._worker_handler._state = RUN
        self._worker_handler.start()
        logger.debug(
            "Pool: started _handle_workers thread(%s:%s)",
            self._worker_handler.name, self._worker_handler.ident
        )

        # Task handler

        # NOTE: Fiber socket is not thread safe. SimpleQueue is built with Fiber
        # socket, so it shouldn't be shared between different threads. But
        # here _quick_put and _inqueue are only accessed by _task_handler
        # except when the Pool is terminated and `_terminate` is called.
        self._task_handler = threading.Thread(
            target=ClassicPool._handle_tasks,
            args=(self._taskqueue, self._quick_put, self._outqueue,
                  self._pool, self._cache)
        )
        self._task_handler.daemon = True
        self._task_handler._state = RUN
        self._task_handler.start()
        logger.debug(
            "Pool: started _handle_tasks thread(%s:%s)",
            self._task_handler.name, self._task_handler.ident
        )

        # Result handler
        self._result_handler = threading.Thread(
            target=ClassicPool._handle_results,
            args=(self._outqueue, self._quick_get, self._cache)
        )
        self._result_handler.daemon = True
        self._result_handler._state = RUN
        self._result_handler.start()
        logger.debug(
            "Pool: started _handle_results thread(%s:%s)",
            self._result_handler.name, self._result_handler.ident
        )

        # TODO use fiber's own weak ref
        self._terminate = mp_util.Finalize(
            self, self._terminate_pool,
            args=(self._taskqueue, self._inqueue, self._outqueue, self._pool,
                  self._threads, self._worker_handler, self._task_handler,
                  self._result_handler, self._cache),
            exitpriority=15
        )
        logger.debug("Pool: registered _terminate_pool finalizer")

    def _setup_queues(self):
        self._inqueue = fiber.queues.SimpleQueue()
        logger.debug("Pool|created Pool._inqueue: %s", self._inqueue)
        self._outqueue = fiber.queues.SimpleQueue()
        logger.debug("Pool|created Pool._outqueue: %s", self._outqueue)

        # TODO(jiale) use send_string instead?
        self._quick_put = self._inqueue.put
        # TODO(jiale) can't use _outqueue.reader.get because _outqueue.reader
        # is a REQ socket. It can't be called consecutively.
        self._quick_get = self._outqueue.get

    def _map_async(self, func, iterable, mapper, chunksize=None, callback=None,
                   error_callback=None):
        """
        Helper function to implement map, starmap and their async counterparts.
        """
        if self._state != RUN:
            raise ValueError("Pool not running")
        if not hasattr(iterable, '__len__'):
            iterable = list(iterable)

        if chunksize is None:
            # use self._processes to replace len(self._pool) to calculate
            # chunk size. This is because len(self._pool) is the number
            # of jobs not total processes.
            chunksize, extra = divmod(len(iterable), self._processes * 4)
            if extra:
                chunksize += 1
        if len(iterable) == 0:
            chunksize = 0

        task_batches = ClassicPool._get_tasks(func, iterable, chunksize)
        result = mp_pool.MapResult(self._cache, chunksize, len(iterable),
                                   callback, error_callback=error_callback)
        self._taskqueue.put(
            (
                self._guarded_task_generation(result._job,
                                              mapper,
                                              task_batches),
                None
            )
        )
        return result

    @staticmethod
    def _handle_workers(cache, taskqueue, ctx, Process, processes, pool,
                        threads, inqueue, outqueue, initializer, initargs,
                        maxtasksperchild, wrap_exception):
        thread = threading.current_thread()

        # Keep maintaining workers until the cache gets drained, unless the
        # pool is terminated.
        while thread._state == RUN or (cache and thread._state != TERMINATE):
            ClassicPool._maintain_pool(ctx, Process, processes, pool, threads,
                                inqueue, outqueue, initializer, initargs,
                                maxtasksperchild, wrap_exception)
            time.sleep(0.1)

        logger.debug("_handle_workers exits")

    @staticmethod
    def _join_exited_workers(pool):
        """Cleanup after any worker processes which have exited due to reaching
        their specified lifetime.  Returns True if any workers were cleaned up.
        """
        cleaned = False

        thread = threading.current_thread()
        for i in reversed(range(len(pool))):
            # leave dead workers for later cleaning
            if getattr(thread, "_state", None) == TERMINATE:
                logger.debug("pool is being terminated, "
                             "leave dead workers for later cleaning")
                break

            worker = pool[i]
            if worker._start_failed:
                cleaned = True
                del pool[i]
                logger.debug("remove process %s which failed to "
                             "start", worker.name)
                continue

            logger.debug("check worker.exitcode %s", worker.name)
            if worker.exitcode is not None:
                # worker exited
                worker.join()
                cleaned = True
                del pool[i]
        return cleaned

    def _repopulate_pool(self):
        return self._repopulate_pool_static(self._ctx, self.Process,
                                            self._processes,
                                            self._pool, self._threads,
                                            self._inqueue,
                                            self._outqueue, self._initializer,
                                            self._initargs,
                                            self._maxtasksperchild,
                                            self._wrap_exception)

    @staticmethod
    def _repopulate_pool_static(ctx, Process, processes, pool, threads,
                                inqueue, outqueue, initializer, initargs,
                                maxtasksperchild, wrap_exception):
        """Bring the number of pool processes up to the specified number,
        for use after reaping workers which have exited.
        """
        logger.debug("_repolulate_pool_static, pool: %s", pool)

        thread = threading.current_thread()

        workers_per_fp = config.cpu_per_job

        remain = processes - len(pool) * workers_per_fp
        while remain > 0:
            # don't repolulate workers if exiting
            logger.debug(
                "processes %s, len(pool) %s",
                processes, len(pool)
            )
            if getattr(thread, "_state", None) == TERMINATE:
                logger.debug("pool is being terminated, stop "
                             "repopulating workers")
                break

            num_workers = (
                workers_per_fp if remain >= workers_per_fp else remain
            )

            w = Process(ctx, target=mp_worker,
                        args=(inqueue, outqueue,
                              initializer,
                              initargs, maxtasksperchild,
                              wrap_exception, num_workers))
            pool.append(w)
            remain = remain - num_workers
            w.name = w.name.replace('Process', 'PoolWorker')
            w.daemon = True
            td = threading.Thread(target=safe_start, args=(w,))
            td.start()
            threads.append(td)
            logger.debug("start multiprocessing.pool.worker starter "
                         "thread(%s:%s)",
                         td.name, td.ident)

        logger.debug("_repolulate_pool_static done, pool: %s", pool)

    @staticmethod
    def _maintain_pool(ctx, Process, processes, pool, threads, inqueue,
                       outqueue, initializer, initargs, maxtasksperchild,
                       wrap_exception):
        """Clean up any exited workers and start replacements for them.
        """
        if ClassicPool._join_exited_workers(pool):
            ClassicPool._repopulate_pool_static(ctx, Process, processes, pool,
                                         threads, inqueue, outqueue,
                                         initializer, initargs,
                                         maxtasksperchild,
                                         wrap_exception)

    @staticmethod
    def _handle_tasks(taskqueue, put, outqueue, pool, cache):
        thread = threading.current_thread()

        for taskseq, set_length in iter(taskqueue.get, None):
            task = None
            try:
                # iterating taskseq cannot fail
                for task in taskseq:
                    if thread._state:
                        break
                    try:
                        put(task)
                    except Exception as e:
                        job, idx = task[:2]
                        try:
                            cache[job]._set(idx, (False, e))
                        except KeyError:
                            pass
                else:
                    if set_length:
                        idx = task[1] if task else -1
                        set_length(idx + 1)
                    continue
                break
            finally:
                task = taskseq = job = None
        else:
            logger.debug('_handle_tasks: task handler got sentinel')

        try:
            # tell result handler to finish when cache is empty
            logger.debug('_handle_tasks: task handler sending sentinel '
                         'to result handler')
            outqueue.put(None)

            # tell workers there is no more work
            logger.debug('_handle_tasks: task handler sending sentinel '
                         'to workers')
            for p in pool:
                put(None)
        except OSError:
            logger.debug('_handle_tasks: task handler got OSError when '
                         'sending sentinels')

        logger.debug('_handle_tasks: task handler exiting')

    @staticmethod
    def _handle_results(outqueue, get, cache):
        thread = threading.current_thread()

        while 1:
            try:
                task = get()
            except (OSError, EOFError):
                # logger.debug('result handler got EOFError/OSError: exiting')
                return

            if thread._state:
                assert thread._state == TERMINATE
                # logger.debug('result handler found thread._state=TERMINATE')
                break

            if task is None:
                # logger.debug('result handler got sentinel')
                break

            job, i, obj = task
            try:
                cache[job]._set(i, obj)
            except KeyError:
                pass
            task = job = obj = None

        while cache and thread._state != TERMINATE:
            try:
                task = get()
            except (OSError, EOFError):
                logger.debug('result handler got EOFError/OSError -- exiting')
                return

            if task is None:
                logger.debug('result handler ignoring extra sentinel')
                continue
            job, i, obj = task
            try:
                cache[job]._set(i, obj)
            except KeyError:
                pass
            task = job = obj = None

        if hasattr(outqueue, '_reader'):
            logger.debug('ensuring that outqueue is not full')
            # If we don't make room available in outqueue then
            # attempts to add the sentinel (None) to outqueue may
            # block.  There is guaranteed to be no more than 2 sentinels.
            try:
                for i in range(10):
                    if not outqueue._reader.poll():
                        break
                    get()
            except (OSError, EOFError):
                pass

        logger.debug('result handler exiting: len(cache)=%s, thread._state=%s',
                     len(cache), thread._state)

    @classmethod
    def _terminate_pool(cls, taskqueue, inqueue, outqueue, pool, threads,
                        worker_handler, task_handler, result_handler, cache):
        # this is guaranteed to only be called once
        logger.debug('finalizing pool')
        start = time.time()
        terminate_start = start

        worker_handler._state = TERMINATE
        task_handler._state = TERMINATE

        logger.debug('helping task handler/workers to finish')

        assert result_handler.is_alive() or len(cache) == 0

        result_handler._state = TERMINATE
        outqueue.put(None)                  # sentinel

        # This was previously done in _handle_workers, we move it here to
        # improve terminating speed
        logger.debug("send sentinel, put None in task queue")
        taskqueue.put(None)

        elapsed = time.time() - start
        start = start + elapsed
        logger.debug("outqueue put None took %s", elapsed)

        # Since worker_handler._state is already set, not more new processes
        # will be created. So we don't need to wait for worker_handler.
        # Original comments below doesn't apply anymore. They are kept here
        # for reference.
        #
        # Original comment: We must wait for the worker handler to exit before
        # terminating workers because we don't want workers to be restarted
        # behind our back.

        # logger.debug('joining worker handler')
        # if threading.current_thread() is not worker_handler:
        #     worker_handler.join()

        elapsed = time.time() - start
        start = start + elapsed
        logger.debug("joining worker handler took %s", elapsed)

        logger.debug('tell process._popen that it should exit')
        for p in pool:
            if p._popen is not None:
                logger.debug('set process._popen._exiting = True '
                             'for %s', p.name)
                p._popen._exiting = True

        elapsed = time.time() - start
        start = start + elapsed
        logger.debug("setting p._popen._exiting took %s", elapsed)

        logger.debug('joining starter threads')
        for td in threads:
            # wait for all starter threads to finish
            logger.debug('joining starter thread %s', td.name)
            td.join()
            logger.debug('joining starter thread finished')

        elapsed = time.time() - start
        start = start + elapsed
        logger.debug("joining starter threads took %s", elapsed)

        N = min(100, len(pool))
        # Thread pool used for process termination. With randomized delay, this
        # roughly equals N requests per second.
        tpool = ThreadPool(N)

        # Terminate workers which haven't already finished.
        if pool and hasattr(pool[0], 'terminate'):
            logger.debug('terminating workers')
            tpool.map_async(safe_terminate_worker, pool)

        elapsed = time.time() - start
        start = start + elapsed
        logger.debug("run safe_terminate_worker threads took %s", elapsed)

        logger.debug('joining task handler')
        if threading.current_thread() is not task_handler:
            task_handler.join()

        elapsed = time.time() - start
        start = start + elapsed
        logger.debug("joining task handler took %s", elapsed)

        logger.debug('joining result handler')
        if threading.current_thread() is not result_handler:
            result_handler.join()

        elapsed = time.time() - start
        start = start + elapsed
        logger.debug("joining result handler took %s", elapsed)

        tpool = ThreadPool(N)
        if pool and hasattr(pool[0], 'terminate'):
            logger.debug('joining pool workers, this may take some '
                         'time. workers: %s', pool)
            tpool.map(safe_join_worker, pool)

        elapsed = time.time() - start
        start = start + elapsed
        logger.debug("joining pool workers took %s", elapsed)
        logger.debug(
            "terminating pool finished. it took %s",
            time.time() - terminate_start
        )


class Inventory():
    """An inventory object that holds map results.

    An `seq` needs to be requested with `add` method so that map result can
    be tracked. `seq` acts as an request id. In later stage, `get` method
    can be called with corresponding `seq`. This inventory will handle waiting,
    managing results from different map calls.
    """
    def __init__(self, queue_get):
        self._seq = 0
        self._queue_get = queue_get
        self._inventory = {}
        self._spec = {}
        self._idx_cur = {}

    def add(self, ntasks):
        self._seq += 1
        self._inventory[self._seq] = [None] * ntasks
        self._spec[self._seq] = ntasks
        self._idx_cur[self._seq] = 0
        return self._seq

    def get(self, job_seq):
        n = self._spec[job_seq]

        while n != 0:
            # seq, batch, batch + i, result
            seq, _, i, result = self._queue_get()
            self._inventory[seq][i] = result
            self._spec[seq] -= 1
            if seq == job_seq:
                n = self._spec[seq]

        ret = self._inventory[job_seq]
        self._inventory[job_seq] = None
        return ret

    def iget_unordered(self, job_seq):
        n = self._spec[job_seq]

        while n != 0:
            # seq, batch, batch + i, result
            seq, _, i, result = self._queue_get()
            self._inventory[seq][i] = result
            self._spec[seq] -= 1
            if seq == job_seq:
                res = self._inventory[job_seq][i]
                self._inventory[job_seq][i] = None
                n = self._spec[seq]
                yield res

        return

    def iget_ordered(self, job_seq):
        idx = self._idx_cur[job_seq]
        total = len(self._inventory[job_seq])

        while idx != total:
            if self._inventory[job_seq][idx] is not None:

                res = self._inventory[job_seq][idx]
                self._inventory[job_seq][idx] = None

                idx += 1
                self._spec[job_seq] = idx
                yield res

                continue

            # seq, batch, batch + i, result
            seq, _, i, result = self._queue_get()
            self._inventory[seq][i] = result

            if seq == job_seq:
                if i == idx:
                    # got the next one
                    res = self._inventory[job_seq][i]
                    self._inventory[job_seq][i] = None

                    idx += 1
                    self._spec[job_seq] = idx

                    yield res

        return


class MapResult():
    def __init__(self, seq, inventory):
        self._seq = seq
        self._inventory = inventory

    def get(self):
        return self._inventory.get(self._seq)

    def iget_ordered(self):
        return self._inventory.iget_ordered(self._seq)

    def iget_unordered(self):
        return self._inventory.iget_unordered(self._seq)


class ApplyResult(MapResult):
    """An object that is returned by asynchronous methods of `Pool`. It
    represents an handle that can be used to get the actual result.
    """

    def get(self):
        """Get the actual result represented by this object

        :returns: Actual result. This method will block if the actual result
            is not ready.
        """
        return self._inventory.get(self._seq)[0]


def zpool_worker_core(master_conn, result_conn, maxtasksperchild,
                      wrap_exception, rank=-1, req=False):
    """
    The actual function that processes tasks.

    :param master_conn: connection that is used to read task from Pool.
    :param result_conn: connection that is used to send results to Pool.
    :param maxtasksperchild: TODO: max tasks per child process.
    "param wrap_exception: TODO
    "param rank: used when zpool_worker started many zpool_worker_core
        processes. `rank` is the id of this process.
    "pram req": whether master_conn is based on a REQ type socket. When this
        flag is set, `zpool_worker_core` needs to request tasks from the Pool
        by send it's (ident, proc.id) to the Pool and Pool will send a task
        back to the worker. This doesn't change result_conn.
    """
    logger.debug("zpool_worker_core started %s", rank)

    proc = None
    ident = secrets.token_bytes(4)
    if req:
        proc = current_process()

    while True:
        if req:
            # master_conn is a REQ type socket, need to send id (rank) to
            # master to request a task. id is packed in type unsigned short (H)
            master_conn.send_bytes(struct.pack("4si", ident, proc.pid))
            #print("worker_core send", ident, proc.pid)

        task = master_conn.recv()
        if task is None:
            logger.debug("task worker got None, exiting")
            break

        seq, batch, func, arg_list, starmap = task
        logger.debug('zpool_worker got %s, %s, %s, %s', seq, batch,
                     func, arg_list)
        if len(arg_list) == 0:
            continue

        if starmap:
            for i, arg_item in enumerate(arg_list):
                nargs = len(arg_item)
                if nargs == 2:
                    args, kwds = arg_item
                    res = func(*args, **kwds)
                elif nargs == 1:
                    args = arg_item[0]
                    res = func(*args)
                else:
                    raise ValueError("Bad number of args, %s %s",
                                     nargs, arg_item)

                data = (seq, batch, batch + i, res)
                if req:
                    data += (ident,)
                result_conn.send(data)
        else:
            for i, args in enumerate(arg_list):
                res = func(args)
                data = (seq, batch, batch + i, res)
                if req:
                    data += (ident,)
                result_conn.send(data)
    #print("worker_core exit, ", rank, proc.pid)


def handle_signal(signal, frame):
    # run sys.exit() so that atexit handlers can run
    sys.exit()

def zpool_worker(master_conn, result_conn, initializer=None, initargs=(),
                 maxtasks=None, wrap_exception=False, num_workers=1, req=False):
    """
    The entry point of Pool worker function.

    :param master_conn: connection that is used to read task from Pool.
    :param result_conn: connection that is used to send results to Pool.
    :param maxtasksperchild: TODO: max tasks per child process.
    :param wrap_exception: TODO
    :param num_workers: number of workers to start.
    :param rank: used when zpool_worker started many zpool_worker_core
        processes. `rank` is the id of this process.
    :param req: whether master_conn is based on a REQ type socket. When this
        flag is set, `zpool_worker_core` needs to request tasks from the Pool
        by send it's (ident, proc.id) to the Pool and Pool will send a task
        back to the worker. This doesn't change result_conn.
    """
    logger.debug("zpool_worker running")
    signal.signal(signal.SIGTERM, handle_signal)

    if wrap_exception:
        # TODO(jiale) implement wrap_exception
        raise NotImplementedError

    assert maxtasks is None or (type(maxtasks) == int and maxtasks > 0)

    if initializer is not None:
        initializer(*initargs)

    if num_workers == 1:
        return zpool_worker_core(master_conn, result_conn, maxtasks,
                                 wrap_exception, 0, req=req)

    workers = []
    for i in range(num_workers):
        # Use fork context to make sure that imported modules and other things
        # are shared. This is useful when modules share objects between
        # processes.
        ctx = mp.get_context('fork')
        p = ctx.Process(target=zpool_worker_core,
                        args=(master_conn, result_conn, maxtasks,
                              wrap_exception, i, req), daemon=True)
        p.start()
        workers.append(p)

    for w in workers:
        w.join()


class ZPool():
    """A Pool implementation based on Fiber sockets.

    ZPool directly uses Fiber sockets instead of SimpleQueue for tasks and
    results handling. This makes it faster.
    """

    def __init__(self, processes=None, initializer=None, initargs=(),
                 maxtasksperchild=None, cluster=None,
                 master_sock_type="w"):

        self._pool = []
        # Set default processes to 1
        self._processes = processes if processes is not None else 1
        self._initializer = initializer
        self._initargs = initargs
        self._maxtasksperchild = maxtasksperchild
        self._cluster = cluster
        self._seq = 0
        self._state = RUN
        self.taskq = queue.Queue()
        self.sent_tasks = 0
        self.recv_tasks = 0
        self.max_processing_tasks = 20000

        # networking related
        backend = get_backend()
        ip, _, _ = backend.get_listen_addr()

        socket = Socket(mode=master_sock_type)
        _master_port = socket.bind()
        _master_addr = 'tcp://{}:{}'.format(ip, _master_port)
        self._master_addr = _master_addr
        self._master_sock = socket

        socket = Socket(mode="r")
        _result_port = socket.bind()
        _result_addr = 'tcp://{}:{}'.format(ip, _result_port)
        self._result_addr = _result_addr
        self._result_sock = socket

        logger.debug("creating %s", self)
        self._inventory = Inventory(self._res_get)

        logger.debug("%s, starting workers", self)

        td = threading.Thread(
            target=self.__class__._handle_workers, args=(self,)
        )
        td.daemon = True
        td._state = RUN
        # `td` will be started later by `lazy_start_workers` later
        self._worker_handler = td
        self._worker_handler_started = False

        # launch task handler
        td = threading.Thread(
            target=self._handle_tasks,
        )
        td.daemon = True
        td._state = RUN
        td.start()
        self._task_handler = td

    def __repr__(self):
        return "<{}({}, {})>".format(
            type(self).__name__,
            getattr(self, "_processes", None),
            getattr(self, "_master_addr", None),
        )

    def _handle_tasks(self):
        taskq = self.taskq
        master_sock = self._master_sock

        while True:
            if self.sent_tasks - self.recv_tasks > self.max_processing_tasks:
                time.sleep(0.2)
                continue
            task = taskq.get()
            data = pickle.dumps(task)
            master_sock.send(data)
            self.sent_tasks += 1

    def _task_put(self, task):
        self.taskq.put(task)

    def _res_get(self):
        payload = self._result_sock.recv()
        self.recv_tasks += 1
        data = pickle.loads(payload)

        return data

    @staticmethod
    def _join_exited_workers(workers):

        thread = threading.current_thread()

        logger.debug("ZPool _join_exited_workers running, workers %s, "
                     "thread._state %s", workers, thread._state)
        exited_workers = []

        for i in reversed(range(len(workers))):
            if thread._state != RUN:
                break

            worker = workers[i]
            if worker._start_failed:
                exited_workers.append(worker)
                del workers[i]
                logger.debug("ZPool _join_exited_workers running, "
                             "worker._start_failed +1 %s", worker)
                continue

            if worker.exitcode is not None:
                worker.join()
                exited_workers.append(worker)
                del workers[i]
                logger.debug("ZPool _join_exited_workers running, "
                             "worker.join() done +1 %s", worker)
                continue

        logger.debug("ZPool _join_exited_workers finished, workers %s, "
                     "exited %s", workers, exited_workers)

        return exited_workers

    @staticmethod
    def _maintain_workers(processes, workers, master_addr, result_addr, initializer,
                          initargs, maxtasksperchild):
        thread = threading.current_thread()

        workers_per_fp = config.cpu_per_job
        left = processes - len(workers)

        logger.debug("ZPool _maintain_workers running, workers %s", workers)

        threads = []
        while left > 0 and thread._state == RUN:

            if left > workers_per_fp:
                n = workers_per_fp
            else:
                n = left

            master_conn = LazyZConnection(("r", master_addr))
            result_conn = LazyZConnection(("w", result_addr))
            master_conn.set_name("master_conn")
            w = fiber.process.Process(target=zpool_worker,
                                      args=(master_conn,
                                            result_conn,
                                            initializer,
                                            initargs,
                                            maxtasksperchild,
                                            False,
                                            n),
                                      daemon=False)

            w.name = w.name.replace("Process", "PoolWorker")

            td = threading.Thread(target=safe_start, args=(w,))
            td.start()
            threads.append(td)
            logger.debug("started safe_start thread %s", td)

            # w.start()
            logger.debug("started proc %s", w)
            workers.append(w)
            left -= n

        for td in threads:
            logger.debug("joining safe_start thread %s", td)
            td.join(2)
            logger.debug("joining safe_start thread %s finished", td)

        logger.debug("ZPool _maintain_workers finished, workers %s", workers)

    @staticmethod
    def _handle_workers(pool):
        logger.debug("%s _handle_workers running", pool)
        td = threading.current_thread()

        ZPool._maintain_workers(
            pool._processes, pool._pool,
            pool._master_addr, pool._result_addr, pool._initializer,
            pool._initargs, pool._maxtasksperchild
        )

        while td._state == RUN:
            if len(ZPool._join_exited_workers(pool._pool)) > 0:
                # create new workers when old workers exited
                ZPool._maintain_workers(
                    pool._processes, pool._pool,
                    pool._master_addr, pool._result_addr, pool._initializer,
                    pool._initargs, pool._maxtasksperchild
                )

            time.sleep(0.5)

        logger.debug("%s _handle_workers finished. Status is not RUN",
                     pool)

    @staticmethod
    def _chunks(iterable, size):
        for i in range(0, len(iterable), size):
            yield iterable[i:i + size]

    def apply_async(self, func, args=(), kwds={}, callback=None,
                    error_callback=None):
        """
        Run function `func` with arguments `args` and keyword arguments `kwds`
        on a remote Pool worker. This is an asynchronous version of `apply`.

        :param func: target function to run.
        :param args: positional arguments that needs to be passed to `func`.
        :param kwds: keyword arguments that needs to be passed to `func`.
        :param callback: Currently not supported. A callback function that will
            be called when the result is ready.
        :param error_callback: Currently not supported. A callback function
            that will be called when an error occurred.

        :returns: An ApplyResult object which has a method `.get()` to get
            the actual results.
        """

        if self._state != RUN:
            raise ValueError("Pool is not running")
        # assert kwds == {}, 'kwds not supported yet'
        self.lazy_start_workers(func)

        seq = self._inventory.add(1)
        self._task_put((seq, 0, func, [(args, kwds)], True))
        res = ApplyResult(seq, self._inventory)

        return res

    def start_workers(self):
        self._worker_handler.start()
        self._worker_handler_started = True

    def lazy_start_workers(self, func):
        if hasattr(func, "__fiber_meta__"):
            if (
                not hasattr(zpool_worker, "__fiber_meta__")
                or zpool_worker.__fiber_meta__ != func.__fiber_meta__
            ):
                if self._worker_handler_started:
                    raise RuntimeError(
                        "Cannot run function that has different resource "
                        "requirements acceptable by this pool. Try creating a "
                        "different pool for it."
                    )
                zpool_worker.__fiber_meta__ = func.__fiber_meta__

        if not self._worker_handler_started:
            self.start_workers()

    def map_async(self, func, iterable, chunksize=None, callback=None,
                  error_callback=None):
        """
        For each element `e` in `iterable`, run `func(e)`. The workload is
        distributed between all the Pool workers. This is an asynchronous
        version of `map`.


        :param func: target function to run.
        :param iterable: an iterable object to be mapped.
        :param chunksize: if set, elements in `iterable` will be put in to
            chunks whose size is decided by `chunksize`. These chunks will be
            sent to Pool workers instead of each elements in `iterable`. If not
            set, the chunksize is decided automatically.
        :param callback: Currently not supported. A callback function that will
            be called when the result is ready.
        :param error_callback: Currently not supported. A callback function
            that will be called when an error occurred.

        :returns: An MapResult object which has a method `.get()` to get
            the actual results.
        """

        if error_callback:
            # TODO(jiale) implement error callback
            raise NotImplementedError

        if self._state != RUN:
            raise ValueError("Pool is not running")

        if chunksize is None:
            chunksize = 32

        if not hasattr(iterable, '__len__'):
            iterable = list(iterable)

        self.lazy_start_workers(func)

        seq = self._inventory.add(len(iterable))

        chunks = self.__class__._chunks(iterable, chunksize)
        for batch, chunk in enumerate(chunks):
            self._task_put((seq, batch * chunksize, func, chunk, False))
        res = MapResult(seq, self._inventory)

        return res

    def apply(self, func, args=(), kwds={}):
        """
        Run function `func` with arguments `args` and keyword arguments `kwds`
        on a remote Pool worker.

        :param func: target function to run.
        :param args: positional arguments that needs to be passed to `func`.
        :param kwds: keyword arguments that needs to be passed to `func`.

        :returns: the return value of `func(*args, **kwargs)`.
        """
        return self.apply_async(func, args, kwds).get()

    def map(self, func, iterable, chunksize=None):
        """
        For each element `e` in `iterable`, run `func(e)`. The workload is
        distributed between all the Pool workers.


        :param func: target function to run.
        :param iterable: an iterable object to be mapped.
        :param chunksize: if set, elements in `iterable` will be put in to
            chunks whose size is decided by `chunksize`. These chunks will be
            sent to Pool workers instead of each elements in `iterable`. If not
            set, the chunksize is decided automatically.

        :returns: A list of results equivalent to calling
            `[func(x) for x in iterable]`.
        """
        logger.debug('%s map func=%s', self, func)
        return self.map_async(func, iterable, chunksize).get()

    def imap(self, func, iterable, chunksize=1):
        """
        For each element `e` in `iterable`, run `func(e)`. The workload is
        distributed between all the Pool workers. This function returns an
        iterator which user and iterate over to get results.


        :param func: target function to run.
        :param iterable: an iterable object to be mapped.
        :param chunksize: if set, elements in `iterable` will be put in to
            chunks whose size is decided by `chunksize`. These chunks will be
            sent to Pool workers instead of each elements in `iterable`. If not
            set, the chunksize is decided automatically.

        :returns: an iterator which user can use to get results.
        """
        res = self.map_async(func, iterable, chunksize)
        return res.iget_ordered()

    def imap_unordered(self, func, iterable, chunksize=1):
        """
        For each element `e` in `iterable`, run `func(e)`. The workload is
        distributed between all the Pool workers. This function returns an
        **unordered** iterator which user and iterate over to get results.
        This means that the order of the results may not match the order of
        the `iterable`.


        :param func: target function to run.
        :param iterable: an iterable object to be mapped.
        :param chunksize: if set, elements in `iterable` will be put in to
            chunks whose size is decided by `chunksize`. These chunks will be
            sent to Pool workers instead of each elements in `iterable`. If not
            set, the chunksize is decided automatically.

        :returns: an unordered iterator which user can use to get results.
        """
        res = self.map_async(func, iterable, chunksize)
        return res.iget_unordered()

    def starmap_async(self, func, iterable, chunksize=None, callback=None,
                      error_callback=None):
        """
        For each element `args` in `iterable`, run `func(*args)`. The workload
        is distributed between all the Pool workers. This is an asynchronous
        version of `starmap`.

        For example, `starmap_async(func, [(1, 2, 3), (4, 5, 6)])` will result
        in calling `func(1, 2, 3)` and `func(4, 5, 6)` on a remote host.


        :param func: target function to run.
        :param iterable: an iterable object to be mapped.
        :param chunksize: if set, elements in `iterable` will be put in to
            chunks whose size is decided by `chunksize`. These chunks will be
            sent to Pool workers instead of each elements in `iterable`. If not
            set, the chunksize is decided automatically.
        :param callback: Currently not supported. A callback function that will
            be called when the result is ready.
        :param error_callback: Currently not supported. A callback function
            that will be called when an error occurred.

        :returns: An MapResult object which has a method `.get()` to get
            the actual results.
        """

        if self._state != RUN:
            raise ValueError("Pool is not running")

        if chunksize is None:
            chunksize = 32

        if not hasattr(iterable, '__len__'):
            iterable = list(iterable)

        self.lazy_start_workers(func)

        seq = self._inventory.add(len(iterable))

        iterable = [(item,) for item in iterable]

        chunks = self.__class__._chunks(iterable, chunksize)
        for batch, chunk in enumerate(chunks):
            self._task_put((seq, batch * chunksize, func, chunk, True))

        res = MapResult(seq, self._inventory)

        return res

    def starmap(self, func, iterable, chunksize=None):
        """
        For each element `args` in `iterable`, run `func(*args)`. The workload
        is distributed between all the Pool workers.

        For example, `starmap_async(func, [(1, 2, 3), (4, 5, 6)])` will result
        in calling `func(1, 2, 3)` and `func(4, 5, 6)` on a remote host.


        :param func: target function to run.
        :param iterable: an iterable object to be mapped.
        :param chunksize: if set, elements in `iterable` will be put in to
            chunks whose size is decided by `chunksize`. These chunks will be
            sent to Pool workers instead of each elements in `iterable`. If not
            set, the chunksize is decided automatically.
        :param callback: Currently not supported. A callback function that will
            be called when the result is ready.
        :param error_callback: Currently not supported. A callback function
            that will be called when an error occurred.

        :returns: A list of results equivalent to calling
            `[func(*arg) for arg in iterable]`
        """
        return self.starmap_async(func, iterable, chunksize).get()

    def _send_sentinels_to_workers(self):
        logger.debug('send sentinels(None) to workers %s', self)
        for i in range(self._processes):
            self._task_put(None)

    def close(self):
        """
        Close this Pool. This means the current pool will be put in to a
        closing state and it will not accept new tasks. Existing workers will
        continue to work on tasks that have been dispatched to them and exit
        when all the tasks are done.
        """
        logger.debug('closing pool %s', self)
        if self._state == RUN:
            self._state = CLOSE
            self._worker_handler._state = CLOSE

            for p in self._pool:
                if hasattr(p, '_sentinel'):
                    p._state = CLOSE

            self._send_sentinels_to_workers()

    def terminate(self):
        """
        Terminate this pool. This means that this pool will be terminated and
        all its pool workers will also be terminated. Task that have been
        dispatched will be discarded.
        """
        logger.debug('terminating pool %s', self)

        logger.debug('set pool._worker_handler.status = TERMINATE')
        self._worker_handler._state = TERMINATE
        self._state = TERMINATE

        for p in self._pool:
            p._state = TERMINATE

        pool = self._pool
        N = min(100, len(pool))
        # Thread pool used for process termination. With randomized delay, this
        # roughly equals N requests per second.
        tpool = ThreadPool(N)

        # Terminate workers which haven't already finished.
        if pool and hasattr(pool[0], 'terminate'):
            logger.debug('terminating workers')
            # tpool.map_async(safe_terminate_worker, pool)
            tpool.map(safe_terminate_worker, pool)

        if pool and hasattr(pool[0], 'terminate'):
            logger.debug("joining pool workers, this may take some "
                         "time. workers: %s", pool)
            tpool.map(safe_join_worker, pool)

        logger.debug("joining pool._worker_handler")
        self._worker_handler.join()

    def join(self):
        """
        Wait for all the pool workers of this pool to exit. This should be
        used after `terminate()` or `close()` are called on this pool.
        """
        logger.debug('%s.join()', self)
        assert self._state in (TERMINATE, CLOSE)

        for p in self._pool:
            if p._state not in (TERMINATE, CLOSE):
                logger.debug("%s.join() ignore newly connected Process %s",
                             self, p)
                continue
            p.join()

    def wait_until_workers_up(self):
        logger.debug('%s begin wait_until_workers_up', self)
        workers_per_fp = config.cpu_per_job
        n = math.ceil(float(self._processes) / workers_per_fp)

        while len(self._pool) < n:
            logger.debug("%s waiting for all workers to be up, expected %s, "
                         "got %s", self, n, self._pool)
            time.sleep(0.5)

        for p in self._pool:
            logger.debug('%s waiting for _sentinel %s', self, p)
            while not hasattr(p, '_sentinel') or p._sentinel is None:
                time.sleep(0.5)
        # now all the worker has connected to the master, wait
        # for some additional time to be sure.
        time.sleep(1)
        logger.debug('%s wait_until_workers_up done', self)


class ResilientZPool(ZPool):
    """
    ZPool with error handling. The differences are:

    * Master socket is a ROUTER socket instead of DEALER socket.
    * Add pending table.
    * When an died worker is detected, it's jobs are resubmitted to work Q
      in addition to restarting that worker.

    The API of `ResilientZPool` is the same as `ZPool`. One difference is that
    if `processes` argument is not set, its default value is 1.
    """
    def __init__(self, processes=None, initializer=None, initargs=(),
                 maxtasksperchild=None, cluster=None):

        self.active_peer_dict = {}
        self.active_peer_list = []
        self.peer_lock = threading.Lock()
        self.taskq = queue.Queue()
        self._pending_table = {}

        super(ResilientZPool, self).__init__(
            processes=processes,
            initializer=initializer,
            initargs=initargs,
            maxtasksperchild=maxtasksperchild,
            cluster=cluster,
            master_sock_type="rep")

        '''
        # launch task handler
        td = threading.Thread(
            target=self._handle_tasks,
            args=(
                self.taskq,
                self._master_sock,
                self.active_peer_list,
                self._pending_table,
            ))
        td.daemon = True
        td._state = RUN
        td.start()
        self._task_handler = td
        '''

        self._pid_to_rid = {}

    def _add_peer(self, ident):
        self.peer_lock.acquire()

        self.active_peer_dict[ident] = True
        self._pending_table[ident] = {}
        self.active_peer_list.append(ident)

        self.peer_lock.release()

    def _remove_peer(self, ident):
        # _pendint_table will be cleared later in error handling phase
        self.peer_lock.acquire()

        del self.active_peer_dict[ident]
        self.active_peer_list.remove(ident)

        self.peer_lock.release()

    def _res_get(self):
        # check for system messages
        payload = self._result_sock.recv()
        data = pickle.loads(payload)

        # remove item from pending table
        seq, batch, i, result, ident = data

        # seq, batch, func, chunks, is_starmap
        task = self._pending_table[ident][(seq, batch)]
        chunksize = len(task[3])

        if i == batch + chunksize - 1:
            # this batch has been processed, remove this item from pending
            # table
            del self._pending_table[ident][(seq, batch)]

        # skip ident
        return data[:-1]

    def _task_put(self, task):
        self.taskq.put(task)

    def _handle_tasks(self):
        thread = threading.current_thread()
        taskq = self.taskq
        master_sock = self._master_sock
        pending_table = self._pending_table

        while thread._state == RUN:
            task = taskq.get()

            if task is None:
                logger.debug("_handle_tasks got sentinel")
                break

            msg = master_sock.recv()
            ident, pid = struct.unpack("4si", msg)

            # add peer
            if not self.active_peer_dict.get(ident, None):
                logger.debug('ResilientZPool got REG %s', msg)
                self._add_peer(ident)
                self._pid_to_rid[pid] = ident

            data = pickle.dumps(task)

            seq, batch, func, arg_list, starmap = task
            # use (seq, batch) to identify a (chunked) task. batch is the start
            # number of that batch.
            pending_table[ident][(seq, batch)] = task

            master_sock.send(data)

        # tell peers to exit
        data = pickle.dumps(None)
        #print("send sentinals to workers, active peers", self.active_peer_list)
        for i in range(len(self._pool)):
            #print("waiting for requests")
            msg = master_sock.recv()
            #print("got ", msg)
            master_sock.send(data)
            #print("send ", data)

        #print("exiting handle_tasks ")
        logger.debug('ResilientZPool _handle_tasks exited')

    @staticmethod
    def _maintain_workers(processes, workers, master_addr, result_addr, initializer,
                          initargs, maxtasksperchild):
        thread = threading.current_thread()

        workers_per_fp = config.cpu_per_job
        left = processes - len(workers)

        logger.debug("ResilientZPool _maintain_workers running, workers %s",
                     workers)

        threads = []
        while left > 0 and thread._state == RUN:

            if left > workers_per_fp:
                n = workers_per_fp
            else:
                n = left

            master_conn = LazyZConnection(("req", master_addr))
            result_conn = LazyZConnection(("w", result_addr))

            #conn = LazyZConnectionReg(("req", master_addr))
            master_conn.set_name("master_conn")
            w = fiber.process.Process(target=zpool_worker,
                                      args=(master_conn,
                                            result_conn,
                                            initializer,
                                            initargs,
                                            maxtasksperchild,
                                            False,
                                            n,
                                            True),
                                      daemon=False)

            w.name = w.name.replace("Process", "PoolWorker")

            td = threading.Thread(target=safe_start, args=(w,))
            td.start()
            threads.append(td)
            logger.debug("started safe_start thread %s", td)

            # w.start()
            logger.debug("started proc %s", w)
            workers.append(w)
            left -= n

        for td in threads:
            logger.debug("joining safe_start thread %s", td)
            td.join(2)
            logger.debug("joining safe_start thread %s finished", td)

        logger.debug("ResilientZPool _maintain_workers finished, workers %s",
                     workers)

    @staticmethod
    def _handle_workers(pool):
        logger.debug("%s _handle_workers running", pool)
        td = threading.current_thread()

        ResilientZPool._maintain_workers(
            pool._processes, pool._pool,
            pool._master_addr, pool._result_addr, pool._initializer,
            pool._initargs, pool._maxtasksperchild
        )

        while td._state == RUN:
            exited_workers = ResilientZPool._join_exited_workers(pool._pool)
            if len(exited_workers) > 0:
                # create new workers when old workers exited
                logger.debug("Exited workers %s", exited_workers)

                ResilientZPool._maintain_workers(
                    pool._processes, pool._pool,
                    pool._master_addr, pool._result_addr, pool._initializer,
                    pool._initargs, pool._maxtasksperchild
                )

                # resubmit tasks

                logger.debug("Resubmitting tasks from failed workers")
                for worker in exited_workers:
                    rid = pool._pid_to_rid[worker.pid]
                    # remove rid from active peers
                    pool._remove_peer(rid)

                    # Take care of pending tasks
                    tasks = pool._pending_table[rid]

                    logger.debug("Failed worker %s tasks %s", worker, tasks)
                    for _, task in tasks.items():
                        # resubmit each task
                        pool._task_put(task)
                        logger.debug("Resubmit task %s", task)

                    # Remove tasks from pending table
                    del pool._pending_table[rid]
                    logger.debug("Remove rid %s from pending table", rid)

            time.sleep(0.5)

        logger.debug("%s _handle_workers finished. Status is not RUN",
                     pool)

    def terminate(self):
        self._task_handler._state = TERMINATE

        super(ResilientZPool, self).terminate()

    def close(self):
        logger.debug('closing pool %s', self)
        if self._state == RUN:
            self._state = CLOSE
            self._worker_handler._state = CLOSE

            for p in self._pool:
                if hasattr(p, '_sentinel'):
                    p._state = CLOSE

            #self._send_sentinels_to_workers()

        #logger.debug("ResilientZPool _send_sentinels_to_workers: "
        #             "send to task handler")
        self._task_put(None)
        self._task_handler._state = CLOSE

    def _send_sentinels_to_workers(self):
        logger.debug("ResilientZPool _send_sentinels_to_workers: "
                     "send to workers")
        data = pickle.dumps(None)
        for ident in self.active_peer_list:
            self._master_sock.send_multipart([ident, b"", data])


#Pool = ZPool
Pool = ResilientZPool
