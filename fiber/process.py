# Copyright 2020 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Fiber introduces a new concept called *job-backed processes*. It is similar to
the *process* in Python's multiprocessing library, but more flexible: while a
process in multiprocessing only runs on a local machine, a Fiber process can
run remotely on a different machine or locally on the same machine.

When starting a new Fiber process, Fiber creates a new job with the proper Fiber
backend on the current computer cluster. Fiber uses containers to encapsulate
the running environment of current processes, including all the required files,
input data, and other dependent program packages, etc., to ensure everything is
self-contained. All the child processes are started with the same container
image as the parent process to guarantee a consistent running environment.

Because each process is a cluster job, its life cycle is the same as any job on
the cluster.
"""

import sys
import fiber.util
import itertools
import multiprocessing as mp
import logging

from multiprocessing.process import BaseProcess


logger = logging.getLogger('fiber')


_children = set()
_current_process = mp.current_process()


def _cleanup():
    # check for processes which have finished
    for p in list(_children):
        if p._popen.poll() is not None:
            _children.discard(p)


def active_children():
    """
    Get a list of children processes of the current process.

    :returns: A list of children processes.

    Example:
    ```python
    p = fiber.Process(target=time.sleep, args=(10,))
    p.start()
    print(fiber.active_children())
    ```
    """
    _cleanup()
    return list(_children)


def current_process():
    """Return a Process object representing the current process.

    Example:
    ```python
    print(fiber.current_process())
    ```
    """
    return _current_process


class Process(BaseProcess):
    """
    Create and manage Fiber processes.

    The API is compatible with Python's multiprocessing.Process, check
    [here](https://docs.python.org/3.6/library/multiprocessing.html#process-and-exceptions)
    for multiprocessing's documents.

    Example usage:

    ```python
    p = Process(target=f, args=('Fiber',))
    p.start()
    ```

    ---

    ###pid

    ```
    Process.pid
    ```

    Return the current process PID. If the process hasn't been fully started,
    the value will be `None`. This PID is assigned by Fiber and is different
    from the operating system process PID. The value of `pid` is derived from
    the job ID of the underlying job that runs this Fiber process.

    ###name
    ```
    Process.name
    ```

    Return the name of this Fiber process. This value need to be set before the
    start of the Process.

    Example:
    ```python
    p = fiber.Process(target=print, args=("demo"))
    p.name = "DemoProcess"
    ```

    ###daemon

    ```
    Process.daemon
    ```

    In multiprocessing, if a process has daemon set, it will be terminated when
    it's parent process exits. In Fiber, current this value has no effect, all
    processes will be cleaned up when their parent exits.

    ###authkey

    ```
    Process.authkey
    ```
    Authkey is used to authenticate between parent and child processes. It is
    a byte string.

    ###exitcode

    ```
    Process.exitcode
    ```
    The exit code of current process. If the process has not exited, the
    exitcode is `None`. If the current process has exited, the value of this
    is an integer.

    ###sentinel

    ```
    Process.sentinel
    ```
    Returns a file descriptor that becomes "ready" when the process exits. You
    can call `select` and other eligible functions that works on fds on this
    file descriptor.
    """
    _start_method = None
    _pid = None

    @staticmethod
    def _Popen(process_obj):
        from .popen_fiber_spawn import Popen
        return Popen(process_obj)

    def __init__(self, group=None, target=None, name=None, args=(), kwargs={},
                 *, daemon=None):
        super(Process, self).__init__(group=group, target=target, name=name,
                                      args=args, kwargs=kwargs, daemon=daemon)
        self._parent_pid = current_process().pid
        # set when Process.start() failed
        self._start_failed = False

    def __repr__(self):
        return "{}({}, {})>".format(type(self).__name__, self._name, self.ident)

    def run(self):
        """Run the target function of current process (in current process)

        :returns: return value of the target function
        """
        return super().run()

    def start(self):
        """Start this process.

        Under the hood, Fiber calls the API on the computer cluster to start a
        new job and run the target function in the new job.
        """
        assert self._popen is None, 'cannot start a process twice'
        assert self._parent_pid == current_process().pid, \
            'can only start a process object created by current process'
        assert not _current_process._config.get('daemon'), \
            'daemonic processes are not allowed to have children'
        _cleanup()

        _popen = self._Popen(self)
        # put waiting logic in another function so that self._popen can be set
        # earlier. This make sure that clean up code can find self._popen.
        logger.debug("create new process %s", self)
        _popen._launch(self)

        # This is not needed because Process._popen is set inside _launch()
        # function
        # self._popen = _popen
        logger.debug("process %s, set sentinel %s",
                     self, self._popen.sentinel)
        self._sentinel = self._popen.sentinel
        # Avoid a refcycle if the target function holds an indirect
        # reference to the process object (see bpo-30775)
        del self._target, self._args, self._kwargs
        _children.add(self)

    def terminate(self):
        """Terminate current process.

        When running locally, Fiber sends an SIGTERM signal to the child
        process. When running on a computer cluster, Fiber calls the
        corresponding API on that platform to terminate the job that runs Fiber
        process.
        """
        if self._popen is None:
            logger.debug("self._popen is None when terminate() is called")
            return
        self._popen.terminate()

    def join(self, timeout=None):
        """Wait for this process to terminate.

        :param timeout: The maximum duration of time in seconds that this call
            should wait before return. if `timeout` is `None` (default value),
            this method will wait forever until the process terminates. If
            `timeout` is `0`, it will check if the process has exited and
            return immediately.

        :returns: The exit code of this process
        """
        return super().join(timeout=timeout)

    def is_alive(self):
        """Check if current process is still alive

        :returns: `True` if current process is still alive. Returns `False` if
            it's not alive.
        """
        return super().is_alive()

    @property
    def ident(self):
        if self._pid is None:
            self._pid = self._popen and self._popen.pid

        return self._pid

    @ident.setter
    def ident(self, pid):
        self._pid = pid

    pid = ident

    def _bootstrap(self):
        from multiprocessing import util, context
        global _current_process, _process_counter, _children
        err = None

        try:
            if self._start_method is not None:
                context._force_start_method(self._start_method)
            _process_counter = itertools.count(1)
            _children = set()
            util._close_stdin()
            old_process = _current_process
            _current_process = self

            try:
                fiber.util._finalizer_registry.clear()
                fiber.util._run_after_forkers()
            except Exception as e:
                err = e
            finally:
                # delay finalization of the old process object until after
                # _run_after_forkers() is executed
                del old_process
            util.info('child process calling self.run()')
            try:
                self.run()
                exitcode = 0
            except Exception as e:
                import traceback
                err = traceback.format_exc()
                exitcode = 1
            finally:
                util._exit_function()
        except SystemExit as e:
            err = None
            if not e.args:
                exitcode = 0
            elif isinstance(e.args[0], int):
                exitcode = e.args[0]
            else:
                sys.stderr.write(str(e.args[0]) + '\n')
                exitcode = 1
        except Exception as e: # noqa E722
            err = e
            exitcode = 1
            import traceback
            msg = traceback.format_exc()
            sys.stderr.write('Process {} exception: {}\n'.format(
                self.name, msg))
        finally:
            try:
                sys.stdout.flush()
            except (AttributeError, ValueError):
                pass
            try:
                sys.stderr.flush()
            except (AttributeError, ValueError):
                pass

        return exitcode, err
