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


import fcntl
import io
import itertools
import logging
import multiprocessing as mp
import os
import socket
import struct
import subprocess
import sys
import threading
import time
from multiprocessing import reduction, spawn, util
from multiprocessing.context import set_spawning_popen

import cloudpickle
import requests

import fiber.util
import fiber.config as config
from fiber.backend import get_backend
from fiber.core import JobSpec
from fiber.core import ProcessStatus

logger = logging.getLogger("fiber")

# each line much ends with `;`
fiber_init_start = """
import sys;
import os;
import socket;
import struct;
os.environ["FIBER_WORKER"]="1";
os.chdir("{cwd}");
import fiber;
import fiber.spawn;
from multiprocessing import spawn, reduction;
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
"""

fiber_init_net_active = """
sock.connect(("{host}", {port}));
conn = sock;
"""

fiber_init_net_passive = """
sock.bind(('', {port}));
sock.listen(0);
conn, _ = sock.accept();
"""

fiber_init_end = """
conn.send(struct.pack("<I", {id}));
fd = conn.fileno();
exitcode = fiber.spawn.spawn_prepare(fd);
sys.exit(exitcode)
"""

fiber_init_start = fiber_init_start.strip()
fiber_init_net_active = fiber_init_net_active.strip()
fiber_init_net_passive = fiber_init_net_passive.strip()
fiber_init_end = fiber_init_end.strip()

admin_host = None
admin_port = None
_fiber_background_thread = None
_fiber_background_thread_lock = threading.Lock()
_event_dict = {}
_event_counter = itertools.count(1)


def get_fiber_init():
    if config.ipc_active:
        fiber_init = fiber_init_start + fiber_init_net_active + fiber_init_end
    else:
        fiber_init = fiber_init_start + fiber_init_net_passive + fiber_init_end

    fiber_init = fiber_init.replace("\n", "").strip()
    return fiber_init


def fiber_background(listen_addr, event_dict):
    global admin_host, admin_port

    # Background thread for handling inter fiber process admin traffic
    ip, port = listen_addr
    host = ""

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    logger.debug("fiber background thread attempt to bind %s:%s", host, port)
    # fix port number (could be 0 previously)
    _, port = sock.getsockname()
    logger.debug("fiber background thread bind addr is to %s:%s", host, port)
    # backlog argument becomes optional since Python 3.5, but in order
    # to make Fiber work with 3.4, here we set a reasonable listen backlog
    # size. See https://docs.python.org/3.4/library/socket.html
    sock.listen(5)

    admin_host = ip
    admin_port = port

    sentinel = event_dict[-1]
    # notifi master thread that background thread is ready
    sentinel.set()
    logger.debug("fiber_background thread ready")
    while True:
        conn, addr = sock.accept()
        logger.debug("got connection from %s", addr)
        # fixed 4 byte id
        buf = conn.recv(4)
        # struct.unpack returns a tuple event if the result only has
        # one element
        ident = struct.unpack("<I", buf)[0]
        event = event_dict.get(ident, None)
        if event is None:
            logger.warn(
                "something is wrong, no event found for this id: %s", ident
            )
            continue
        logger.debug("got connection for id %s", ident)
        event_dict[ident] = conn
        event.set()


def get_python_exe(backend_name):
    if backend_name == "docker":
        # TODO(jiale) fix python path
        python_exe = "/usr/local/bin/python"
    else:
        python_exe = sys.executable

    logger.debug("backend is \"%s\", use python exe \"%s\"",
                 backend_name, python_exe)
    return python_exe


def get_pid_from_jid(jid):
    # Some Linux system has 32768 as max pid number. 32749 is a prime number
    # close to 32768.
    return hash(jid) % 32749


class Popen(object):
    method = "spawn"

    def __del__(self):
        if getattr(self, "ident", None):
            # clean up entry in event_dict
            global _event_dict
            #logger.debug("cleanup entry _event_dict[%s]", self.ident)
            _event_dict.pop(self.ident, None)

    def __repr__(self):
        return "<{}({})>".format(
            type(self).__name__, getattr(self, "process_obj", None)
        )

    def __init__(self, process_obj, backend=None, launch=False):
        self.returncode = None
        self.backend = get_backend()

        ip, _, _ = self.backend.get_listen_addr()

        self.master_host = ip
        self.master_port = config.ipc_admin_master_port
        self.worker_port = config.ipc_admin_worker_port

        self.sock = None
        self.host = ""

        self.job = None
        self.pid = None
        self.process_obj = process_obj
        self._exiting = None
        self.sentinel = None
        self.ident = None

        if launch:
            self._launch(process_obj)

    def launch_fiber_background_thread_if_needed(self):
        global _fiber_background_thread_lock
        _fiber_background_thread_lock.acquire()

        global _fiber_background_thread
        if _fiber_background_thread is not None:
            _fiber_background_thread_lock.release()
            return

        try:
            logger.debug(
                "_fiber_background_thread is None, creating "
                "background thread"
            )
            # Create a background thread to handle incoming connections
            # from fiber child processes
            event = threading.Event()
            event.clear()
            _event_dict[-1] = event
            td = threading.Thread(
                target=fiber_background,
                args=((self.master_host, self.master_port), _event_dict),
                daemon=True,
            )
            td.start()
        except Exception as e:
            raise e
        finally:
            logger.debug("waiting for background thread")
            event.wait()
            logger.debug(
                "master received message that fiber_background thread is ready"
            )
            _fiber_background_thread = td
            _fiber_background_thread_lock.release()

    def get_command_line(self, **kwds):
        """Returns prefix of command line used for spawning a child process."""
        prog = get_fiber_init()
        prog = prog.format(**kwds)
        opts = util._args_from_interpreter_flags()
        if config.debug:
            return (
                [get_python_exe(self.backend.name)]
                + opts
                + ["-u", "-c", prog, "--multiprocessing-fork"]
            )

        return (
            [get_python_exe(self.backend.name)]
            + opts
            + ["-c", prog, "--multiprocessing-fork"]
        )

    def _accept(self):
        conn, addr = self.sock.accept()
        logger.debug("successfully accept")
        # TODO verify if it's the same client
        return conn

    def _get_job(self, cmd):
        spec = JobSpec(
            command=cmd,
            image=config.image,
            name=self.process_obj.name,
            cpu=config.cpu_per_job,
            mem=config.mem_per_job,
        )
        if hasattr(self.process_obj._target, "__self__"):
            metadata = getattr(
                self.process_obj._target.__self__, "__fiber_meta__", None
            )
        else:
            metadata = getattr(self.process_obj._target, "__fiber_meta__", None)
        if metadata:
            for k, v in metadata.items():
                setattr(spec, k, v)

        return spec

    def _run_job(self, job):
        try:
            job = self.backend.create_job(job)
        except requests.exceptions.ReadTimeout as e:
            raise mp.TimeoutError(str(e))
        self.job = job

        return job

    def _sentinel_readable(self, timeout=0):
        # Use fcntl(fd, F_GETFD) instead of select.* becuase:
        # * select.select() can't work with fd > 1024
        # * select.poll() is not thread safe
        # * select.epoll() is Linux only
        # Also, fcntl(fd, F_GETFD) is cheaper than the above calls.
        return fcntl.fcntl(self.sentinel, fcntl.F_GETFD)

    def poll(self, flag=os.WNOHANG):

        # returns None if the process is not stopped yet. Otherwise, returns
        # process exit code.
        if flag != os.WNOHANG:
            raise NotImplementedError("flag {} is not supported".format(flag))

        # no need to wait here. get_job_status will be called later
        # if self._exiting:
        #     logger.debug("[poll] self._exiting is True, calling self.wait.")
        #     return self.wait(timeout=1)

        if self.job is None:
            # self.job is None meaning this process hasn't been fully started
            # yet.
            # logger.debug("[poll] self.job is None, return.")
            return None

        if self.returncode is None:
            if self.sentinel is not None:
                if not self._sentinel_readable(0):
                    logger.debug(
                        "self.sentinel not readable, process is still up"
                    )

                    # process is still up
                    return None
                logger.debug("self.sentinel readable, process is probably down")

            logger.debug("self.sentinel is None, getting status")
            status = self.backend.get_job_status(self.job)
            # When exiting, make sure to call wait to get exit code
            if status != ProcessStatus.STOPPED and (not self._exiting):
                return None
        return self.wait(timeout=0)

    def wait(self, timeout=None):
        if self.job is None:
            # self.job is None meaning this process hasn't been fully started
            # yet.
            # TODO(jiale) timeout is ignored here, but since the job hasn't
            # been started yet, there is nothing to wait. This can happen when
            # Process.start() is called in another thread and usually it takes
            # a long time to start a new Process, so self.job can be empty.
            return None

        if self.returncode is None:
            code = self.backend.wait_for_job(self.job, timeout)
            if code is None:
                return code
            assert type(code) is int
            self.returncode = code
        return self.returncode

    def _pickle_data(self, data, fp):
        if fiber.util.is_in_interactive_console():
            logger.debug("in interactive shell, use cloudpickle")
            cloudpickle.dump(data, fp)
        else:
            logger.debug("not in interactive shell, use reduction")
            reduction.dump(data, fp)

    def _launch(self, process_obj):
        logger.debug("%s %s _launch called", process_obj, self)

        if config.ipc_active:
            logger.debug("%s ipc_active is set, launch background thread", self)
            self.launch_fiber_background_thread_if_needed()
        else:
            logger.debug("%s ipc_active is not set", self)

        # Setup networking
        ident = next(_event_counter)
        self.ident = ident
        # this needs to happen after self._setup_listen where port is decided
        global admin_host, admin_port
        if config.ipc_active:
            # in active mode, port is admin_port which could be 0 at first and
            # set to the actual port by the backend thread
            port = admin_port
        else:
            # in passive mode, port can't be 0 because this need to be
            # pre-determined information between master and workers.
            assert self.worker_port != 0, (
                "port can't be 0 because this "
                "need to be pre-determined information between master "
                "and workers."
            )
            port = self.worker_port

        if config.ipc_active:
            assert admin_host is not None
            assert admin_port is not None

        cmd = self.get_command_line(
            cwd=os.getcwd(), host=admin_host, port=port, id=ident
        )

        job = self._get_job(cmd)

        event = threading.Event()
        event.clear()
        _event_dict[ident] = event
        logger.debug(
            "%s popen_fiber_spawn created event %s and set _event_dict[%s]",
            self,
            event,
            ident,
        )

        # prepare data and serialize
        prep_data = spawn.get_preparation_data(process_obj.name)
        prep_data["fiber_config"] = config.get_object()
        # TODO(jiale) what is a better way to setup sys_path inside containers?
        prep_data.pop("sys_path", None)
        logger.debug("%s prep_data: %s", self, str(prep_data))
        fp = io.BytesIO()
        set_spawning_popen(self)
        try:
            self._pickle_data(prep_data, fp)
            self._pickle_data(process_obj, fp)
        finally:
            set_spawning_popen(None)

        # Set process_obj._popen = self here so that we have have the
        # Process <-> Popen binding before _run_job() is called. After
        # _run_job() is called, we started a job with Fiber backend. So
        # Process <-> Popen has to exist so that Process.terminate() can
        # find Popen object and terminate the underlying job. Also, when
        # we serize process_obj above, we have to make sure that
        # Process <-> Popen binding doesn't exist so that Popen doesn't get
        # serialized becuase 1) it's not necessary and 2) some part of Popen
        # cannot be pickled.
        process_obj._popen = self

        # launch job
        job = self._run_job(job)
        self.pid = get_pid_from_jid(job.jid)
        # Fix process obj's pid
        process_obj.ident = self.pid
        post_data = {"pid": self.pid}
        self._pickle_data(post_data, fp)

        send_buffer = fp.getbuffer()

        if config.ipc_active:
            # (worker) active mode, wait for event to be set by
            # background thread
            done = False
            while not done:
                if self._exiting:
                    logger.debug(
                        "process is exiting, don't wait for job to "
                        "connect back"
                    )
                    return
                done = event.wait(0.5)
                status = self.check_status()
                if status == ProcessStatus.STOPPED:
                    return

            logger.debug(
                "popen_fiber_spawn is waiting for accept event %s to finish",
                event,
            )
            conn = _event_dict[ident]
            logger.debug("got conn from _event_counter[%s]", ident)
            del _event_dict[ident]
            logger.debug("remove entry _event_counter[%s]", ident)
        else:
            # (worker) passive mode, check job status until it's ready
            while True:
                if self._exiting:
                    logger.debug(
                        "process is exiting, don't wait for job to start"
                    )
                    return
                status = self.check_status()
                if status == ProcessStatus.STARTED:
                    break
                logger.debug(
                    "waiting 1s for job to start, current status: %s", status
                )
                time.sleep(1)
            # connect to woker job
            if job.host is None:
                job.update()
            ip = job.host
            port = self.worker_port
            addr = (ip, port)

            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            while True:
                if self._exiting:
                    logger.debug(
                        "process is exiting, don't try to connect to the job"
                    )
                    return
                try:
                    logger.debug("connecting to %s", addr)
                    conn.connect(addr)
                except ConnectionRefusedError:
                    # not ready yet, sleep
                    logger.debug(
                        "Fiber worker is not up yet, waiting 1s for the "
                        "worker to listen on admin address: %s",
                        addr,
                    )
                    time.sleep(1)
                    continue
                break

        logger.debug("send buffer")
        conn.send(send_buffer)

        # logger.debug("sent fp.getbuffer() to sub process")

        self.sentinel = conn
        logger.debug("_launch finished")

    def check_status(self):
        status = self.backend.get_job_status(self.job)
        if status == ProcessStatus.STOPPED:
            # something happened that caused Fiber process to hit an early stop
            msg = self.backend.get_job_logs(self.job)
            logger.error(
                "Failed to start Fiber process %s. Logs from inside the "
                "container:\n%s",
                self.process_obj.name,
                msg,
            )

        return status

    def terminate(self):
        logger.debug("[Popen]terminate() called")

        self._exiting = True
        if self.job is not None:
            logger.debug(
                "[Popen]terminate():self.job is not None, terminate job"
            )
            self.backend.terminate_job(self.job)
        else:
            logger.error("[Popen]terminate():self.job is None, terminate job")
            # raise RuntimeError("[Popen]terminate():self.job is "
            #                    "None %s", self.process_obj)
