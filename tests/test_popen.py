import multiprocessing as mp
import io
import re
import time
import logging
import socket
import fiber
import fiber.popen_fiber_spawn as fspawn
import docker
import pytest


@pytest.fixture(scope="module")
def client():
    return docker.from_env()


class TestPopen():

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

    def test_get_python_exe(self):
        b1 = fspawn.get_python_exe("docker")
        assert b1 == "/usr/local/bin/python"

    @pytest.mark.skip(reason="testing everytime is not needed")
    def test_command_line(self):
        fiber.init(use_bash=False)
        try:
            p = fiber.Process(name="test_command_line")
            popen = fspawn.Popen(p, backend="docker", launch=False)

            cmd = popen.get_command_line(host="127.0.0.1", port="8000", id=1)
        finally:
            fiber.init(use_bash=True)
        assert cmd == ["/usr/local/bin/python", "-c", 'import sys;import os;import socket;import struct;import fiber;import fiber.util;from multiprocessing import spawn, reduction;sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM);sock.connect(("127.0.0.1", 8000));conn = sock;conn.send(struct.pack("<I", 1));fd = conn.fileno();exitcode = fiber.util.spawn_prepare(fd);sys.exit(exitcode)', '--multiprocessing-fork']

    def test_get_job(self):
        p = fiber.Process(name="test_get_job")
        popen = fspawn.Popen(p, launch=False)

        job = popen._get_job(["echo", "hello fiber"])
        assert job.command == ["echo", "hello fiber"]
        #assert job.image == "fiber-test:latest"
        # by default, do image should be provided. Backend should decide if
        # to use default image or current container image.
        assert job.image is None

    def test_popen_method(self):
        # make sure that fiber works with multiprocessing
        p = fiber.Process(name="test_popen_method")
        popen = p._Popen(p)
        assert isinstance(popen, fiber.popen_fiber_spawn.Popen)

        p = mp.Process(name="test_popen_method2")
        popen = p._Popen(p)
        assert popen.method == "fork"
        assert isinstance(popen, mp.popen_fork.Popen)

    def test_launch_fiber_background_thread(self):
        # reset global variable
        fiber.popen_fiber_spawn._fiber_background_thread = None

        fp = io.StringIO()
        handler = logging.StreamHandler(fp)
        logger = logging.getLogger("fiber")
        logger.setLevel(level=logging.DEBUG)
        logger.addHandler(handler)

        try:
            procs = []
            for i in range(4):
                p = fiber.Process(target=time.sleep, args=(1,), name="test_launch_fiber_background_thread_{}".format(i))
                procs.append(p)
            for p in procs:
                p.start()

            logs = fp.getvalue()
            times = len(list(re.finditer('creating background thread', logs)))
            assert(times == 1)
        finally:
            for p in procs:
                p.join()
            logger.removeHandler(handler)

    def test_fiber_with_more_than_1024_fds(self):
        # select.select can't handle fd >= 1024
        # https://stackoverflow.com/questions/7328165/how-do-you-compile-python-with-1024-file-descriptors
        import resource
        resource.setrlimit(resource.RLIMIT_OFILE, (8192, 8192))

        socks = []
        for i in range(1023):
            s = socket.socket(socket.AF_INET)
            socks.append(s)

        p = fiber.Process(target=time.sleep, args=(10,), name="test_fiber_with_more_than_1024_fds")
        p.start()

        # if select is used, we should see errors like:
        # `ValueError: filedescriptor out of range in select()`
        print(p.exitcode)

        p.terminate()
