import pytest
import time
import math
import select
import fiber
import docker
import requests
from fiber import Process
from fiber.docker_backend import Backend

import fiber.config as fiber_config


def func_add(a, b):
    return a + b


def func_error():
    raise ValueError


@pytest.fixture(scope="module")
def client():
    return docker.from_env()


class TimeoutBackend(Backend):
    def __init__(self, n):
        # the first n times calling create_job will result a timeout
        super(TimeoutBackend, self).__init__()
        self.count = 0
        self.n = n

    def create_job(self, job_spec):
        self.count += 1
        if self.count <= self.n:
            raise requests.exceptions.ReadTimeout
        else:
            return super(TimeoutBackend, self).create_job(job_spec)


def square_worker(a):
    return a**2


class TestBasic():
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

    def test_process(self):
        p = Process(target=func_add, args=(42, 89), name="test_process")
        p.start()
        p.join()
        assert 0 == p.exitcode

    def test_process_error(self):
        p = Process(target=func_error, name="test_process_error")
        p.start()
        p.join()
        assert 0 != p.exitcode

    def test_none_process(self):
        p = Process(target=None, name="test_none_process")
        p.start()
        p.join()
        assert 0 == p.exitcode

    def test_multi_processes(self):
        n = 3
        procs = []
        for i in range(n):
            procs.append(Process(target=func_add, args=(42, 89), name="test_multi_processes"))
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        for p in procs:
            assert 0 == p.exitcode

    def test_process_run(self):
        p = Process(target=func_error, name="test_process_run")
        with pytest.raises(ValueError):
            p.run()

    def test_join_timeout(self):
        p = Process(target=func_add, args=(42, 89), name="test_join_timeout")
        p.start()
        now = time.time()
        p.join(1)
        diff = time.time() - now
        # because docker backend status updates are every 1 second. In the
        # worst case, this should be 1 second
        # make sure there is no leaked processes
        p.join()
        assert math.fabs(diff - 1) < 1

    def test_is_alive(self):
        p = Process(target=func_add, args=(42, 89), name="test_is_alive")
        assert p.is_alive() is False
        p.start()
        assert p.is_alive() is True
        p.join()
        assert p.is_alive() is False

    def test_name(self):
        p = Process(target=func_add, args=(42, 89), name="my_process")
        assert p.name == "my_process"

    def test_daemon(self):
        p = Process(target=func_add, args=(42, 89), daemon=True, name="test_daemon")
        p.start()
        assert p.daemon is True
        p.join()

    def test_pid(self):
        p = Process(target=func_add, args=(42, 89), daemon=True, name="test_pid")
        assert p.pid is None
        p.start()
        assert p.pid is not None
        assert p.pid < 32768
        assert p.pid > 0
        p.join()

    def test_authkey(self):
        p = Process(target=func_add, args=(42, 89), daemon=True, name="test_authkey")
        assert p.authkey is not None

    def test_sentinel(self):
        p = Process(target=time.sleep, args=(0.2,), name="test_sentinel")
        p.start()
        fds = select.select([p.sentinel], [], [], 0.1)
        assert len(fds[0]) == 0
        fds = select.select([p.sentinel], [], [])
        assert len(fds[0]) == 1
        assert fds[0][0] == p.sentinel
        p.join()

    def test_terminate(self):
        p = Process(target=time.sleep, args=(1000,), name="test_terminate")
        p.start()
        assert p.is_alive() is True
        p.terminate()
        time.sleep(0.5)
        assert p.is_alive() is False
        # when terminated, exit code can be 0 for k8s backend
        #assert p.exitcode != 0
        p.join()

    def test_exitcode(self):
        p = Process(target=func_add, args=(42, 89), name="test_terminate")
        p.start()
        while True:
            if p.exitcode is not None:
                break
            time.sleep(1)
        assert p.exitcode == 0

    def test_ipc_passive(self):
        if fiber.config.default_backend == "kubernetes":
            # ipc passive is not very useful for current setup, consider disable this completely
            pytest.skip("skipped because current backend is kubernetes")
        # worker process waits for connection from master process
        fiber_config.ipc_active = False
        try:
            p = Process(target=func_add, args=(42, 89), name="test_ipc_passive")
            p.start()
            p.join()
            assert 0 == p.exitcode
        finally:
            fiber_config.ipc_active = True

    def test_start_timeout(self):
        fiber.backend.get_backend(name="docker")
        old_backend = fiber.backend._backends["docker"]
        fiber.backend._backends["docker"] = TimeoutBackend(n=4)

        p = fiber.Pool(4)
        res = p.map(square_worker, [1, 2, 3, 4])
        p.terminate()
        fiber.backend._backends["docker"] = old_backend

        assert res == [i**2 for i in range(1, 5)]

        # wait for 2 seconds to let docker finish starting
        #time.sleep(2)

# name, daemon, pid, authkey, sentinel, terminate
