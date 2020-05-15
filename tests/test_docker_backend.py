import fiber
import multiprocessing as mp
import time
import pytest
import docker
import fiber.config
from fiber.docker_backend import Backend
from fiber.util import find_ip_by_net_interface
import psutil
import ipaddress
import socket


def wrap_result(queue):
    backend = Backend()
    r = backend.get_listen_addr()
    queue.put(r)

delay_secs=iter([0, 1, 2, 3])
class DelayedBackend(Backend):
    def create_job(self, job_spec):
        time.sleep(next(delay_secs))
        return super(DelayedBackend, self).create_job(job_spec)


def square_worker(a):
    return a**2


@pytest.fixture(scope="module")
def client():
    return docker.from_env()


def get_ifce(target_ifce):
    ifces = psutil.net_if_addrs()
    ifce = ifces[target_ifce]

    for sincaddr in ifce:
        if sincaddr.family == socket.AF_INET:
            return sincaddr

    return None

@pytest.mark.skipif(fiber.config.default_backend != "docker",
                    reason="skipped because current backend is not docker")
class TestDockerBackend():
    # See https://docs.pytest.org/en/latest/fixture.html#autouse-fixtures-xunit-setup-on-steroids
    @pytest.fixture(autouse=True)
    def check_leak(self, request, client):
        assert fiber.active_children() == []
        pre_workers = client.containers.list()
        yield
        post_workers = client.containers.list()
        assert pre_workers == post_workers


    def test_get_listen_addr_fp(self):
        ifce = get_ifce("docker0")
        network = ipaddress.IPv4Network((ifce.address, ifce.netmask), strict=False)

        q = fiber.SimpleQueue()
        p = fiber.Process(target=wrap_result, args=(q, ), name="test_get_listen_addr")
        p.start()
        res = q.get()
        ip = res[0]
        assert ipaddress.IPv4Address(ip) in network

        p.join()
        assert p.exitcode == 0

    def test_get_listen_addr_mp(self):
        ifce = get_ifce("docker0")
        network = ipaddress.IPv4Network((ifce.address, ifce.netmask), strict=False)

        q = mp.SimpleQueue()
        p = mp.Process(target=wrap_result, args=(q, ), name="test_get_listen_addr")
        p.start()
        p.join()
        assert p.exitcode == 0
        res = q.get()

        ip = res[0]
        assert ipaddress.IPv4Address(ip) in network

    def test_job_creation_with_delay(self):
        fiber.backend.get_backend(name="docker")
        old_backend = fiber.backend._backends["docker"]
        fiber.backend._backends["docker"] = DelayedBackend()

        p = fiber.Pool(4)
        # explicitly start workers instead of lazy start
        p.start_workers()
        p.wait_until_workers_up()

        res = p.map(square_worker, [1, 2, 3, 4])

        p.terminate()
        fiber.backend._backends["docker"] = old_backend

        assert res == [i**2 for i in range(1, 5)]

        # wait for 2 seconds to let docker finish starting
        #time.sleep(2)
        p.join()
