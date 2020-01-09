import fiber
import time
import pytest
import docker
from fiber.managers import AsyncManager


@pytest.fixture(scope="module")
def client():
    return docker.from_env()


def f(ns, ls, di):
    ns.x += 1
    ns.y[0] += 1
    ns_z = ns.z
    ns_z[0] += 1
    ns.z = ns_z

    ls[0] += 1
    ls[1][0] += 1  # unmanaged, not assigned back
    ls_2 = ls[2]   # unmanaged...
    ls_2[0] += 1
    ls[2] = ls_2   # ... but assigned back
    ls[3][0] += 1  # managed, direct manipulation

    di[0] += 1
    di[1][0] += 1  # unmanaged, not assigned back
    di_2 = di[2]   # unmanaged...
    di_2[0] += 1
    di[2] = di_2   # ... but assigned back
    di[3][0] += 1  # managed, direct manipulation


class Calculator():
    def add(self, a, b):
        return a + b

    def add_with_delay(self, a, b, delay=1):
        # delay for 1 seconds
        time.sleep(1)
        return a + b


class MyManager(AsyncManager):
    pass


class TestManagers():

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

    # This test is adapted from
    # https://stackoverflow.com/questions/9436757/how-does-multiprocessing-manager-work-in-python

    def test_managers_basic(self):
        manager = fiber.Manager()
        ns = manager.Namespace()
        ns.x = 1
        ns.y = [1]
        ns.z = [1]
        ls = manager.list([1, [1], [1], manager.list([1])])
        di = manager.dict({0: 1, 1: [1], 2: [1], 3: manager.list([1])})

        p = fiber.Process(target=f, args=(ns, ls, di), name="test_managers_basic")
        p.start()
        p.join()
        assert ns.x == 2
        assert ns.y == [1]
        assert ns.z == [2]

        assert ls[0] == 2
        assert ls[1] == [1]
        assert ls[2] == [2]
        assert ls[3][0] == 2
        assert len(ls) == 4

        assert di[0] == 2
        assert di[1] == [1]
        assert di[2] == [2]
        assert di[3][0] == 2

    def test_async_manager_basic(self):
        MyManager.register("Calculator", Calculator)

        manager = MyManager()
        manager.start()

        calc = manager.Calculator()

        res = calc.add(10, 32)

        assert res.get() == 42

        total = 4
        managers = [MyManager() for i in range(total)]

        [manager.start() for manager in managers]

        calculators = [manager.Calculator() for manager in managers]

        start_ts = time.time()
        handles = [calc.add_with_delay(10, 32) for calc in calculators]

        results = [handle.get() for handle in handles]

        duration = time.time() - start_ts

        assert duration < 2
        assert results == [42 for i in range(total)]
