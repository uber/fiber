import pytest
import time
import fiber
from fiber import Process
from fiber.queues import Pipe, SimpleQueue
import docker


def get_current_pid(q):
    import fiber
    pid = q.get()
    assert fiber.current_process().pid == pid


@pytest.fixture(scope="module")
def client():
    return docker.from_env()


class TestMisc():

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

    def test_active_children(self):
        # Reset fiber module so other processes won't inteference with
        # current run
        import importlib
        importlib.reload(fiber)
        fiber.process._children = set()
        try:
            p1 = Process(target=time.sleep, args=(1,), name="test_active_children1")
            p1.start()
            p2 = Process(target=time.sleep, args=(1,), name="test_active_children2")
            p2.start()
            assert len(fiber.active_children()) == 2
        finally:
            p1.join()
            p2.join()

    def test_cpu_count(self):
        c = fiber.cpu_count()
        assert c > 0 and c != 0

    def test_current_process(self):
        c = fiber.current_process()
        assert c.pid > 0

    def test_current_process_nested(self):
        q = SimpleQueue()
        p = Process(target=get_current_pid, args=(q,), name="test_active_children")
        p.start()
        q.put(p.pid)
        p.join()
        assert p.exitcode == 0
