import re
import io
import os
import logging
import glob
import fiber
import fiber.config
from fiber.docker_backend import Backend
import time
import pytest
import multiprocessing
from unittest import mock

import fiber.config as fiber_config

def pytest_sessionstart(session):
    assert 1 == 0
    print("session start")


def pytest_sessionfinish(session, exitstatus):
    assert 1 == 0
    print("session finished")


def import_missing_module_worker():
    import this_module_does_not_exist


def dummy_q_worker(q):
    q.put("DUMMY_WORKER_DONE")
    time.sleep(0.1)


class TestMisc():
    backend = Backend()

    # Throw an error so that the job won't be started. `python:3.6-alpine`
    # doesn't have proper depencency installed like `pytest`
    @mock.patch.object(fiber.docker_backend.Backend, 'create_job',
                       side_effect=NotImplementedError)
    def test_init_image(self, mock_create_job):
        if fiber.config.default_backend != "docker":
            pytest.skip("skipped because current backend is not docker")

        try:
            fiber.init(image='python:3.6-alpine', backend='docker')
            p = fiber.Process(name="test_init_image")
            p.start()
        except NotImplementedError:
            pass
        finally:
            fiber.reset()
        args = mock_create_job.call_args
        # https://docs.python.org/3/library/unittest.mock.html#unittest.mock.Mock.call_args # noqa E501
        job_spec_arg = args[0][0]
        assert job_spec_arg.image == "python:3.6-alpine"

    @mock.patch.object(fiber.docker_backend.Backend, 'create_job',
                       side_effect=NotImplementedError)
    def test_init_image2(self, mock_create_job):
        if fiber.config.default_backend != "docker":
            pytest.skip("skipped because current backend is not docker")

        # don't set backend when init

        try:
            fiber.init(image='python:3.6-alpine')
            p = fiber.Process(name="test_init_image2")
            p.start()
        except NotImplementedError:
            pass
        finally:
            fiber.reset()
        args = mock_create_job.call_args
        # https://docs.python.org/3/library/unittest.mock.html#unittest.mock.Mock.call_args # noqa E501
        job_spec_arg = args[0][0]
        assert job_spec_arg.image == "python:3.6-alpine"

    def test_interactive_shell_cloudpickle(self):
        def add(a, b):
            return a + b

        # This will fail: "AttributeError: Can't pickle local object"
        # default pickler can pickle local object
        with pytest.raises(AttributeError):
            p = fiber.Process(target=add, args=(1, 2),
                              name="test_interactive_shell_cloudpickle")
            p.start()

        # This will work. When using interactive console, pickler
        # is set to cloudpickle

        with mock.patch("fiber.util.is_in_interactive_console") as func:
            func.return_value = True

            p = fiber.Process(target=add, args=(1, 2),
                              name="test_interactive_shell_cloudpickle2")
            p.start()
            p.join()

    def test_image_not_found(self):
        if fiber.config.default_backend != "docker":
            pytest.skip("skipped because current backend is not docker")

        try:
            with pytest.raises(multiprocessing.ProcessError):
                fiber.init(
                    image='this-image-does-not-exist-and-is-only-used-for-testing'
                )
                p = fiber.Process(name="test_image_not_found")
                p.start()
        finally:
            fiber.reset()

    """
    def test_push_pull_queue(self):
        # Use pull queue by default
        fiber.reset()
        q = fiber.SimpleQueue()
        assert isinstance(q, fiber.queues.SimpleQueuePull)

        try:
            fiber.init(use_push_queue=True)
            q = fiber.SimpleQueue()
            assert isinstance(q, fiber.queues.SimpleQueuePush)
        finally:
            fiber.reset()

        try:
            fiber.init(use_push_queue=False)
            q = fiber.SimpleQueue()
            assert isinstance(q, fiber.queues.SimpleQueuePull)
        finally:
            fiber.reset()
    """

    # Instead of raising an exception, Fiber now just print an error log
    def test_no_python3_inside_image(self):
        if fiber.config.default_backend != "docker":
            pytest.skip("skipped because current backend is not docker")

        '''
        fp = io.StringIO()
        handler = logging.StreamHandler(fp)
        logger = logging.getLogger("fiber")
        logger.setLevel(level=logging.DEBUG)
        logger.addHandler(handler)
        '''

        with pytest.raises(multiprocessing.ProcessError):
            try:
                fiber.init(image='ubuntu:18.04')
                p = fiber.Process(name="test_no_python3_inside_image")
                p.start()
            finally:
                fiber.reset()
                #logger.removeHandler(handler)

        '''
        try:
            fiber.init(image='ubuntu:18.04')
            p = fiber.Process(name="test_no_python3_inside_image")
            p.start()

            logs = fp.getvalue()
            times = len(list(re.finditer('Failed to start Fiber process',
                                         logs)))
            assert times == 1
        finally:
            fiber.reset()
            logger.removeHandler(handler)
        '''

    """
    def test_missing_module_inside_image(self):
        with pytest.raises(multiprocessing.ProcessError):
            p = fiber.Process(target=import_missing_module_worker, name="test_missing_module_inside_image")
            p.start()
    """

    def test_log_file_path(self):
        if fiber.config.default_backend != "docker":
            pytest.skip("skipped because current backend is not docker")

        # make sure workers and master doesn't write to the same log file
        try:
            log_file = fiber_config.log_file
            log_level = fiber_config.log_level
            # clean up
            if os.path.isdir("tests/logs"):
                files = glob.glob("tests/logs/*")
                for f in files:
                    os.remove(f)
            files = glob.glob("tests/logs/*")
            assert files == []

            fiber.init(log_file="tests/logs/fiber.log", log_level="DEBUG")
            q = fiber.SimpleQueue()
            p = fiber.Process(target=dummy_q_worker, args=(q,))
            p.start()
            msg = q.get()

            assert msg == "DUMMY_WORKER_DONE"

            files = glob.glob("tests/logs/*")
            files.sort()
            assert len(files) == 2
            assert files[0] == "tests/logs/fiber.log.MainProcess"
            assert "tests/logs/fiber.log.Process-" in files[1]

            regex = r'\w+:Process-\d+\(\d+\):'
            with open("tests/logs/fiber.log.MainProcess") as f:
                content = f.read()
                assert re.search(regex, content) is None, (
                    "Fiber subprocess log found in master log file")

            p.join()

        finally:
            fiber.init(log_file=log_file, log_level=log_level)
