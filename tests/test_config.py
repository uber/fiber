import fiber
import fiber.config
import logging
import pytest
import time

import fiber.config as fiber_config


def config_worker(writer):
    cfg = fiber.config.get_dict()
    print("[worker]", cfg)
    writer.send(cfg)
    time.sleep(0.1)


class TestMisc():
    def test_config_file(self):
        config = fiber.config.Config(conf_file="tests/demo_config")
        assert config.image == "demo-image:latest"
        assert config.log_level == logging.INFO
        assert config.ipc_active is False
        assert config.ipc_admin_master_port == 0
        assert config.ipc_admin_worker_port == 8000
        assert config.mem_per_job == 12345

    def test_invalid_key(self):
        with pytest.raises(ValueError, match=r""):
            _ = fiber.config.Config(conf_file="tests/invalid_config")

    def test_config_env(self):
        import os
        old_val = os.environ.get("FIBER_IMAGE", None)
        os.environ["FIBER_IMAGE"] = "image-from-env:latest"
        try:
            fiber.init()
            assert fiber_config.image == "image-from-env:latest"
        finally:
            if old_val is None:
                del os.environ["FIBER_IMAGE"]
            else:
                os.environ["FIBER_IMAGE"] = old_val
            fiber.init()

    def test_config_sync(self):
        # child process should have the same config as parent process
        r, w = fiber.Pipe()
        p = fiber.Process(target=config_worker, args=(w,))
        p.start()

        got = r.recv()
        expected = fiber.config.get_dict()

        p.join()
        assert got == expected
