import os
import fiber.backend
import fiber.config


class TestBackend():
    def test_backend_selection(self):
        backend = fiber.backend.auto_select_backend()
        assert backend == fiber.config.default_backend

        try:
            orig_val = os.environ.get("KUBERNETES_SERVICE_HOST", "")
            os.environ["KUBERNETES_SERVICE_HOST"] = "127.0.0.1"

            backend = fiber.backend.auto_select_backend()
            assert backend == "kubernetes"
        finally:
            os.environ["KUBERNETES_SERVICE_HOST"] = orig_val
