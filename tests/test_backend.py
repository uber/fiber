import os
import fiber.backend
import fiber.config
import fiber.docker_backend
import fiber.kubernetes_backend
import fiber.local_backend


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

    def test_backend_initialization(self):
        doker_backend = fiber.backend.get_backend("docker")
        assert isinstance(doker_backend, fiber.docker_backend.Backend)
        assert doker_backend.name == "docker"

        # kuberntes backend needs k8s config, so it's skipped here
        #k8s_backend = fiber.backend.get_backend("kubernetes", incluster=False)
        #assert isinstance(k8s_backend, fiber.kubernetes_backend.Backend)
        #assert k8s_backend.name == "kubernetes"

        local_backend = fiber.backend.get_backend("local")
        assert isinstance(local_backend, fiber.local_backend.Backend)
        assert local_backend.name == "local"
