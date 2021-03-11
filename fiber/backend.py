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


import os
import importlib
import multiprocessing as mp

import fiber.config as config
from fiber.core import Backend

_backends: dict


_backends = {}
available_backend = ['kubernetes', 'docker', 'local']


def is_inside_kubenetes_job() -> bool:
    if os.environ.get("KUBERNETES_SERVICE_HOST", None):
        return True
    return False


def is_inside_docker_job() -> bool:
    if os.environ.get("FIBER_BACKEND", "") == "docker":
        return True
    return False


BACKEND_TESTS = {
    "kubernetes": is_inside_kubenetes_job,
    "docker": is_inside_docker_job,
}


def auto_select_backend() -> str:
    for backend_name, test in BACKEND_TESTS.items():
        if test():
            name = backend_name
            break
    else:
        name = config.default_backend

    return name


def get_backend(name=None, **kwargs) -> Backend:
    """
    Returns a working Fiber backend. If `name` is specified, returns a
    backend specified by `name`.
    """
    global _backends
    if name is None:
        if config.backend is not None:
            name = config.backend
        else:
            name = auto_select_backend()
    else:
        if name not in available_backend:
            raise mp.ProcessError("Invalid backend: {}".format(name))

    _backend = _backends.get(name, None)
    if _backend is None:
        backend_name = "fiber.{}_backend".format(name)
        backend_module = importlib.import_module(backend_name)
        _backend = backend_module.Backend(**kwargs) # type: ignore
        _backends[name] = _backend
    return _backend
