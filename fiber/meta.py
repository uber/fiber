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


VALID_META_KEYS = ["cpu", "memory", "gpu"]


def post_process(metadata):
    # memory should be in MB
    if "memory" in metadata:
        memory = metadata.pop("memory")
        metadata["mem"] = memory

    return metadata


def meta(**kwargs):
    """
    fiber.meta API allows you to decorate your function and provide some hints to
    Fiber. Currently this is mainly used for specify the resource usage of user
    functions.

    Currently, support keys are:

    | key                   | Type | Default | Notes |
    | --------------------- |:------|:-----|:------|
    | cpu                   | int   | None | The number of CPU cores that this function needs |
    | memory                | int   | None | The size of memory space in MB this function needs |
    | gpu                   | int   | None | The number of GPUs that this function needs. For how to setup Fiber with GPUs, check out [here](advanced.md#using-fiber-with-gpus) |

    Example usage:

    ```python
    @fiber.meta(cpu=4, memory=1000, gpu=1)
    def func():
        do_something()
    ```
    """
    for k in kwargs:
        assert k in VALID_META_KEYS, "Invalid meta argument \"{}\"".format(k)

    def decorator(func):
        meta = post_process(kwargs)
        func.__fiber_meta__ = meta
        return func

    return decorator
