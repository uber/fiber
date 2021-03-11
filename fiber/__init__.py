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


import sys
import os
import fiber.config as fiber_config
import logging
from fiber import context
from fiber.init import init_fiber
from fiber.meta import meta
from typing import List
from typing import TYPE_CHECKING

__version__: str
_in_interactive_console: bool
_names: List[str]


__version__ = "0.2.1"

logger = logging.getLogger('fiber')


if sys.platform == 'win32':
    raise NotImplementedError("win32 platform is not supported now")


init_fiber()

if os.environ.get("FIBER_WORKER", None) is None:
    # Only initialize logger when fiber is imported in master process. Worker
    # process will get their logger initialized later.
    # A file based logger could be created, no logging lines after
    # this in this module.
    logger.setLevel(fiber_config.log_level)


if hasattr(sys, 'ps1'):
    _in_interactive_console = True
else:
    _in_interactive_console = False


def reset() -> None:
    init_fiber()


def init(**kwargs) -> None:
    """
    Initialize Fiber. This function is called when you want to re-initialize
    Fiber with new config values and also re-init loggers.

    :param kwargs: If kwargs is not None, init Fiber system with corresponding
        key/value pairs in kwargs as config keys and values.
    """
    init_fiber(**kwargs)


_names = [x for x in dir(context._default_context) if x[0] != "_"]
globals().update((name, getattr(context._default_context, name))
                 for name in _names)
__all__ = _names + []


if TYPE_CHECKING:
    current_process = context.FiberContext.current_process
    active_children = context.FiberContext.active_children
    Process = context.FiberContext.Process
    Manager = context.FiberContext.Manager
    Pool = context.FiberContext.Pool
    SimpleQueue = context.FiberContext.SimpleQueue
    Pipe = context.FiberContext.Pipe
    cpu_count = context.FiberContext.cpu_count
    get_context = context.FiberContext.get_context
