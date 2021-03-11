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
import fiber.config as config
from fiber import process
from typing import Optional, Tuple, Callable, Sequence

from fiber.managers import SyncManager
from fiber.pool import ZPool, ResilientZPool
from fiber.queues import SimpleQueuePush, LazyZConnection
from fiber.queues import Pipe

_default_context: "FiberContext"


class FiberContext:
    _name = ""
    Process = process.Process

    current_process = staticmethod(process.current_process)
    active_children = staticmethod(process.active_children)

    def Manager(self) -> SyncManager:
        """Returns a manager associated with a running server process

        The managers methods such as `Lock()`, `Condition()` and `Queue()`
        can be used to create shared objects.
        """
        m = SyncManager()
        m.start()
        return m

    def Pool(
        self,
        processes: int = None,
        initializer: Callable = None,
        initargs: Sequence = (),
        maxtasksperchild: int = None,
        error_handling: bool = False,
    ) -> ZPool:
        """Returns a process pool object"""
        if error_handling:
            return ResilientZPool(
                processes, initializer, initargs, maxtasksperchild
            )
        else:
            return ZPool(processes, initializer, initargs, maxtasksperchild)

    def SimpleQueue(self) -> SimpleQueuePush:
        """Returns a queue object"""
        if config.use_push_queue:
            return SimpleQueuePush()

        # PullQueue is not supported anymore
        raise NotImplementedError

    def Pipe(
        self, duplex: bool = True
    ) -> Tuple[LazyZConnection, LazyZConnection]:
        """Returns two connection object connected by a pipe"""
        return Pipe(duplex)

    def cpu_count(self) -> Optional[int]:
        return os.cpu_count()

    def get_context(self, method: str = None) -> "FiberContext":
        if method is None:
            return self
        if method != "spawn":
            raise ValueError("Fiber only supports spawn context")
        return _concrete_contexts[method]


_concrete_contexts = {"spawn": FiberContext()}

_default_context = _concrete_contexts["spawn"]
