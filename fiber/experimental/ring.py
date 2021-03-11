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

"""
Ring allows you to create a Ring like topology easily on a computer cluster.
"""

import multiprocessing as mp
import random

import fiber
from fiber.backend import get_backend
from typing import Any, NoReturn, Callable, Sequence, Dict, Iterator, Optional, List

_manager: Any


__all__ = [
    "Ring",
    "RingNode",
]


_manager = None


def _get_manager() -> fiber.managers.BaseManager:
    global _manager
    if _manager is None:
        _manager = fiber.Manager() # type: ignore

    return _manager


# TODO(jiale) convert this into a manager type
class RingNode:
    """
    A `RingNode` represents a node in the `Ring`.

    :param rank: The id assigned to this node. Each node will be assigned a
        unique id called `rank`. Rank 0 is the control node of the `Ring`.
    """
    def __init__(self, rank) -> None:
        self.rank = rank
        self.connected = False
        self.ip = None
        self.port = None


class Ring:
    """
    Create a ring of nodes (processes). Each node runs a copy of the same
    function.

    :param processes: Number of ring nodes in this Ring.
    :param func: the target function that each ring node will run.
    :param initializer: the initialization function that each ring node runs.
        This is used to setup the custom ring structure like PyTorch's
        torch.distributed, Horovod, etc.
    :param initargs: positional arguments that are passed to initializer.
        Currently this is not used.
    """

    __fiber_meta__: Dict

    def __init__(
        self,
        processes: int,
        func: Callable[[int, int], Any],
        initializer: Callable[[Optional[Iterator[Any]]], None],
        initargs: Iterator[Any] = None
    ) -> None:

        self.size = processes
        self.initializer = initializer
        self.initargs = initargs
        self.func = func
        self.rank = 0

        if getattr(func, "__fiber_meta__", None):
            # Propogate meta info
            # We can't set attributes to bound/unbound methods (PEP 232),
            # so we set it to Ring object
            self.__fiber_meta__ = func.__fiber_meta__ # type: ignore

        manager = _get_manager()
        self.members = manager.list([RingNode(i) for i in range(self.size)]) # type: ignore

    def _target(self) -> None:
        rank = self.rank
        node = self.members[rank]

        backend = get_backend()
        ip, _, _ = backend.get_listen_addr()
        port = random.randint(30000, 50000)

        node.connected = True
        node.ip = ip
        node.port = port
        self.members[rank] = node

        self.initializer(self.initargs)
        self.func(rank, self.size)

    def run(self) -> None:
        """
        Start this Ring. This will start the ring 0 process on the same machine
        and start all the other ring nodes with Fiber processes.
        """
        if self.size <= 0:
            return

        procs = []
        rank = self.rank
        # Start process rank 0
        self.rank = 0
        ctx = mp.get_context("spawn")
        p0 = ctx.Process(target=self._target)
        p0.start()
        procs.append(p0)

        for i in range(1, self.size):
            self.rank = i
            p = fiber.Process(target=self._target)
            p.start()
            procs.append(p) # type: ignore[arg-type]

        self.rank = rank
        # wait for all processes to finish
        for i in range(self.size):
            procs[i].join()
