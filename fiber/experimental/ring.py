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


__all__ = [
    "Ring",
    "RingNode",
]


_manager = None


def _get_manager():
    global _manager
    if _manager is None:
        _manager = fiber.Manager()

    return _manager


# TODO(jiale) convert this into a manager type
class RingNode:
    """
    A `RingNode` represents a node in the `Ring`.

    :param rank: The id assigned to this node. Each node will be assigned a
        unique id called `rank`. Rank 0 is the control node of the `Ring`.
    """
    def __init__(self, rank):
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
    def __init__(self, processes, func, initializer, initargs=None):
        self.size = processes
        self.initializer = initializer
        self.initargs = initargs
        self.func = func
        self.rank = 0

        if getattr(func, "__fiber_meta__", None):
            # Propogate meta info
            # We can't set attributes to bound/unbound methods (PEP 232),
            # so we set it to Ring object
            self.__fiber_meta__ = func.__fiber_meta__

        manager = _get_manager()
        self.members = manager.list([RingNode(i) for i in range(self.size)])

    def _target(self):
        rank = self.rank
        node = self.members[rank]

        backend = get_backend()
        ip, _, _ = backend.get_listen_addr()
        port = random.randint(30000, 50000)

        node.connected = True
        node.ip = ip
        node.port = port
        self.members[rank] = node

        self.initializer(self)
        self.func(rank, self.size)

    def run(self):
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
        p = ctx.Process(target=self._target)
        p.start()
        procs.append(p)

        for i in range(1, self.size):
            self.rank = i
            p = fiber.Process(target=self._target)
            p.start()
            procs.append(p)

        self.rank = rank
        # wait for all processes to finish
        for i in range(self.size):
            procs[i].join()
