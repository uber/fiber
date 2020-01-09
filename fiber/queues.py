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
*Queues and pipes* in Fiber behave the same as in multiprocessing. The
difference is that queues and pipes are now shared by multiple processes
running on different machines.  Two processes can read from and write to the
same pipe. Furthermore, queues can be shared between many processes on
different machines and each process can send to or receive from the same queue
at the same time. Fiber's queue is implemented with
[Nanomsg](https://nanomsg.org/), a high-performance asynchronous message queue
system.

We implemented two version of `SimpleQueue`: `SimpleQueuePush` and
`SimpleQueuePull`.  To decide which queue to use, you can set
`fiber.config.use_push_queue` to `True` to use `SimpleQueuePush` or `False` to
use `SimpleQueuePull`.

The difference between `SimpleQueuePush` and
`SimpleQueuePull` is whether data is push from master process to sub-processes
or data is requested by sub-processes. When `SimpleQueuePull` is used,
sub-processes will send an message to the master process to notify master
process that it is ready. And the master process will send the data to
sub-processes. In this way, data in the queue are load-balanced between
different sub-processes due to the workload of them. When `SimpleQueuePush` is
used, all the data are push to the sub-processes as soon as possible and the
data are load-balanced in a round-robin manner. This way, there is less
overhead for sending the data to the client, but there may be uneven load
between each of the sub-processes.

Example:
```python
q = fiber.SimpleQueue()
q.put(42)

# By default, `SimpleQueue` is `SimpleQueuePull`, now we switch to
# `SimpleQueuePush`
fiber.config.use_push_queue = True
q = fiber.SimpleQueue()

```

"""


import logging
import multiprocessing.connection
import multiprocessing.reduction as reduction
from multiprocessing.util import register_after_fork as mp_register_after_fork
import atexit

from fiber.process import current_process
from fiber.backend import get_backend
from fiber.socket import Socket, ProcessDevice

# define the port range that fiber can use to listen for incoming connections
MIN_PORT = 40000
MAX_PORT = 65535
logger = logging.getLogger('fiber')


_io_threads = []
_poller_threads = {}


def _clean_up():
    for t in _io_threads:
        logger.debug("cleanup thread %s", t)
        t.stop()
    for t in _io_threads:
        logger.debug("join thread %s", t)
        t.join()


class ZConnection(multiprocessing.connection._ConnectionBase):
    """A Connection class implemented with Fiber socket.

    It takes a (sock_type, dest_addr) tuple as input, creates a new connection
    which can send and recv Python objects. This ZConnection class can be
    serialized by pickle and then send to another remote process. Fiber socket
    will be reconnected after unpickle.

    :param handle: a (sock_type, dest_addr) tuple.
        `sock_type` should be a Fiber socket type. `dest_addr` should be in
        format like "tcp://127.0.0.1:9000".

    > note: ZConnection's fileno method returns a Fiber socket.
    """

    def __init__(self, handle, readable=True, writable=True):
        self._name = None
        if handle is None:
            raise ValueError("invalid socket")

        assert isinstance(handle, tuple)

        sock_type, dest_addr = handle
        self.sock_type = sock_type
        self.dest_addr = dest_addr
        self._create_handle()

        if not readable and not writable:
            raise ValueError(
                "at least one of `readable` and `writable` must be True")
        self._readable = readable
        self._writable = writable

        mp_register_after_fork(self, ZConnection._create_handle)
        atexit.register(self._close)

    def __getstate__(self):
        return {"sock_type": self.sock_type,
                "dest_addr": self.dest_addr,
                "_readable": self._readable,
                "_writable": self._writable,
                "_name": self._name}

    def __setstate__(self, state):
        self.sock_type = state['sock_type']
        self.dest_addr = state['dest_addr']
        self._readable = state['_readable']
        self._writable = state['_writable']
        self._name = state['_name']
        self._create_handle()
        mp_register_after_fork(self, ZConnection._create_handle)
        atexit.register(self._close)

    def __del__(self):
        # __del__ is not reliable, we use atexit to clean up things
        return

    def __repr__(self):
        name = (
            self._name
            if getattr(self, "_name", None) is not None
            else self.dest_addr
        )
        return '<ZConnection [{}, {}]>'.format(name, getattr(self, "_handle", None))

    def _create_handle(self):
        logger.debug("%s _create_handle called", self)

        #self._handle = context.socket(self.sock_type)
        self._handle = Socket(mode=self.sock_type)
        self._handle.connect(self.dest_addr)
        logger.debug("connect to %s", self.dest_addr)

    def _close(self):
        if self._handle:
            self._handle.close()
            self._handle = None

    def _send_bytes(self, buf):
        self._handle.send(buf)

    # TODO(jiale) _ConnectionBase's send use _ForkingPickler. Switch to
    # send_pyobj instead?

    def _recv_bytes(self, maxsize=None):
        # TODO(jiale) support maxsize

        return self._handle.recv()

    def recv(self):
        """Receive a (picklable) object"""
        #logger.debug("recv called")
        self._check_closed()
        self._check_readable()
        buf = self._recv_bytes()
        return reduction.ForkingPickler.loads(buf)

    def _poll(self, timeout):
        return bool(self._handle.poll(timeout=timeout))

    def set_name(self, name):
        self._name = name


class LazyZConnection(ZConnection):
    def __init__(self, handle, readable=True, writable=True, name=None):
        self._name = None

        if handle is None:
            raise ValueError("invalid socket")

        if isinstance(handle, tuple):
            sock_type, dest_addr = handle
            self.sock_type = sock_type
            self.dest_addr = dest_addr
            self._inited = False
            # Do not create handle
            # self._create_handle()
        else:
            raise ValueError("can't pass a socket to LazyZConnection")

        if not readable and not writable:
            raise ValueError(
                "at least one of `readable` and `writable` must be True")
        self._readable = readable
        self._writable = writable

    def _check_closed(self):
        if self._inited is False:
            self._create_handle()
            self._inited = True
        if self._handle is None:
            raise OSError("handle is closed")

    def _close(self):
        if not self._inited:
            return
        self._handle.close()

    def _send_bytes(self, buf):
        #self._handle.send_multipart([b"", buf])
        self._handle.send(buf)

    def _recv_bytes(self, maxsize=None):
        # TODO(jiale) support maxsize
        #msg = self._handle.recv_multipart()
        # msg -> b'' b'message data' (because of ROUTER)
        #data = msg[1]
        data = self._handle.recv()

        return data

    def __getstate__(self):
        return {"sock_type": self.sock_type,
                "dest_addr": self.dest_addr,
                "_readable": self._readable,
                "_writable": self._writable}

    def __setstate__(self, state):
        self.sock_type = state['sock_type']
        self.dest_addr = state['dest_addr']
        self._readable = state['_readable']
        self._writable = state['_writable']
        self._inited = False


class LazyZConnectionPipe(LazyZConnection):
    def _send_bytes(self, buf):
        self._handle.send(buf)

    def _recv_bytes(self, maxsize=None):
        # TODO(jiale) support maxsize

        return self._handle.recv()


def Pipe(duplex=True):
    """Return a pair of connected ZConnection objects.

    :param duplex: if duplex, then both read and write are allowed on each
        of the returned connection object. Otherwise, the first returned
        connection will be read-only and the second connection will be
        write-only. By default, duplex is enabled.
    """

    if duplex:
        d = ProcessDevice("rw", "rw")
    else:
        d = ProcessDevice("r", "w")
    d.start()

    if duplex:
        return (LazyZConnection(("rw", d.out_addr,)),
                LazyZConnection(("rw", d.in_addr,)))
    return (LazyZConnection(("r", d.out_addr,)),
            LazyZConnection(("w", d.in_addr,)))


class SimpleQueuePush():
    """A queue build on top of Fiber socket. It uses "w" - ("r" - "w") - "r"
    socket combination. Messages are pushed from one end of the queue to
    the other end without explicitly pulling.
    """
    def __repr__(self):
        return "SimpleQueuePush<reader:{}, writer: {}>".format(
            self._reader_addr, self._writer_addr)

    def __init__(self):
        self.done = False
        backend = get_backend()
        ip, _, _ = backend.get_listen_addr()
        # Listens on '0.0.0.0' so that it accepts connection regardless of
        # net interfaces. When connect connects, it should use the address
        # obtained from backend.get_listen_addr().
        d = ProcessDevice("r", "w")
        d.start()

        self._reader_addr = d.out_addr
        self._writer_addr = d.in_addr

        # client side
        # set reader to None because if reader is connected, Fiber socket will
        # fairly queue  messages to all readers even if this reader is
        # not reading.
        #self.reader = None
        self.reader = LazyZConnection(("r", self._reader_addr,))
        self.writer = LazyZConnection(("w", self._writer_addr,))

    def get(self):
        """Get an element from this Queue.

        :returns: An element from this queue. If there is no element in the
            queue, this method will block.
        """
        """
        if self.reader is None:
            logger.debug(
                "reader is None, create ZConnection((\"r\", %s))",
                self._reader_addr
            )

            self.reader = LazyZConnection(("r", self._reader_addr,))
        """
        #data = self.reader._handle.recv()
        msg = self.reader.recv()
        #msg = reduction.ForkingPickler.loads(data)
        logger.debug("%s got %s", self, msg)
        return msg

    def put(self, obj):
        """Put an element into the Queue.

        :param obj: Any picklable Python object.
        """
        logger.debug("%s put %s", self, obj)
        #data = reduction.ForkingPickler.dumps(obj)
        self.writer.send(obj)

    '''
    def __getstate__(self):
        d = self.__dict__.copy()
        # set reader to None so that de-serialized Queue doesn't connect to
        # reader automatically. Fiber socket will fairly queue all the messages
        # to all the readers. We want to prevent this behavior.
        d["reader"] = None
        return d
    '''


#Pipe = ClassicPipe
SimpleQueue = SimpleQueuePush
