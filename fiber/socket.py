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

from abc import ABC, abstractmethod
import random
import logging
import multiprocessing as mp
import threading

from fiber.backend import get_backend
from typing import Any, NoReturn, Optional, Tuple


MIN_PORT = 40000
MAX_PORT = 65535
logger = logging.getLogger("fiber")

socket_lib = "nanomsg"
default_socket_ctx: "SockContext"


if socket_lib == "nanomsg":
    import nnpy
    import nnpy.errors
elif socket_lib == "nng":
    import pynng
    from pynng.exceptions import check_err
elif socket_lib == "zmq":
    import zmq
    import zmq.devices
else:
    raise ValueError("bad socket_lib value: {}".format(socket_lib))


def get_ctx() -> "SockContext":
    return default_socket_ctx


def bind_to_random_port(sock, addr_base: str, max_tries:int = 100, nng: bool = True) -> Optional[int]:
    num_tries = 0
    while num_tries < max_tries:
        try:
            port = random.randint(MIN_PORT, MAX_PORT)
            addr = "{}:{}".format(addr_base, port)
            if nng:
                sock.listen(addr)
            else:
                sock.bind(addr)
            return port
        except:
            num_tries += 1
            continue

    return None


class SockContext(ABC):

    default_addr:str = ""

    @abstractmethod
    def new(self, mode: str) -> "Socket":
        pass

    @staticmethod
    @abstractmethod
    def bind_random(sock, addr: str) -> int:
        pass

    @staticmethod
    @abstractmethod
    def connect(sock, addr: str) -> None:
        pass

    @staticmethod
    def close(sock) -> None:
        sock.close()


class ZMQContext(SockContext):
    default_addr = "tcp://0.0.0.0"

    def __init__(self) -> None:

        self._mode_to_type = {
            "r": zmq.DEALER,
            "w": zmq.DEALER,
            "rw": zmq.DEALER,
            "req": zmq.REQ,
            "rep": zmq.REP,
        }
        self.context = zmq.Context.instance()

    def new(self, mode: str) -> "zmq.Socket":
        sock_type = self._mode_to_type[mode]
        if sock_type is None:
            return None
        return self.context.socket(sock_type)

    @staticmethod
    def bind_random(sock, addr: str) -> int:
        assert type(addr) == str
        port = sock.bind_to_random_port(
            addr, min_port=MIN_PORT, max_port=MAX_PORT, max_tries=100
        )
        if port is None:
            raise RuntimeError("ZMQContext Failed to bind to a random port")

        return port

    @staticmethod
    def connect(sock, addr) -> Any:
        return sock.connect(addr)

    def device(self, s1_mode: str, s2_mode: str) -> Tuple["zmq.devices.ThreadDevice", str, str]:

        backend = get_backend()
        ip_ext, _, _ = backend.get_listen_addr()
        ip_bind = "0.0.0.0"
        addr_bind = "tcp://{}".format(ip_bind)

        s1_type = self._mode_to_type[s1_mode]
        s2_type = self._mode_to_type[s2_mode]

        device = zmq.devices.ThreadDevice(in_type=s1_type, out_type=s2_type)
        _writer_port = device.bind_in_to_random_port(
            addr_bind, min_port=MIN_PORT, max_port=MAX_PORT, max_tries=100
        )
        _reader_port = device.bind_out_to_random_port(
            addr_bind, min_port=MIN_PORT, max_port=MAX_PORT, max_tries=100
        )
        _reader_ext_addr = "tcp://{}:{}".format(ip_ext, _reader_port)
        _writer_ext_addr = "tcp://{}:{}".format(ip_ext, _writer_port)
        return device, _reader_ext_addr, _writer_ext_addr


class NNGDevice:
    STATE_INIT = 0
    STATE_AFTER_INIT = 1
    STATE_FINISHED = 2

    def __init__(self, ctx: "NNGContext", s1_mode: str, s2_mode: str, default_addr: str = "tcp://0.0.0.0") -> None:

        self._mode_to_opener = {
            "r": pynng.lib.nng_pull0_open_raw,
            "w": pynng.lib.nng_push0_open_raw,
            "rw": None,
        }

        self.s1_mode = s1_mode
        self.s2_mode = s2_mode
        self.default_addr = default_addr
        self._start_process()

    def _start_process(self) -> None:
        parent_conn, child_conn = mp.Pipe()
        self._proc = threading.Thread(
            target=self._run, args=(child_conn,), daemon=True
        )
        '''
        self._proc = mp.Process(
            target=self._run, args=(child_conn,), daemon=True
        )
        '''
        self._proc.start()

        #child_conn.close()
        self.conn = parent_conn

    def _create_socks(self) -> Tuple["pynng.Socket", "pynng.Socket"]:

        opener1 = self._mode_to_opener[self.s1_mode]
        opener2 = self._mode_to_opener[self.s2_mode]

        s1 = pynng.Socket(opener=opener1)
        s2 = pynng.Socket(opener=opener2)

        return s1, s2

    def _bind_socks(self, s1: "pynng.Socket", s2: "pynng.Socket") -> Tuple[Optional[int], Optional[int]]:
        port1 = bind_to_random_port(s1, self.default_addr)
        port2 = bind_to_random_port(s2, self.default_addr)

        return port1, port2

    def _run_device(self, s1: "pynng.Socket", s2: "pynng.Socket") -> None:

        ret = pynng.lib.nng_device(s1.socket, s2.socket)
        check_err(ret)

    def _run(self, conn: "mp.connection.Connection") -> None:
        s1, s2 = self._create_socks()

        state = NNGDevice.STATE_INIT

        while True:
            cmd = conn.recv()
            if state == NNGDevice.STATE_INIT:
                # wait for bind
                if cmd != "#bind":
                    conn.send("bad command, expecting #bind")
                else:
                    logger.debug("%s got #bind", self.__class__.__name__)
                    # bind sockets
                    port1, port2 = self._bind_socks(s1, s2)

                    conn.send((port1, port2))

                    state = NNGDevice.STATE_AFTER_INIT
            elif state == NNGDevice.STATE_AFTER_INIT:
                # wait for start()
                if cmd != "#start":
                    conn.send("bad command, expecting #start")
                else:
                    logger.debug("%s got #start", self.__class__.__name__)
                    # ACK
                    conn.send(None)
                    # Keeps running forever
                    self._run_device(s1, s2)
                    state = NNGDevice.STATE_FINISHED
                    logger.debug("%s finished", self.__class__.__name__)
                    conn.send("bad command, expecting #start")
                    break
            else:
                break

    def bind(self) -> Tuple[str, str]:
        self.conn.send("#bind")
        in_addr, out_addr = self.conn.recv()
        if in_addr is None:
            raise RuntimeError("NNGDevice failed to bind")
        self.in_addr = in_addr
        self.out_addr = out_addr
        return in_addr, out_addr

    def start(self) -> None:
        self.conn.send("#start")
        code = self.conn.recv()
        if code is not None:
            raise RuntimeError("Failed to start NNGDevice, {}".format(code))


class NNGContext(SockContext):
    default_addr = "tcp://0.0.0.0"

    def __init__(self) -> None:

        self._mode_to_creator = {
            "r": pynng.Pull0,
            "w": pynng.Push0,
            "rw": pynng.Pair0,
            "req": pynng.Req0,
            "rep": pynng.Rep0,
        }

    def new(self, mode: str) -> "pynng.Socket":
        func = self._mode_to_creator[mode]
        if func is None:
            return None
        return func()

    @staticmethod
    def bind_random(sock, addr: str) -> int:
        port = bind_to_random_port(sock, addr)
        if port is None:
            raise RuntimeError("NNGContext Failed to bind to a random port")

        return port

    @staticmethod
    def connect(sock, addr: str) -> "pynng.Dialer":
        return sock.dial(addr)

    def device(self, s1_mode: str, s2_mode: str) -> Tuple[NNGDevice, str, str]:
        self.s1_mode = s1_mode
        self.s2_mode = s2_mode

        device = NNGDevice(
            self, s1_mode, s2_mode, default_addr=NNGContext.default_addr
        )
        in_port, out_port = device.bind()

        backend = get_backend()
        ip_ext, _, _ = backend.get_listen_addr()

        in_addr = "tcp://{}:{}".format(ip_ext, in_port)
        out_addr = "tcp://{}:{}".format(ip_ext, out_port)

        return device, in_addr, out_addr


class NanomsgDevice(NNGDevice):
    def __init__(self, ctx: "NanomsgContext", s1_mode: str, s2_mode: str, default_addr: str ="tcp://0.0.0.0") -> None:
        self.s1_mode = s1_mode
        self.s2_mode = s2_mode
        self.default_addr = default_addr
        self.ctx = ctx

        self._start_process()

    def _create_socks(self) -> Tuple[nnpy.Socket, nnpy.Socket]:

        s1 = nnpy.Socket(nnpy.AF_SP_RAW, self.ctx._mode_to_type[self.s1_mode])
        s2 = nnpy.Socket(nnpy.AF_SP_RAW, self.ctx._mode_to_type[self.s2_mode])
        return s1, s2

    def _bind_socks(self, s1: nnpy.Socket, s2: nnpy.Socket) -> Tuple[int, int]:
        port1 = bind_to_random_port(s1, self.default_addr, nng=False)
        port2 = bind_to_random_port(s2, self.default_addr, nng=False)

        if port1 is None:
            raise RuntimeError("NanomsgDevice failed to bind port1")

        if port2 is None:
            raise RuntimeError("NanomsgDevice failed to bind port2")

        return port1, port2

    def _run_device(self, s1: nnpy.Socket, s2: nnpy.Socket) -> Any:

        rc = nnpy.nanomsg.nn_device(s1.sock, s2.sock)
        return nnpy.errors.convert(rc, rc)


class NanomsgContext(SockContext):
    default_addr = "tcp://0.0.0.0"

    def __init__(self) -> None:

        self._mode_to_type = {
            "r": nnpy.PULL,
            "w": nnpy.PUSH,
            "rw": nnpy.PAIR,
            "rep": nnpy.REP,
            "req": nnpy.REQ,
        }

    def new(self, mode) -> nnpy.Socket:

        sock_type = self._mode_to_type[mode]
        if sock_type is None:
            raise RuntimeError(
                "NangmsgContext got Invalid mode: {}".format(mode)
            )

        return nnpy.Socket(nnpy.AF_SP, sock_type)

    @staticmethod
    def bind_random(sock, addr) -> int:
        port = bind_to_random_port(sock, addr, nng=False)

        if port is None:
            raise RuntimeError("NanomsgContext Failed to bind to a random port")

        return port

    @staticmethod
    def connect(sock, addr) -> Any:
        return sock.connect(addr)

    def device(self, s1_mode, s2_mode) -> Tuple[NanomsgDevice, str, str]:
        self.s1_mode = s1_mode
        self.s2_mode = s2_mode

        device = NanomsgDevice(
            self, s1_mode, s2_mode, default_addr=NanomsgContext.default_addr
        )
        in_port, out_port = device.bind()

        backend = get_backend()
        ip_ext, _, _ = backend.get_listen_addr()

        in_addr = "tcp://{}:{}".format(ip_ext, in_port)
        out_addr = "tcp://{}:{}".format(ip_ext, out_port)

        return device, in_addr, out_addr


if socket_lib == "nanomsg":
    default_socket_ctx = NanomsgContext()
elif socket_lib == "nng":
    default_socket_ctx = NNGContext()
elif socket_lib == "zmq":
    default_socket_ctx = ZMQContext()
else:
    raise ValueError("bad socket_lib value: {}".format(socket_lib))


class Socket:
    def __repr__(self) -> str:
        return "{}<{},{}>".format(
            self.__class__.__name__,
            self._ctx.__class__.__name__,
            self._mode)

    def __init__(self, ctx=get_ctx(), mode="rw") -> None:
        self._mode = mode
        self._ctx = ctx
        self._sock = ctx.new(mode)
        if self._sock is None:
            raise ValueError("Socket mode \"{}\" not supported by {}".format(
                mode, ctx.__class__.__name__))

    def send(self, data) -> None:
        self._sock.send(data)

    def recv(self) -> bytes:
        return self._sock.recv()

    def bind(self) -> int:
        addr = self._ctx.default_addr
        bind_random = self._ctx.bind_random
        port = bind_random(self._sock, addr)

        return port

    def connect(self, addr) -> None:
        _connect = self._ctx.connect
        _connect(self._sock, addr)

    def close(self) -> None:
        _close = self._ctx.close
        _close(self._sock)


class ProcessDevice:
    def __init__(self, s1_mode: Any, s2_mode: Any, ctx=get_ctx()) -> None:
        device, in_addr, out_addr = ctx.device(s1_mode, s2_mode)
        self.device = device
        self.in_addr = in_addr
        self.out_addr = out_addr

    def start(self) -> None:
        self.device.start()
        logger.debug("started device in:%s out:%s", self.in_addr, self.out_addr)
