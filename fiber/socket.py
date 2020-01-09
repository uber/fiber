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

import random
import logging
import multiprocessing as mp
import threading

from fiber.backend import get_backend


MIN_PORT = 40000
MAX_PORT = 65535
logger = logging.getLogger("fiber")

socket_lib = "nanomsg"
default_socket_ctx = None


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


def get_ctx():
    return default_socket_ctx


def bind_to_random_port(sock, addr_base, max_tries=100, nng=True):
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

    return


class SockContext:
    default_addr = None

    def new(self, mode):
        raise NotImplementedError

    @staticmethod
    def bind_random(sock, addr):
        raise NotImplementedError

    @staticmethod
    def connect(sock, addr):
        raise NotImplementedError

    @staticmethod
    def close(sock):
        sock.close()


class ZMQContext(SockContext):
    default_addr = "tcp://0.0.0.0"

    def __init__(self):

        self._mode_to_type = {
            "r": zmq.DEALER,
            "w": zmq.DEALER,
            "rw": zmq.DEALER,
            "req": zmq.REQ,
            "rep": zmq.REP,
        }
        self.context = zmq.Context.instance()

    def new(self, mode):
        sock_type = self._mode_to_type[mode]
        if sock_type is None:
            return None
        return self.context.socket(sock_type)

    @staticmethod
    def bind_random(sock, addr):
        assert type(addr) == str
        return sock.bind_to_random_port(
            addr, min_port=MIN_PORT, max_port=MAX_PORT, max_tries=100
        )

    @staticmethod
    def connect(sock, addr):
        return sock.connect(addr)

    def device(self, s1_mode, s2_mode):

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

    def __init__(self, ctx, s1_mode, s2_mode, default_addr="tcp://0.0.0.0"):

        self._mode_to_opener = {
            "r": pynng.lib.nng_pull0_open_raw,
            "w": pynng.lib.nng_push0_open_raw,
            "rw": None,
        }

        self.s1_mode = s1_mode
        self.s2_mode = s2_mode
        self.default_addr = default_addr
        self._start_process()

    def _start_process(self):
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

    def _mode_to_opener(self, mode):
        opener = self._mode_to_opener[mode]
        if opener is None:
            raise ValueError(
                "Mode {} not supported by {}", mode, self.__class__.__name__
            )
        return opener

    def _create_socks(self):

        opener1 = self._mode_to_opener[self.s1_mode]
        opener2 = self._mode_to_opener[self.s2_mode]

        s1 = pynng.Socket(opener=opener1)
        s2 = pynng.Socket(opener=opener2)

        return s1, s2

    def _bind_socks(self, s1, s2):
        port1 = bind_to_random_port(s1, self.default_addr)
        port2 = bind_to_random_port(s2, self.default_addr)

        return port1, port2

    def _run_device(self, s1, s2):

        ret = pynng.lib.nng_device(s1.socket, s2.socket)
        check_err(ret)

    def _run(self, conn):
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

    def bind(self):
        self.conn.send("#bind")
        in_addr, out_addr = self.conn.recv()
        if in_addr is None:
            raise RuntimeError("NNGDevice failed to bind")
        self.in_addr = in_addr
        self.out_addr = out_addr
        return in_addr, out_addr

    def start(self):
        self.conn.send("#start")
        code = self.conn.recv()
        if code is not None:
            raise RuntimeError("Failed to start NNGDevice, {}".format(code))


class NNGContext(SockContext):
    default_addr = "tcp://0.0.0.0"

    def __init__(self):

        self._mode_to_creator = {
            "r": pynng.Pull0,
            "w": pynng.Push0,
            "rw": pynng.Pair0,
            "req": pynng.Req0,
            "rep": pynng.Rep0,
        }

    def new(self, mode):
        func = self._mode_to_creator[mode]
        if func is None:
            return None
        return func()

    @staticmethod
    def bind_random(sock, addr):
        return bind_to_random_port(sock, addr)

    @staticmethod
    def connect(sock, addr):
        return sock.dial(addr)

    def device(self, s1_mode, s2_mode):
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
    def __init__(self, ctx, s1_mode, s2_mode, default_addr="tcp://0.0.0.0"):
        self.s1_mode = s1_mode
        self.s2_mode = s2_mode
        self.default_addr = default_addr
        self.ctx = ctx

        self._start_process()

    def _create_socks(self):

        s1 = nnpy.Socket(nnpy.AF_SP_RAW, self.ctx._mode_to_type[self.s1_mode])
        s2 = nnpy.Socket(nnpy.AF_SP_RAW, self.ctx._mode_to_type[self.s2_mode])
        return s1, s2

    def _bind_socks(self, s1, s2):
        port1 = bind_to_random_port(s1, self.default_addr, nng=False)
        port2 = bind_to_random_port(s2, self.default_addr, nng=False)
        return port1, port2

    def _run_device(self, s1, s2):

        rc = nnpy.nanomsg.nn_device(s1.sock, s2.sock)
        return nnpy.errors.convert(rc, rc)


class NanomsgContext(SockContext):
    default_addr = "tcp://0.0.0.0"

    def __init__(self):

        self._mode_to_type = {
            "r": nnpy.PULL,
            "w": nnpy.PUSH,
            "rw": nnpy.PAIR,
            "rep": nnpy.REP,
            "req": nnpy.REQ,
        }

    def new(self, mode):

        sock_type = self._mode_to_type[mode]
        if sock_type is None:
            return None
        return nnpy.Socket(nnpy.AF_SP, sock_type)

    @staticmethod
    def bind_random(sock, addr):
        return bind_to_random_port(sock, addr, nng=False)

    @staticmethod
    def connect(sock, addr):
        return sock.connect(addr)

    def device(self, s1_mode, s2_mode):
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
    def __repr__(self):
        return "{}<{},{}>".format(
            self.__class__.__name__,
            self._ctx.__class__.__name__,
            self._mode)

    def __init__(self, ctx=get_ctx(), mode="rw"):
        self._mode = mode
        self._ctx = ctx
        self._sock = ctx.new(mode)
        if self._sock is None:
            raise ValueError("Socket mode \"{}\" not supported by {}".format(
                mode, ctx.__class__.__name__))

    def send(self, data):
        self._sock.send(data)

    def recv(self):
        return self._sock.recv()

    def bind(self):
        addr = self._ctx.default_addr
        bind_random = self._ctx.bind_random
        port = bind_random(self._sock, addr)

        return port

    def connect(self, addr):
        _connect = self._ctx.connect
        _connect(self._sock, addr)

    def close(self):
        _close = self._ctx.close
        _close(self._sock)


class ProcessDevice:
    def __init__(self, s1_mode, s2_mode, ctx=get_ctx()):
        device, in_addr, out_addr = ctx.device(s1_mode, s2_mode)
        self.device = device
        self.in_addr = in_addr
        self.out_addr = out_addr

    def start(self):
        self.device.start()
        logger.debug("started device in:%s out:%s", self.in_addr, self.out_addr)
