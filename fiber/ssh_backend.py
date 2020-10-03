# This is just an naieve implementation of ssh backend
# Features needed:
#  reuse connections
#  resource management
# ssh2-python document: https://github.com/ParallelSSH/ssh2-python#complete-example

import os
import socket
import itertools
import shlex
import multiprocessing as mp

import fiber
import fiber.core as core
from fiber.core import ProcessStatus
from fiber.util import find_listen_address, find_ip_by_net_interface

import psutil
from ssh2.session import Session


class SSHProc:
    def __init__(self, channel, job_spec, ident, node):
        self.channel = channel
        self.job_spec = job_spec
        self.ident = ident
        self.node = node

    def is_alive(self):
        if self.channel.eof():
            return False
        return True

    def wait(self, timeout):
        if timeout:
            # set session timeout
            self.channel.session.set_timeout(timeout)
            res = self.channel.get_exit_status()
            if res == ssh2.error_codes.LIBSSH2_ERROR_TIMEOUT:
                return None
            return res

        status = self.channel.get_exit_status()
        return status

    def terminate(self):
        self.channel.close()

    @property
    def host(self):
        return self.node["host"]

    def get_log(self):
        res = []
        size, data = self.channel.read_stderr()
        while size > 0:
            res.append(data)
            size, data = self.channel.read_stderr()

        size, data = self.channel.read()
        while size > 0:
            res.append(data)
            size, data = self.channel.read()

        return b"".join(res)


"""
node:
    session
    host
    port
    user
    pass
    cpus
    mem
"""


def get_channel(node):
    host = node["host"]
    port = node.get("port", 22)
    user = node.get("user", None)
    password = node.get("password", None)

    if user is None:
        user = os.getlogin()

    if port is None:
        port = 22

    if password is None:
        import getpass

        password = getpass.getpass()
        node["password"] = password

    assert password is not None, "Password needed"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    session = Session()
    session.handshake(sock)
    # session.agent_auth(user)
    session.userauth_password(user, password)

    channel = session.open_session()

    return channel


def channel_execute(channel, cmd):
    channel.execute(cmd)
    res = []
    size, data = channel.read()
    while size > 0:
        res.append(data)
        size, data = channel.read()

    return b"".join(res)


def parse_ssh_nodes(ssh_nodes_str):
    """
    Format: user:pass@host:port
    """
    res = []
    node_strs = ssh_nodes_str.split(",")
    for node_str in node_strs:
        if "@" not in node_str:
            raise ValueError(
                "Bad ssh nodes str, missing '@': {}".format(ssh_nodes_str)
            )

        parts = node_str.split("@")
        assert len(parts) == 2
        if ":" not in parts[0]:
            user = parts[0]
            password = None
        else:
            user, password = parts[0].split(":")

        if ":" in parts[1]:
            host, port = parts[1].split(":")
        else:
            host = parts[1]
            port = None

        res.append(
            {
                "host": host,
                "port": port,
                "user": user,
                "password": password,
            }
        )

    return res


class Backend(core.Backend):
    """
    This is a backend based on ssh
    """

    name = "ssh"

    def __init__(self, nodes=""):
        self.ident = itertools.count(1)
        self.nodes = parse_ssh_nodes(nodes)
        self._create_sessions()

    def _get_core(self, channel):
        # TODO use python / pythohn3
        output = channel_execute(
            channel, "python3 -c 'import os;print(os.cpu_count())'"
        )
        cores = int(output)
        return cores

    def _create_sessions(self):
        for node in self.nodes:
            channel = get_channel(node)
            assert channel is not None
            node["channel"] = channel
            node["cpu"] = self._get_core(channel)
            # TODO(add memory support)

    def _schedule(self, job_spec):
        for node in self.nodes:
            if node["cpu"] >= job_spec.cpu:
                node["cpu"] -= job_spec.cpu
                return node

        raise RuntimeError("Run out of resources")

    def _run(self, node, job_spec):
        # create a new channel
        channel = get_channel(node)
        out = []
        quote = False
        for part in job_spec.command:
            if quote:
                out.append(shlex.quote(part))
                quote = False
            else:
                out.append(part)
            if part == "-c":
                quote = True
                continue

        # TODO DEBUB
        out.append("2>&1")
        out.append("|")
        out.append("tee /tmp/fiber.log")

        cmd = " ".join(out)
        channel.execute(cmd)
        proc = SSHProc(channel, job_spec, next(self.ident), node)
        return proc

    def create_job(self, job_spec):
        node = self._schedule(job_spec)
        proc = self._run(node, job_spec)
        job = core.Job(proc, proc.ident)
        job.host = proc.host

        return job

    def get_job_status(self, job):
        proc = job.data

        if not proc.is_alive():
            return ProcessStatus.STOPPED

        return ProcessStatus.STARTED

    def get_job_logs(self, job):
        proc = job.data
        return proc.get_log()

    def wait_for_job(self, job, timeout):
        proc = job.data

        if timeout == 0:
            return proc.wait(timeout=0)

        return proc.wait(timeout=timeout)

    def terminate_job(self, job):
        proc = job.data

        proc.terminate()
        proc.node["cpu"] += proc.job_spec.cpu

    def get_listen_addr(self):
        ip = None

        ip, ifce = find_listen_address()

        # ip = find_ip_by_net_interface(ifce)
        if ip is None:
            raise mp.ProcessError(
                "Can't find a usable IPv4 address to listen. ifce_name: {}, "
                "ifces: {}".format(ifce, psutil.net_if_addrs())
            )
        # use 0 to bind to a random free port number
        return ip, 0, ifce
