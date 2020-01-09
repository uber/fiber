#
# Module providing various facilities to other parts of the package
#
# multiprocessing/util.py
#
# Copyright (c) 2006-2008, R Oudkerk
# Licensed to PSF under a Contributor Agreement.
#
# Modifications Copyright (c) 2020 Uber Technologies

import itertools
import logging
import multiprocessing.util as mpu
import os
import re
import sys
import socket
import threading
import weakref

import psutil


logger = logging.getLogger('fiber')
_afterfork_registry = weakref.WeakValueDictionary()
_afterfork_counter = itertools.count()


_finalizer_registry = {}
_finalizer_counter = itertools.count()


def register_after_fork(obj, func):
    _afterfork_registry[(next(_afterfork_counter), id(obj), func)] = obj


def _run_after_forkers():
    logging.debug('_fun_after_forkers called')
    items = list(_afterfork_registry.items())
    items.sort()
    for (index, ident, func), obj in items:
        try:
            logging.debug('run after forker %s(%s)', func, obj)
            func(obj)
        except Exception as e:
            logging.info('after forker raised exception %s', e)


class Finalize(mpu.Finalize):
    """Basically this is the same as multiprocessing's Finalize class except
    this one uses it's own _finalizer_registry.
    """
    def __init__(self, obj, callback, args=(), kwargs=None, exitpriority=None):
        assert exitpriority is None or type(exitpriority) is int

        if obj is not None:
            self._weakref = weakref.ref(obj, self)
        else:
            assert exitpriority is not None

        self._callback = callback
        self._args = args
        self._kwargs = kwargs or {}
        self._key = (exitpriority, next(_finalizer_counter))
        self._pid = os.getpid()

        _finalizer_registry[self._key] = self


def find_ip_by_net_interface(target_interface):
    """Returns ip, debug_info."""
    ifces = psutil.net_if_addrs()
    ip = None
    for ifce in ifces:
        # See https://docs.docker.com/v17.09/engine/userguide/networking/default_network/custom-docker0/ # noqa E501
        if ifce == target_interface:
            addrs = ifces[ifce]
            for snicaddr in addrs:
                # IPv4 only
                if snicaddr.family == socket.AF_INET:
                    ip = snicaddr.address
                    return ip
    return None


class ForkAwareThreadLock(object):
    def __init__(self):
        self._reset()
        register_after_fork(self, ForkAwareThreadLock._reset)

    def _reset(self):
        self._lock = threading.Lock()
        self.acquire = self._lock.acquire
        self.release = self._lock.release

    def __enter__(self):
        return self._lock.__enter__()

    def __exit__(self, *args):
        return self._lock.__exit__(*args)


class ForkAwareLocal(threading.local):
    def __init__(self):
        register_after_fork(self, lambda obj: obj.__dict__.clear())

    def __reduce__(self):
        return type(self), ()


def find_listen_address():
    """Find an IP address for Fiber to use."""
    ip = None
    ifce = None
    ifces = psutil.net_if_addrs()
    for ifce, addrs in ifces.items():
        if re.match(r"^eth", ifce) or re.match(r"^en", ifce):
            for snicaddr in addrs:
                # IPv4 only
                if snicaddr.family == socket.AF_INET:
                    ip = snicaddr.address
                    break

    return ip, ifce


def is_in_interactive_console():
    if hasattr(sys, 'ps1'):
        return True

    return False
