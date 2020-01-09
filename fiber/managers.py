#
# Module providing the `SyncManager` class for dealing
# with shared objects
#
# multiprocessing/managers.py
#
# Copyright (c) 2006-2008, R Oudkerk
# Licensed to PSF under a Contributor Agreement.
#
# Modifications Copyright (c) 2020 Uber Technologies

"""
*Managers and proxy objects* enable multiprocessing to support shared storage,
which is critical to distributed systems. Usually, this function is handled by
external storage systems like Cassandra, Redis, etc. Fiber instead provides
built-in in-memory storage for applications to use (based on
multiprocessing.managers). The interface is the same as multiprocessing's
Manager type.
"""

import logging
import multiprocessing
import multiprocessing.util as mp_util
import queue
import threading
from multiprocessing.connection import (SocketListener, _validate_family,
                                        address_type)
from multiprocessing.context import get_spawning_popen
from multiprocessing.managers import (Array, DictProxy, ListProxy, Namespace,
                                      RebuildProxy, State, Value, dispatch)
from multiprocessing.process import AuthenticationString

import fiber.util
import fiber.queues
from fiber import process
from fiber.backend import get_backend

logger = logging.getLogger('fiber')


default_family = 'AF_INET'


class Listener(multiprocessing.connection.Listener):
    def __init__(self, address=None, family=None, backlog=500, authkey=None):
        family = family or (address and address_type(address)) \
                 or default_family
        if family != 'AF_INET':
            raise NotImplementedError

        backend = get_backend()
        # TODO(jiale) Add support for other address family for
        #  backend.get_listen_addr
        address = address or backend.get_listen_addr()
        self._address = address

        _validate_family(family)
        if family == 'AF_PIPE':
            # PIPE cannot be used across machines
            raise NotImplementedError
        else:
            # Listens on '0.0.0.0' so that it accepts connection regardless of
            # net interfaces. When connect connects, it uses a specific IP
            # address.
            self._listener = SocketListener(('0.0.0.0', 0), family, backlog)

        if authkey is not None and not isinstance(authkey, bytes):
            raise TypeError('authkey should be a byte string')

        self._authkey = authkey

    # Use IP from `backend.get_listen_addr` and port from
    # self._listener._address.
    # This represents an address that other client can connect to
    address = property(lambda self: (self._address[0],
                                     self._listener._address[1]))


listener_client = {
    # Use customized Listener because we need to get listening address from
    # the backend.
    'pickle': (Listener,
               multiprocessing.connection.Client),
}


class Server(multiprocessing.managers.Server):
    def __init__(self, registry, address, authkey, serializer):
        assert isinstance(authkey, bytes)
        self.registry = registry
        self.authkey = AuthenticationString(authkey)
        Listener, Client = listener_client[serializer]

        # do authentication later
        self.listener = Listener(address=address, backlog=500)
        self.address = self.listener.address

        self.id_to_obj = {'0': (None, ())}
        self.id_to_refcount = {}
        self.id_to_local_proxy_obj = {}
        self.mutex = threading.Lock()


class BaseManager(multiprocessing.managers.BaseManager):
    """
    A Base class for people to create
    [customized managers](https://docs.python.org/3.6/library/multiprocessing.html#customized-managers).
    Equivalent to multiprocessing.managers.BaseManager but can work on a
    computer cluster.

    The API of this class is the same as
    [multiprocessing.managers.BaseManager](https://docs.python.org/3.6/library/multiprocessing.html#multiprocessing.managers.BaseManager)
    """
    _registry = {}
    _Server = Server

    def __init__(self, address=None, authkey=None, serializer='pickle',
                 ctx=None):
        if authkey is None:
            authkey = process.current_process().authkey
        self._address = address     # XXX not final address if eg ('', 0)
        self._authkey = AuthenticationString(authkey)
        self._state = State()
        self._state.value = State.INITIAL
        self._serializer = serializer
        self._Listener, self._Client = listener_client[serializer]

    def get_server(self):
        """
        Return server object with serve_forever() method and address attribute
        """
        assert self._state.value == State.INITIAL
        return Server(self._registry, self._address,
                      self._authkey, self._serializer)

    @classmethod
    def _run_server(cls, registry, address, authkey, serializer, writer,
                    initializer=None, initargs=()):
        """Create a server, report its address and run it."""
        if initializer is not None:
            initializer(*initargs)

        # create server
        server = cls._Server(registry, address, authkey, serializer)

        # inform parent process of the server's address
        writer.send(server.address)
        writer.close()

        # run the manager
        logger.info('manager serving at %r', server.address)
        server.serve_forever()

    def start(self, initializer=None, initargs=()):
        """Spawn a server process for this manager object."""
        assert self._state.value == State.INITIAL
        logger.debug("start manager %s", self)

        if initializer is not None and not callable(initializer):
            raise TypeError('initializer must be a callable')

        # pipe over which we will retrieve address of server
        reader, writer = fiber.queues.Pipe(duplex=False)

        # spawn process which runs a server
        self._process = fiber.Process(
            target=type(self)._run_server,
            args=(self._registry, self._address, self._authkey,
                  self._serializer, writer, initializer, initargs),
        )
        ident = ':'.join(str(i) for i in self._process._identity)
        self._process.name = type(self).__name__ + '-' + ident
        self._process.start()

        # get address of server
        writer.close()
        self._address = reader.recv()
        reader.close()

        # register a finalizer
        self._state.value = State.STARTED
        self.shutdown = mp_util.Finalize(
            self, type(self)._finalize_manager,
            args=(self._process, self._address, self._authkey,
                  self._state, self._Client),
            exitpriority=0
        )

    @classmethod
    def register(cls, typeid, callable=None, proxytype=None, exposed=None,
                 method_to_typeid=None, create_method=True):
        """Register a typeid with the manager type."""
        if '_registry' not in cls.__dict__:
            cls._registry = cls._registry.copy()

        if proxytype is None:
            proxytype = AutoProxy

        exposed = exposed or getattr(proxytype, '_exposed_', None)

        method_to_typeid = method_to_typeid or \
            getattr(proxytype, '_method_to_typeid_', None)

        if method_to_typeid:
            for key, value in list(method_to_typeid.items()):
                assert type(key) is str, '%r is not a string' % key
                assert type(value) is str, '%r is not a string' % value

        cls._registry[typeid] = (
            callable, exposed, method_to_typeid, proxytype
        )

        if create_method:
            def temp(self, *args, **kwds):
                logger.debug("calling self._create(%s,%s), self=%s",
                             args, kwds, self)
                token, exp = self._create(typeid, *args, **kwds)
                proxy = proxytype(
                    token, self._serializer, manager=self,
                    authkey=self._authkey, exposed=exp
                )
                conn = self._Client(token.address, authkey=self._authkey)
                dispatch(conn, None, 'decref', (token.id,))
                return proxy
            temp.__name__ = typeid
            setattr(cls, typeid, temp)


class ProcessLocalSet(set):
    def __init__(self):
        fiber.util.register_after_fork(self, lambda obj: obj.clear())

    def __reduce__(self):
        return type(self), ()


class BaseProxy(multiprocessing.managers.BaseProxy):
    """A base for proxies of shared objects."""
    _address_to_local = {}
    _mutex = fiber.util.ForkAwareThreadLock()

    def __init__(self, token, serializer, manager=None,
                 authkey=None, exposed=None, incref=True, manager_owned=False):
        with BaseProxy._mutex:
            tls_idset = BaseProxy._address_to_local.get(token.address, None)
            if tls_idset is None:
                tls_idset = fiber.util.ForkAwareLocal(), ProcessLocalSet()
                BaseProxy._address_to_local[token.address] = tls_idset

        # self._tls is used to record the connection used by this
        # thread to communicate with the manager at token.address
        self._tls = tls_idset[0]

        # self._idset is used to record the identities of all shared
        # objects for which the current process owns references and
        # which are in the manager at token.address
        self._idset = tls_idset[1]

        self._token = token
        self._id = self._token.id
        self._manager = manager
        self._serializer = serializer
        self._Client = listener_client[serializer][1]

        # Should be set to True only when a proxy object is being created
        # on the manager server; primary use case: nested proxy objects.
        # RebuildProxy detects when a proxy is being created on the manager
        # and sets this value appropriately.
        self._owned_by_manager = manager_owned

        if authkey is not None:
            self._authkey = AuthenticationString(authkey)
        elif self._manager is not None:
            self._authkey = self._manager._authkey
        else:
            self._authkey = process.current_process().authkey

        if incref:
            self._incref()

        fiber.util.register_after_fork(self, BaseProxy._after_fork)

    def connect(self):
        """Connect manager object to the server process."""
        Listener, Client = listener_client[self._serializer]
        conn = Client(self._address, authkey=self._authkey)
        dispatch(conn, None, 'dummy')
        self._state.value = State.STARTED

    def __reduce__(self):
        kwds = {}
        if get_spawning_popen() is not None:
            kwds['authkey'] = self._authkey

        if getattr(self, '_isauto', False):
            kwds['exposed'] = self._exposed_
            return (RebuildProxy,
                    (AutoProxy, self._token, self._serializer, kwds))
        else:
            return (RebuildProxy,
                    (type(self), self._token, self._serializer, kwds))


def AutoProxy(token, serializer, manager=None, authkey=None,
              exposed=None, incref=True):
    """Return an auto-proxy for `token`."""
    _Client = listener_client[serializer][1]

    if exposed is None:
        conn = _Client(token.address, authkey=authkey)
        try:
            exposed = dispatch(conn, None, 'get_methods', (token,))
        finally:
            conn.close()

    if authkey is None and manager is not None:
        authkey = manager._authkey
    if authkey is None:
        authkey = process.current_process().authkey

    ProxyType = MakeProxyType('AutoProxy[%s]' % token.typeid, exposed)
    proxy = ProxyType(token, serializer, manager=manager, authkey=authkey,
                      incref=incref)
    proxy._isauto = True
    return proxy


def MakeProxyType(name, exposed, _cache={}):
    """Return a proxy type whose methods are given by `exposed`."""
    exposed = tuple(exposed)
    try:
        return _cache[(name, exposed)]
    except KeyError:
        pass

    dic = {}

    for meth in exposed:
        exec("""def %s(self, *args, **kwds):
        return self._callmethod(%r, args, kwds)""" % (meth, meth), dic)

    ProxyType = type(name, (BaseProxy,), dic)
    ProxyType._exposed_ = exposed
    _cache[(name, exposed)] = ProxyType
    return ProxyType


# Definition of SyncManager
#

class SyncManager(BaseManager):
    """Subclass of `BaseManager` which supports a number of shared object types.

    The types that are supported include:

    * [Queue](https://docs.python.org/3.6/library/multiprocessing.html#multiprocessing.Queue)
    * [JoinableQueue](https://docs.python.org/3.6/library/multiprocessing.html#multiprocessing.JoinableQueue)
    * [list](https://docs.python.org/3.6/library/stdtypes.html#list)
    * [dict](https://docs.python.org/3.6/library/stdtypes.html#dict)
    * [Value](https://docs.python.org/3.6/library/multiprocessing.html#multiprocessing.Value)
    * [Array](https://docs.python.org/3.6/library/multiprocessing.html#multiprocessing.Array)
    * [Namespace](https://docs.python.org/3.6/library/multiprocessing.html#multiprocessing.managers.Namespace)
    * [AsyncResult](https://docs.python.org/3.6/library/multiprocessing.html#multiprocessing.pool.AsyncResult)

    `SyncManager` is exposed as `fiber.Manager`. You can also use
    `fiber.managers.SyncManager` directly. The difference is that
    `fiber.Manager` will start the newly created manager and when using
    `SyncManager`, the user need to call the `start()` method of the manager
    explicitly.

    `fiber.Manager` example:

    ```
    manager = fiber.Manager()
    d = manager.dict()
    d["name"] = "managed_dict"
    print(d)
    ```

    `fiber.managers.SyncManager` example:

    ```
    manager = fiber.managers.SyncManager()
    manager.start()
    l = manager.list()
    l.append("x")
    print(l)
    ```
    """


ArrayProxy = MakeProxyType('ArrayProxy', (
    '__len__', '__getitem__', '__setitem__'
))


class NamespaceProxy(BaseProxy):
    _exposed_ = ('__getattribute__', '__setattr__', '__delattr__')

    def __getattr__(self, key):
        if key[0] == '_':
            return object.__getattribute__(self, key)
        callmethod = object.__getattribute__(self, '_callmethod')
        return callmethod('__getattribute__', (key,))

    def __setattr__(self, key, value):
        if key[0] == '_':
            return object.__setattr__(self, key, value)
        callmethod = object.__getattribute__(self, '_callmethod')
        return callmethod('__setattr__', (key, value))

    def __delattr__(self, key):
        if key[0] == '_':
            return object.__delattr__(self, key)
        callmethod = object.__getattribute__(self, '_callmethod')
        return callmethod('__delattr__', (key,))


class ValueProxy(BaseProxy):
    _exposed_ = ('get', 'set')

    def get(self):
        return self._callmethod('get')

    def set(self, value):
        return self._callmethod('set', (value,))

    value = property(get, set)


# Define AsyncProxy and AsyncManager

class AsyncProxyResult():
    def __init__(self, conn, proxy):
        self._conn = conn
        self._proxy = proxy

    def get(self):
        kind, result = self._conn.recv()

        if kind == '#RETURN':
            return result
        elif kind == '#PROXY':
            # TODO(jiale)
            raise NotImplementedError


class AsyncBaseProxy(BaseProxy):
    def _callmethod(self, methodname, args=(), kwds={}):
        try:
            conn = self._tls.connection
        except AttributeError:
            self._connect()
            conn = self._tls.connection

        conn.send((self._id, methodname, args, kwds))

        return AsyncProxyResult(conn, self)


def MakeAsyncProxyType(name, exposed, _cache={}):
    """Return a proxy type whose methods are given by `exposed`."""
    exposed = tuple(exposed)
    try:
        return _cache[(name, exposed)]
    except KeyError:
        pass

    dic = {}

    for meth in exposed:
        exec("""def %s(self, *args, **kwds):
        return self._callmethod(%r, args, kwds)""" % (meth, meth), dic)

    ProxyType = type(name, (AsyncBaseProxy,), dic)
    ProxyType._exposed_ = exposed
    _cache[(name, exposed)] = ProxyType
    return ProxyType


def AsyncAutoProxy(token, serializer, manager=None, authkey=None,
                   exposed=None, incref=True):
    """Return an auto-proxy for `token`."""
    _Client = listener_client[serializer][1]

    if exposed is None:
        conn = _Client(token.address, authkey=authkey)
        try:
            exposed = dispatch(conn, None, 'get_methods', (token,))
        finally:
            conn.close()

    if authkey is None and manager is not None:
        authkey = manager._authkey
    if authkey is None:
        authkey = process.current_process().authkey

    ProxyType = MakeAsyncProxyType('AutoProxy[%s]' % token.typeid, exposed)
    proxy = ProxyType(token, serializer, manager=manager, authkey=authkey,
                      incref=incref)
    proxy._isauto = True
    return proxy


class AsyncValueProxy(AsyncBaseProxy):
    _exposed_ = ('get', 'set')

    def get(self):
        return self._callmethod('get')

    def set(self, value):
        return self._callmethod('set', (value,))

    value = property(get, set)


class AsyncManager(BaseManager):
    """
    Asynchronous version of BaseManager. Currently, this type doesn't register
    any shared object types like SyncManager, it is more meant to be used
    as a base class for
    [customized managers](https://docs.python.org/3.6/library/multiprocessing.html#customized-managers).

    The API of the class is the same as `SyncManager` and
    [multiprocessing.managers.BaseManager](https://docs.python.org/3.6/library/multiprocessing.html#multiprocessing.managers.BaseManager)

    Example usage:

    ```python
    from fiber.managers import AsyncManager

    class Calculator():
        def add(self, a, b):
            return a + b

    class MyManager(AsyncManager):
        pass

    MyManager.register("Calculator", Calculator)
    manager = MyManager()
    manager.start()

    calc = manager.Calculator()
    res = calc.add(10, 32)
    print(type(res)) # fiber.managers.AsyncProxyResult
    print(res.get()) # 42
    ```
    """
    @classmethod
    def register(cls, typeid, callable=None, proxytype=None, exposed=None,
                 method_to_typeid=None, create_method=True):
        """Register a typeid with the manager type."""
        if '_registry' not in cls.__dict__:
            cls._registry = cls._registry.copy()

        if proxytype is None:
            proxytype = AsyncAutoProxy

        exposed = exposed or getattr(proxytype, '_exposed_', None)

        method_to_typeid = method_to_typeid or \
            getattr(proxytype, '_method_to_typeid_', None)

        if method_to_typeid:
            for key, value in list(method_to_typeid.items()):
                assert type(key) is str, '%r is not a string' % key
                assert type(value) is str, '%r is not a string' % value

        cls._registry[typeid] = (
            callable, exposed, method_to_typeid, proxytype
        )

        if create_method:
            def temp(self, *args, **kwds):
                logger.debug("calling self._create(%s,%s)", args, kwds)
                token, exp = self._create(typeid, *args, **kwds)
                proxy = proxytype(
                    token, self._serializer, manager=self,
                    authkey=self._authkey, exposed=exp
                )
                conn = self._Client(token.address, authkey=self._authkey)
                dispatch(conn, None, 'decref', (token.id,))
                return proxy
            temp.__name__ = typeid
            setattr(cls, typeid, temp)


AsyncBaseListProxy = MakeAsyncProxyType('AsyncBaseListProxy', (
    '__add__', '__contains__', '__delitem__', '__getitem__', '__len__',
    '__mul__', '__reversed__', '__rmul__', '__setitem__',
    'append', 'count', 'extend', 'index', 'insert', 'pop', 'remove',
    'reverse', 'sort', '__imul__'
    ))


class AsyncListProxy(AsyncBaseListProxy):
    def __iadd__(self, value):
        self._callmethod('extend', (value,))
        return self

    def __imul__(self, value):
        self._callmethod('__imul__', (value,))
        return self


AsyncDictProxy = MakeAsyncProxyType('AsyncDictProxy', (
    '__contains__', '__delitem__', '__getitem__', '__iter__', '__len__',
    '__setitem__', 'clear', 'copy', 'get', 'has_key', 'items',
    'keys', 'pop', 'popitem', 'setdefault', 'update', 'values'
    ))
AsyncDictProxy._method_to_typeid_ = {
    '__iter__': 'Iterator',
    }


AsyncArrayProxy = MakeAsyncProxyType('AsyncArrayProxy', (
    '__len__', '__getitem__', '__setitem__'
    ))

# Register methods for SyncManager

SyncManager.register('Queue', queue.Queue)
SyncManager.register('JoinableQueue', queue.Queue)
# TODO(jiale) add support for these types
# SyncManager.register('Event', threading.Event, EventProxy)
# SyncManager.register('Lock', threading.Lock, AcquirerProxy)
# SyncManager.register('RLock', threading.RLock, AcquirerProxy)
# SyncManager.register('Semaphore', threading.Semaphore, AcquirerProxy)
# SyncManager.register('BoundedSemaphore', threading.BoundedSemaphore,
#                     AcquirerProxy)
# SyncManager.register('Condition', threading.Condition, ConditionProxy)
# SyncManager.register('Barrier', threading.Barrier, BarrierProxy)
# SyncManager.register('Pool', pool.Pool, PoolProxy)
SyncManager.register('list', list, ListProxy)
SyncManager.register('dict', dict, DictProxy)
SyncManager.register('Value', Value, ValueProxy)
SyncManager.register('Array', Array, ArrayProxy)
SyncManager.register('Namespace', Namespace, NamespaceProxy)

# types returned by methods of PoolProxy
# SyncManager.register('Iterator', proxytype=IteratorProxy, create_method=False) # noqa E501
SyncManager.register('AsyncResult', create_method=False)


# Register methods for AyncManager

#AsyncManager.register('Value', Value, AsyncValueProxy)
#AsyncManager.register('Queue', queue.Queue)
#AsyncManager.register('JoinableQueue', queue.Queue)
#AsyncManager.register('list', list, AsyncListProxy)
#AsyncManager.register('dict', dict, AsyncDictProxy)
#AsyncManager.register('Value', Value, ValueProxy)
#AsyncManager.register('Array', Array, AsyncArrayProxy)
#AsyncManager.register('Namespace', Namespace, NamespaceProxy)
