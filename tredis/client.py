"""
Cluster Supporting Redis Client

"""
import collections
import logging

import hiredis
from tornado import concurrent
from tornado import ioloop
from tornado import locks
from tornado import iostream
from tornado import tcpclient

from tredis import common
from tredis import crc16
from tredis import exceptions
from tredis import cluster
from tredis import connection
from tredis import geo
from tredis import hashes
from tredis import hyperloglog
from tredis import keys
from tredis import lists
from tredis import pubsub
from tredis import scripting
from tredis import server
from tredis import sets
from tredis import sortedsets
from tredis import strings
from tredis import transactions

LOGGER = logging.getLogger(__name__)

CRLF = b'\r\n'

DEFAULT_HOST = 'localhost'
"""The default host to connect to"""

DEFAULT_PORT = 6379
"""The default port to connect to"""

DEFAULT_DB = 0
"""The default database number to use"""

HASH_SLOTS = 16384
"""Redis Cluster Hash Slots Value"""

# Python 2 support for ascii()
if 'ascii' not in dir(__builtins__):  # pragma: nocover
    from tredis.compat import ascii

Command = collections.namedtuple(
    'Command', ['command', 'connection', 'expectation', 'callback'])


class _Connection(object):
    """Manages the redis TCP connection.

    :param str host: The hostname to connect to
    :param int port: The port to connect on
    :param int db: The database number to use
    :param method on_close: The method to call if the connection is closed

    """

    def __init__(self,
                 host,
                 port,
                 db,
                 on_written,
                 on_close,
                 io_loop,
                 cluster_node=False,
                 read_only=False,
                 slots=None):
        super(_Connection, self).__init__()
        self.connected = False
        self.io_loop = io_loop
        self.host = host
        self.port = port
        self.database = int(db or DEFAULT_DB)

        self._client = tcpclient.TCPClient()
        self._cluster_node = cluster_node
        self._read_only = read_only
        self._slots = slots or []
        self._stream = None
        self._on_connect = None
        self._on_close = on_close
        self._on_written = on_written

    def close(self):
        """Close the stream.

        :raises: :class:`tredis.exceptions.ConnectionError` if the
            stream is not currently connected

        """
        if self._stream is None:
            raise exceptions.ConnectionError('Not connected')
        self._stream.close()

    def connect(self):
        """Connect to the Redis server if necessary.

        :rtype: :class:`~tornado.concurrent.Future`
        :raises: :class:`~tredis.exceptions.ConnectError`
                 :class:`~tredis.exceptinos.RedisError`

        """
        future = concurrent.Future()

        if self.connected:
            raise exceptions.ConnectError('already connected')

        LOGGER.debug('%s connecting', self.name)
        self.io_loop.add_future(
            self._client.connect(self.host, self.port),
            lambda f: self._on_connected(f, future))
        return future

    def execute(self, command, future):
        """Execute a command after connecting if necessary.

        :param bytes command: command to execute after the connection
            is established
        :param tornado.concurrent.Future future:  future to resolve
            when the command's response is received.

        """
        LOGGER.debug('execute(%r, %r)', command, future)
        if self.connected:
            self._write(command, future)
        else:

            def on_connected(cfuture):
                if cfuture.exception():
                    return future.set_exception(cfuture.exception())
                self._write(command, future)

            self.io_loop.add_future(self.connect(), on_connected)

    @property
    def name(self):
        """Return the connection name as it is returned in the cluster nodes
        command.

        :rtype: str

        """
        return '{}:{}'.format(self.host, self.port)

    def read(self, callback):
        """Issue a read on the stream, invoke callback when completed.

        :raises: :class:`tredis.exceptions.ConnectionError` if the
            stream is not currently connected

        """
        self._stream.read_bytes(65536, callback, None, True)

    def set_read_only(self, read_only):
        """Change the connection's read-only flag in the client.

        :param bool read_only: Value to set
        """
        self._read_only = read_only

    def set_slots(self, slots):
        """Change the connection's slot list in the client.

        :param list slots: The updated slot values

        """
        self._slots = slots

    @property
    def slots(self):
        """Return the connection's slot values for clustering.

        :rtype: list

        """
        return self._slots

    def _on_closed(self):
        """Invoked when the connection is closed"""
        LOGGER.error('Redis connection closed')
        self.connected = False
        self._on_close()
        self._stream = None

    def _on_connected(self, stream_future, connect_future):
        """Invoked when the socket stream has connected, setting up the
        stream callbacks and invoking the on connect callback if set.

        :param stream_future: The connection socket future
        :type stream_future: :class:`~tornado.concurrent.Future`
        :param stream_future: The connection response future
        :type stream_future: :class:`~tornado.concurrent.Future`
        :raises: :exc:`tredis.exceptions.ConnectError`

        """
        if stream_future.exception():
            connect_future.set_exception(
                exceptions.ConnectError(stream_future.exception()))
        else:
            self._stream = stream_future.result()
            self._stream.set_close_callback(self._on_closed)
            self.connected = True
            connect_future.set_result(self)

    def _write(self, command, future):
        """Write a command to the socket

        :param Command command: the Command data structure

        """

        def on_written():
            self._on_written(command, future)

        try:
            self._stream.write(command.command, callback=on_written)
        except iostream.StreamClosedError as error:
            future.set_exception(exceptions.ConnectionError(error))
        except Exception as error:
            LOGGER.exception('unhandled write failure - %r', error)
            future.set_exception(exceptions.ConnectionError(error))


class Client(server.ServerMixin, keys.KeysMixin, strings.StringsMixin,
             geo.GeoMixin, hashes.HashesMixin, hyperloglog.HyperLogLogMixin,
             lists.ListsMixin, sets.SetsMixin, sortedsets.SortedSetsMixin,
             pubsub.PubSubMixin, connection.ConnectionMixin,
             cluster.ClusterMixin, scripting.ScriptingMixin,
             transactions.TransactionsMixin):
    """Asynchronous Redis client that supports Redis with master/slave failover
    and clustering. When ``clustering`` is ``True``, the client will
    automatically discover all of the nodes in the cluster and connect to them.

    The ``hosts`` argument should contain a list of Redis servers to connect
    to. The connection information for the server should be a :class:`dict`. In
    the following example, the client will connect to Redis running at
    ``127.0.0.1`` on port ``6379`` using database # ``2``:

    .. code:: python

        class RequestHandler(web.RequestHandler):

            @gen.coroutine
            def connect_to_redis(self)
                client = tredis.Client([{
                        'host': '127.0.0.1', 'port': 6379, 'db': 2
                    }], auto_connect=False, clustering=True)
                yield client.connect()

    When ``auto_connect`` is set to ``True``, the connection to the Redis
    server or the Redis cluster starts on creation of the client. You should be
    aware that this will not block on creation and the connection will be
    established asynchronously in the background. Any requests made with the
    client while it is connecting will block until the connection is available.

    When ``auto_connect`` is set to ``False``, you will need to invoke the
    :meth:`~tredis.Client.connect` method, yielding to the
    :class:`~tornado.concurrent.Future` that it returns.

    .. added: 0.7.0

    :param hosts: A list of host connection values.
    :type hosts: list(dict)
    :param io_loop: Override the current Tornado IOLoop instance
    :type io_loop: tornado.ioloop.IOLoop
    :param method on_close: The method to call if the connection is closed
    :param bool clustering: Toggle the cluster support in the client
    :param bool auto_connect: Toggle the auto-connect on creation feature


    """

    def __init__(self,
                 hosts,
                 on_close=None,
                 io_loop=None,
                 clustering=False,
                 auto_connect=True):
        """Create a new instance of the ``Client`` class.

        :param hosts: A list of host connection values.
        :type hosts: list(dict)
        :param io_loop: Override the current Tornado IOLoop instance
        :type io_loop: tornado.ioloop.IOLoop
        :param method on_close: The method to call if the connection is closed
        :param bool clustering: Toggle the cluster support in the client
        :param bool auto_connect: Toggle the auto-connect on creation feature

        """
        self._buffer = bytes()
        self._busy = locks.Lock()
        self._closing = False
        self._cluster = {}
        self._clustering = clustering
        self._connected = locks.Event()
        self._connect_future = concurrent.Future()
        self._connection = None
        self._discovery = False
        self._hosts = hosts
        self._on_close_callback = on_close
        self._reader = hiredis.Reader()
        self.io_loop = io_loop or ioloop.IOLoop.current()
        if not self._clustering:
            if len(hosts) > 1:
                raise ValueError('Too many hosts for non-clustering mode')
        if auto_connect:
            LOGGER.debug('Auto-connecting')
            self.connect()

    def connect(self):
        """Connect to the Redis server or Cluster.

        :rtype: tornado.concurrent.Future

        """
        LOGGER.debug('Creating a%s connection to %s:%s (db %s)',
                     ' cluster node'
                     if self._clustering else '', self._hosts[0]['host'],
                     self._hosts[0]['port'], self._hosts[0].get(
                         'db', DEFAULT_DB))
        self._connect_future = concurrent.Future()
        conn = _Connection(
            self._hosts[0]['host'],
            self._hosts[0]['port'],
            self._hosts[0].get('db', DEFAULT_DB),
            self._read,
            self._on_closed,
            self.io_loop,
            cluster_node=self._clustering)
        self.io_loop.add_future(conn.connect(), self._on_connected)
        return self._connect_future

    def close(self):
        """Close any open connections to Redis.

        :raises: :exc:`tredis.exceptions.ConnectionError`

        """
        if not self._connected.is_set():
            raise exceptions.ConnectionError('not connected')
        self._closing = True
        if self._clustering:
            for host in self._cluster.keys():
                self._cluster[host].close()
        elif self._connection:
            self._connection.close()

    @property
    def ready(self):
        """Indicates that the client is connected to the Redis server or
        cluster and is ready for use.

        :rtype: bool

        """
        if self._clustering:
            return (all([c.connected for c in self._cluster.values()])
                    and len(self._cluster))
        return (self._connection and self._connection.connected)

    def _build_command(self, parts):
        """Build the command that will be written to Redis via the socket

        :param list parts: The list of strings for building the command
        :rtype: bytes

        """
        return self._encode_resp(parts)

    def _create_cluster_connection(self, node):
        """Create a connection to a Redis server.

        :param node: The node to connect to
        :type node: tredis.cluster.ClusterNode

        """
        LOGGER.debug('Creating a cluster connection to %s:%s', node.ip,
                     node.port)
        conn = _Connection(
            node.ip,
            node.port,
            0,
            self._read,
            self._on_closed,
            self.io_loop,
            cluster_node=True,
            read_only='slave' in node.flags,
            slots=node.slots)
        self.io_loop.add_future(conn.connect(), self._on_connected)

    def _encode_resp(self, value):
        """Dynamically build the RESP payload based upon the list provided.

        :param mixed value: The list of command parts to encode
        :rtype: bytes

        """
        if isinstance(value, bytes):
            return b''.join(
                [b'$',
                 ascii(len(value)).encode('ascii'), CRLF, value, CRLF])
        elif isinstance(value, str):  # pragma: nocover
            return self._encode_resp(value.encode('utf-8'))
        elif isinstance(value, int):
            return self._encode_resp(ascii(value).encode('ascii'))
        elif isinstance(value, float):
            return self._encode_resp(ascii(value).encode('ascii'))
        elif isinstance(value, list):
            output = [b'*', ascii(len(value)).encode('ascii'), CRLF]
            for item in value:
                output.append(self._encode_resp(item))
            return b''.join(output)
        else:
            raise ValueError('Unsupported type: {0}'.format(type(value)))

    @staticmethod
    def _eval_expectation(command, response, future):
        """Evaluate the response from Redis to see if it matches the expected
        response.

        :param command: The command that is being evaluated
        :type command: tredis.client.Command
        :param bytes response: The response value to check
        :param future: The future representing the execution of the command
        :type future: tornado.concurrent.Future
        :return:
        """
        if isinstance(command.expectation, int) and command.expectation > 1:
            future.set_result(response == command.expectation or response)
        else:
            future.set_result(response == command.expectation)

    def _execute(self, parts, expectation=None, format_callback=None):
        """Really execute a redis command

        :param list parts: The list of command parts
        :param mixed expectation: Optional response expectation

        :rtype: :class:`~tornado.concurrent.Future`
        :raises: :exc:`~tredis.exceptions.SubscribedError`

        """
        future = concurrent.TracebackFuture()

        try:
            command = self._build_command(parts)
        except ValueError as error:
            future.set_exception(error)
            return future

        def on_locked(_):
            if self.ready:
                if self._clustering:
                    cmd = Command(command, self._pick_cluster_host(parts),
                                  expectation, format_callback)
                else:
                    LOGGER.debug('Connection: %r', self._connection)
                    cmd = Command(command, self._connection, expectation,
                                  format_callback)
                LOGGER.debug('_execute(%r, %r, %r) on %s', cmd.command,
                             expectation, format_callback, cmd.connection.name)
                cmd.connection.execute(cmd, future)
            else:
                LOGGER.critical('Lock released & not ready, aborting command')

        # Wait until the cluster is ready, letting cluster discovery through
        if not self.ready and not self._connected.is_set():
            self.io_loop.add_future(
                self._connected.wait(),
                lambda f: self.io_loop.add_future(self._busy.acquire(), on_locked)
            )
        else:
            self.io_loop.add_future(self._busy.acquire(), on_locked)

        # Release the lock when the future is complete
        self.io_loop.add_future(future, lambda r: self._busy.release())
        return future

    def _on_cluster_discovery(self, future):
        """Invoked when the Redis server has responded to the ``CLUSTER_NODES``
        command.

        :param future: The future containing the response from Redis
        :type future: tornado.concurrent.Future

        """
        LOGGER.debug('_on_cluster_discovery(%r)', future)
        common.maybe_raise_exception(future)
        nodes = future.result()
        for node in nodes:
            name = '{}:{}'.format(node.ip, node.port)
            if name in self._cluster:
                LOGGER.debug('Updating cluster connection info for %s:%s',
                             node.ip, node.port)
                self._cluster[name].set_slots(node.slots)
                self._cluster[name].set_read_only('slave' in node.flags)
            else:
                self._create_cluster_connection(node)
        self._discovery = True

    def _on_closed(self):
        """Invoked by connections when they are closed."""
        self._connected.clear()
        if not self._closing:
            if self._on_close_callback:
                self._on_close_callback()
            else:
                raise exceptions.ConnectionError('closed')

    def _on_cluster_data_moved(self, response, command, future):
        """Process the ``MOVED`` response from a Redis cluster node.

        :param bytes response: The response from the Redis server
        :param command: The command that was being executed
        :type command: tredis.client.Command
        :param future: The execution future
        :type future: tornado.concurrent.Future

        """
        LOGGER.debug('on_cluster_data_moved(%r, %r, %r)', response, command,
                     future)
        parts = response.split(' ')
        name = '{}:{}'.format(*common.split_connection_host_port(parts[2]))
        LOGGER.debug('Moved to %r', name)
        if name not in self._cluster:
            raise exceptions.ConnectionError(
                '{} is not connected'.format(name))
        self._cluster[name].execute(
            command._replace(connection=self._cluster[name]), future)

    def _on_connected(self, future):
        """Invoked when connections have been established. If the client is
        in clustering mode, it will kick of the discovery step if needed. If
        not, it will select the configured database.

        :param future: The connection future
        :type future: tornado.concurrent.Future

        """
        if future.exception():
            self._connect_future.set_exception(future.exception())
            return

        conn = future.result()
        LOGGER.debug('Connected to %s (%r, %r, %r)', conn.name,
                     self._clustering, self._discovery, self._connected)
        if self._clustering:
            self._cluster[conn.name] = conn
            if not self._discovery:
                self.io_loop.add_future(self.cluster_nodes(),
                                        self._on_cluster_discovery)
            elif self.ready:
                LOGGER.debug('Cluster nodes all connected')
                if not self._connect_future.done():
                    self._connect_future.set_result(True)
                self._connected.set()
        else:

            def on_selected(sfuture):
                LOGGER.debug('Initial setup and selection processed')
                if sfuture.exception():
                    self._connect_future.set_exception(sfuture.exception())
                else:
                    self._connect_future.set_result(True)
                self._connected.set()

            select_future = concurrent.Future()
            self.io_loop.add_future(select_future, on_selected)
            self._connection = conn
            cmd = Command(
                self._build_command(['SELECT', str(conn.database)]),
                self._connection, None, None)
            cmd.connection.execute(cmd, select_future)

    def _on_read_only_error(self, command, future):
        """Invoked when a Redis node returns an error indicating it's in
        read-only mode. It will use the ``INFO REPLICATION`` command to
        attempt to find the master server and failover to that, reissuing
        the command to that server.

        :param command: The command that was being executed
        :type command: tredis.client.Command
        :param future: The execution future
        :type future: tornado.concurrent.Future

        """
        failover_future = concurrent.TracebackFuture()

        def on_replication_info(_):
            common.maybe_raise_exception(failover_future)
            LOGGER.debug('Failover closing current read-only connection')
            self._closing = True
            database = self._connection.database
            self._connection.close()
            self._connected.clear()
            self._connect_future = concurrent.Future()

            info = failover_future.result()
            LOGGER.debug('Failover connecting to %s:%s', info['master_host'],
                         info['master_port'])
            self._connection = _Connection(
                info['master_host'], info['master_port'], database, self._read,
                self._on_closed, self.io_loop, self._clustering)

            # When the connection is re-established, re-run the command
            self.io_loop.add_future(
                self._connect_future,
                lambda f: self._connection.execute(
                    command._replace(connection=self._connection), future))

            # Use the normal connection processing flow when connecting
            self.io_loop.add_future(self._connection.connect(),
                                    self._on_connected)

        if self._clustering:
            command.connection.set_readonly(True)

        LOGGER.debug('%s is read-only, need to failover to new master',
                     command.connection.name)

        cmd = Command(
            self._build_command(['INFO', 'REPLICATION']), self._connection,
            None, common.format_info_response)

        self.io_loop.add_future(failover_future, on_replication_info)
        cmd.connection.execute(cmd, failover_future)

    def _read(self, command, future):
        """Invoked when a command is executed to read and parse its results.
        It will loop on the IOLoop until the response is complete and then
        set the value of the response in the execution future.

        :param command: The command that was being executed
        :type command: tredis.client.Command
        :param future: The execution future
        :type future: tornado.concurrent.Future

        """
        response = self._reader.gets()
        if response is not False:
            if isinstance(response, hiredis.ReplyError):
                if response.args[0].startswith('MOVED '):
                    self._on_cluster_data_moved(response.args[0], command,
                                                future)
                elif response.args[0].startswith('READONLY '):
                    self._on_read_only_error(command, future)
                else:
                    future.set_exception(exceptions.RedisError(response))
            elif command.callback is not None:
                future.set_result(command.callback(response))
            elif command.expectation is not None:
                self._eval_expectation(command, response, future)
            else:
                future.set_result(response)
        else:

            def on_data(data):
                # LOGGER.debug('Read %r', data)
                self._reader.feed(data)
                self._read(command, future)

            command.connection.read(on_data)

    def _pick_cluster_host(self, value):
        """Selects the Redis cluster host for the specified value.

        :param mixed value: The value to use when looking for the host
        :rtype: tredis.client._Connection

        """
        crc = crc16.crc16(self._encode_resp(value[1])) % HASH_SLOTS
        for host in self._cluster.keys():
            for slot in self._cluster[host].slots:
                if slot[0] <= crc <= slot[1]:
                    return self._cluster[host]
        LOGGER.debug('Host not found for %r, returning first connection',
                     value)
        host_keys = sorted(list(self._cluster.keys()))
        return self._cluster[host_keys[0]]


class RedisClient(Client):
    """This is provided for backwards compatibility for versions < 0.7.

    .. deprecated:: 0.7

    :param str host: The hostname to connect to
    :param int port: The port to connect on
    :param int db: The database number to use
    :param method on_close: The method to call if the connection is closed
    :param bool clustering: Toggle the cluster support in the client
    :param bool auto_connect: Toggle the auto-connect on creation feature

    """

    def __init__(self,
                 host=DEFAULT_HOST,
                 port=DEFAULT_PORT,
                 db=DEFAULT_DB,
                 on_close=None,
                 clustering=False,
                 auto_connect=True):
        super(RedisClient, self).__init__(
            [{
                'host': host,
                'port': port,
                'db': db
            }],
            on_close,
            clustering=clustering,
            auto_connect=auto_connect)
