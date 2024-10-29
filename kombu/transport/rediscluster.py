from __future__ import annotations

from collections import namedtuple
from contextlib import contextmanager
from queue import Empty
from time import time

from rediscluster import ClusterConnectionPool
from kombu.utils.encoding import bytes_to_str
from kombu.utils.eventio import ERR, READ, poll
from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property
from kombu.utils.uuid import uuid

from . import virtual
from .redis import (
    Channel as RedisChannel,
    MultiChannelPoller,
    MutexHeld,
    QoS as RedisQoS,
    Transport as RedisTransport,
)

try:
    import redis
except ImportError:  # pragma: no cover
    redis = None

try:
    import rediscluster
except ImportError:  # pragma: no cover
    rediscluster = None

DEFAULT_HEALTH_CHECK_INTERVAL = 25

error_classes_t = namedtuple('error_classes_t', (
    'connection_errors', 'channel_errors',
))

PRIORITY_STEPS = [0, 3, 6, 9]


class MutexHeld(Exception):
    """Raised when another party holds the lock."""


@contextmanager
def Mutex(client, name, expire):
    """Acquire redis lock in non blocking way.

    Raise MutexHeld if not successful.
    """
    lock_id = uuid().encode('utf-8')
    acquired = client.set(name, lock_id, ex=expire, nx=True)
    try:
        if acquired:
            yield
        else:
            raise MutexHeld()
    finally:
        if acquired:
            if client.get(name) == lock_id:
                client.delete(name)


def _after_fork_cleanup_channel(channel):
    channel._after_fork()


class QoS(RedisQoS):
    """Redis Ack Emulation."""

    restore_at_shutdown = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._vrestore_count = 0

    def restore_visible(self, start=0, num=10, interval=10):
        self._vrestore_count += 1
        if (self._vrestore_count - 1) % interval:
            return
        with self.channel.conn_or_acquire() as client:
            ceil = time() - self.visibility_timeout
            try:
                with Mutex(client, self.unacked_mutex_key,
                           self.unacked_mutex_expire):
                    visible = client.zrevrangebyscore(
                        self.unacked_index_key, ceil, 0,
                        start=num and start, num=num, withscores=True)
                    for tag, score in visible or []:
                        self.restore_by_tag(tag, client)
            except MutexHeld:
                pass


class ClusterPoller(MultiChannelPoller):
    """Async I/O poller for Redis transport."""

    def _register(self, channel, client, conn, type):
        if (channel, client, conn, type) in self._chan_to_sock:
            self._unregister(channel, client, conn, type)
        if conn._sock is None:  # not connected yet.
            conn.connect()
        sock = conn._sock
        self._fd_to_chan[sock.fileno()] = (channel, conn, type)
        self._chan_to_sock[(channel, client, conn, type)] = sock
        self.poller.register(sock, self.eventflags)

    def _unregister(self, channel, client, conn, type):
        self.poller.unregister(self._chan_to_sock[(channel, client, conn, type)])

    def _register_BRPOP(self, channel):
        """Enable BRPOP mode for channel."""
        conns = self._get_conns_for_channel(channel)

        for conn in conns:
            ident = (channel, channel.client, conn, 'BRPOP')

            if (conn._sock is None or ident not in self._chan_to_sock):
                channel._in_poll = False
                self._register(*ident)

        if not channel._in_poll:  # send BRPOP
            channel._brpop_start()

    def _register_LISTEN(self, channel):
        """Enable LISTEN mode for channel."""
        conns = self._get_conns_for_channel(channel)

        for conn in conns:
            ident = (channel, channel.subclient, conn, 'LISTEN')
            if (conn._sock is None or ident not in self._chan_to_sock):
                channel._in_listen = False
                self._register(*ident)

        if not channel._in_listen:
            channel._subscribe()  # send SUBSCRIBE

    def _get_conns_for_channel(self, channel):
        if self._chan_to_sock:
            return [conn for _, _, conn, _ in self._chan_to_sock]

        return [
            channel.client.connection_pool.get_connection_by_key(key, 'NOOP')
            for key in channel.active_queues
        ]

    def on_poll_start(self):
        for channel in self._channels:
            if channel.active_queues:  # BRPOP mode?
                if channel.qos.can_consume():
                    self._register_BRPOP(channel)
            if channel.active_fanout_queues:  # LISTEN mode?
                self._register_LISTEN(channel)

    def on_readable(self, fileno):
        try:
            chan, conn, type = self._fd_to_chan[fileno]
        except KeyError:
            return

        if chan.qos.can_consume():
            chan.handlers[type](**{'conn': conn})

    def handle_event(self, fileno, event):
        if event & READ:
            return self.on_readable(fileno), self
        elif event & ERR:
            chan, conn, type = self._fd_to_chan[fileno]
            chan._poll_error(type)

    def get(self, callback, timeout=None):
        self._in_protected_read = True
        try:
            for channel in self._channels:
                if channel.active_queues:  # BRPOP mode?
                    if channel.qos.can_consume():
                        self._register_BRPOP(channel)
                if channel.active_fanout_queues:  # LISTEN mode?
                    self._register_LISTEN(channel)

            events = self.poller.poll(timeout)
            if events:
                for fileno, event in events:
                    ret = self.handle_event(fileno, event)
                    if ret:
                        return
            # - no new data, so try to restore messages.
            # - reset active redis commands.
            self.maybe_restore_messages()
            raise Empty()
        finally:
            self._in_protected_read = False
            while self.after_read:
                try:
                    fun = self.after_read.pop()
                except KeyError:
                    break
                else:
                    fun()

    @property
    def fds(self):
        return self._fd_to_chan


class Channel(RedisChannel):
    """Redis cluster Channel."""

    QoS = QoS

    def _subscribe(self):
        keys = [self._get_subscribe_topic(queue)
                for queue in self.active_fanout_queues]
        if not keys:
            return
        c = self.subclient
        # if c.connection._sock is None:
        #     c.connection.connect()
        self._in_listen = True
        c.psubscribe(keys)

    def _brpop_start(self, timeout=1):
        queues = self._queue_cycle.consume(len(self.active_queues))
        if not queues:
            return
        keys = queues
        self._in_poll = True

        node_to_keys = {}
        pool = self.client.connection_pool

        for key in queues:
            node = self.client.connection_pool.get_node_by_slot(pool.nodes.keyslot(key))
            node_to_keys.setdefault(node['name'], []).append(key)

        for chan, client, conn, cmd in self.connection.cycle._chan_to_sock:
            expected = (self, self.client, 'BRPOP')
            keys = node_to_keys.get(conn.node['name'])

            if keys and (chan, client, cmd) == expected:
                for key in keys:
                    conn.send_command('BRPOP', key, timeout)

    def _brpop_read(self, **options):
        try:
            dest__item = None
            conn = options.pop('conn', None)
            if conn:
                try:
                    dest__item = self.client.parse_response(conn,
                                                            'BRPOP',
                                                            **options)
                except self.connection_errors:
                    conn.disconnect()
                    raise
            if dest__item:
                dest, item = dest__item
                dest = bytes_to_str(dest).rsplit(self.sep, 1)[0]
                self._queue_cycle.rotate(dest)
                self.connection._deliver(loads(bytes_to_str(item)), dest)
                return True
            else:
                raise Empty()
        finally:
            self._in_poll = None

    @contextmanager
    def conn_or_acquire(self, client=None):
        if client:
            yield client
        else:
            yield self._create_client()

    @property
    def pool(self):
        if self._pool is None:
            self._pool = self._get_pool()
        return self._pool

    @property
    def async_pool(self):
        if self._async_pool is None:
            self._async_pool = self._get_pool(asynchronous=True)
        return self._async_pool

    @cached_property
    def client(self):
        """Client used to publish messages, BRPOP etc."""
        return self._create_client(asynchronous=True)

    @cached_property
    def subclient(self):
        """Pub/Sub connection used to consume fanout queues."""
        client = self._create_client(asynchronous=True)
        return client.pubsub()

    def _create_client(self, asynchronous=False):
        params = {'skip_full_coverage_check': True}
        if asynchronous:
            params['connection_pool'] = self.async_pool
        else:
            params['connection_pool'] = self.pool
        return self.Client(**params)

    def _get_pool(self, asynchronous=False):
        params = self._connparams(asynchronous=asynchronous)
        params['skip_full_coverage_check'] = True
        params.pop('username', None)
        return ClusterConnectionPool(**params)

    def _get_client(self):
        print(f'kombu.transport.rediscluster.Channel._get_client {rediscluster.RedisCluster}')
        return rediscluster.RedisCluster


class Transport(RedisTransport):
    Channel = Channel

    driver_type = 'redis-cluster'
    driver_name = driver_type

    implements = virtual.Transport.implements.extend(
        asynchronous=True, exchange_type=frozenset(['direct'])
    )

    def __init__(self, *args, **kwargs):
        if rediscluster is None:
            raise ImportError('dependency missing: redis-py-cluster')

        super().__init__(*args, **kwargs)
        self.cycle = ClusterPoller()

    def driver_version(self):
        return rediscluster.__version__
