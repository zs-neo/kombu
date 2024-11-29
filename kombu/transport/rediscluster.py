from __future__ import annotations

import functools
from collections import namedtuple
from contextlib import contextmanager
from queue import Empty
from time import time

import redis
from rediscluster.pubsub import ClusterPubSub

from kombu.utils.encoding import bytes_to_str
from kombu.utils.eventio import ERR, READ
from kombu.utils.json import loads
from kombu.utils.uuid import uuid

from . import virtual
from .redis import (
    Channel as RedisChannel,
    MultiChannelPoller,
    MutexHeld,
    QoS as RedisQoS,
    Transport as RedisTransport, PrefixedRedisPubSub, GlobalKeyPrefixMixin,
)
from ..exceptions import VersionMismatch
from ..log import get_logger

try:
    import rediscluster
    from rediscluster.connection import SSLClusterConnection, ClusterConnection, ClusterConnectionPool
except ImportError:
    rediscluster = None
    ClusterConnection = None
    SSLClusterConnection = None

logger = get_logger('kombu.transport.rediscluster')
crit, warning = logger.critical, logger.warning

DEFAULT_PORT = 6379
DEFAULT_DB = 0

DEFAULT_HEALTH_CHECK_INTERVAL = 25

error_classes_t = namedtuple('error_classes_t', (
    'connection_errors', 'channel_errors',
))


@contextmanager
def Mutex(client, name, expire):
    """
    https://redis.io/docs/latest/develop/use/patterns/distributed-locks/
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


class QoS(RedisQoS):

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

    def restore_by_tag(self, tag, client=None, leftmost=False):
        with self.channel.conn_or_acquire(client) as client:
            # Transaction support is disabled in redis-py-cluster.
            # Use pipelines to avoid extra network round-trips, not to ensure atomicity.
            p = client.hget(self.unacked_key, tag)
            with self.pipe_or_acquire() as pipe:
                self._remove_from_indices(tag, pipe)
                if p:
                    M, EX, RK = loads(bytes_to_str(p))  # json is unicode
                    self.channel._do_restore_message(M, EX, RK, pipe, leftmost)
                pipe.execute()


class PrefixedRedisCluster(GlobalKeyPrefixMixin, rediscluster.RedisCluster):

    def __init__(self, *args, **kwargs):
        self.global_keyprefix = kwargs.pop('global_keyprefix', '')
        rediscluster.RedisCluster.__init__(self, *args, **kwargs)

    def pubsub(self, **kwargs):
        return PrefixedRedisClusterPubSub(
            self.connection_pool,
            global_keyprefix=self.global_keyprefix,
            **kwargs,
        )


class PrefixedRedisClusterPubSub(PrefixedRedisPubSub, rediscluster.pubsub.ClusterPubSub):
    pass


class ClusterMultiChannelPoller(MultiChannelPoller):

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
        conns = self.get_conns_for_channel(channel)

        for conn in conns:
            ident = (channel, channel.client, conn, 'BRPOP')

            if (conn._sock is None or ident not in self._chan_to_sock):
                channel._in_poll = False
                self._register(*ident)

        if not channel._in_poll:  # send BRPOP
            channel._brpop_start()

    def _register_LISTEN(self, channel):
        conns = self.get_conns_for_channel(channel)

        for conn in conns:
            ident = (channel, channel.subclient, conn, 'LISTEN')
            if (conn._sock is None or ident not in self._chan_to_sock):
                channel._in_listen = False
                self._register(*ident)

        if not channel._in_listen:
            channel._subscribe()  # send SUBSCRIBE

    def get_conns_for_channel(self, channel):
        if self._chan_to_sock:
            return [conn for _, _, conn, _ in self._chan_to_sock]

        return [
            channel.client.connection_pool.get_connection_by_key(key, 'NOOP')
            for key in channel.active_queues
        ]

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
            chan._poll_error(conn, type)


class Channel(RedisChannel):
    QoS = QoS
    connection_class = ClusterConnection
    connection_class_ssl = SSLClusterConnection

    min_priority = 0
    max_priority = 0
    # Because the keys may be distributed in different slots and each slot may require different connections,
    # we can not use the brpop command that supports multiple keys in the current framework
    priority_steps = [min_priority]

    def _subscribe(self):
        keys = [self._get_subscribe_topic(queue)
                for queue in self.active_fanout_queues]
        if not keys:
            return
        c = self.subclient
        self._in_listen = True
        if c.connection._sock is None:
            c.connection.connect()
        self._in_listen = c.connection
        c.psubscribe(keys)

    def _brpop_start(self, timeout=1):
        queues = self._queue_cycle.consume(len(self.active_queues))
        if not queues:
            return
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
                    command_args = ['BRPOP', key, timeout]
                    if self.global_keyprefix:
                        command_args = self.client._prefix_args(command_args)
                    conn.send_command(*command_args)

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
        if redis.VERSION < (3, 0, 0):
            # https://redis-py-cluster.readthedocs.io/en/master/
            raise VersionMismatch(
                'Redis cluster transport requires redis-py versions 3.0.0 or later. '
                'You have {0.__version__}'.format(redis))

        if self.global_keyprefix:
            return functools.partial(
                PrefixedRedisCluster,
                global_keyprefix=self.global_keyprefix,
            )

        return rediscluster.RedisCluster

    def _poll_error(self, conn, type, **options):
        if type == 'LISTEN':
            self.subclient.parse_response()
        else:
            self.client.parse_response(conn, type)

    def close(self):
        self._closing = True
        if self._in_poll:
            try:
                conns = [conn for _, _, conn, _ in self.connection.cycle._chan_to_sock]
                for conn in conns:
                    self._brpop_read(**{'conn': conn})
            except Empty:
                pass
        if not self.closed:
            # remove from channel poller.
            self.connection.cycle.discard(self)

            # delete fanout bindings
            client = self.__dict__.get('client')  # only if property cached
            if client is not None:
                for queue in self._fanout_queues:
                    if queue in self.auto_delete_queues:
                        self.queue_delete(queue, client=client)
            self._disconnect_pools()
            self._close_clients()
        super().close()


class Transport(RedisTransport):
    Channel = Channel

    driver_type = 'redis-cluster'
    driver_name = 'redis-cluster'

    implements = virtual.Transport.implements.extend(
        asynchronous=True, exchange_type=frozenset(['direct'])
    )

    def __init__(self, *args, **kwargs):
        if rediscluster is None:
            raise ImportError('dependency missing: redis-py-cluster')

        super().__init__(*args, **kwargs)
        self.cycle = ClusterMultiChannelPoller()

    def driver_version(self):
        return rediscluster.__version__
