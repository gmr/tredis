"""Redis Cluster Commands Mixin"""
import collections

from tredis import common

ClusterNode = collections.namedtuple('ClusterNode', [
    'id', 'ip', 'port', 'flags', 'master', 'ping_sent', 'pong_recv',
    'config_epoch', 'link_state', 'slots'
])
""":class:`tredis.cluster.ClusterNode` is a :class:`~collections.namedtuple`
that contains the attributes for a single node returned by the
``CLUSTER NODES`` command.

.. versionadded: 0.7

:param bytes id: The node ID
:param bytes ip: The IP address of the node
:param int port: The node TCP port
:param bytes flags: A list of comma separated flags: ``myself``, ``master``,
    ``slave``, ``fail?``, ``fail``, ``handshake``, ``noaddr``, ``noflags``.
:param bytes master: If the node is a slave, and the master is known, the master
    node ID, otherwise the ``-`` character.
:param int ping_sent: Milliseconds unix time at which the currently active ping
    was sent, or zero if there are no pending pings.
:param int pong_recv: Milliseconds unix time the last pong was received.
:param int config_epoch: The configuration epoch (or version) of the current
    node (or of the current master if the node is a slave). Each time there is
    a failover, a new, unique, monotonically increasing configuration epoch is
    created. If multiple nodes claim to serve the same hash slots, the one with
    higher configuration epoch wins.
:param bytes link_state: The state of the link used for the node-to-node cluster
    bus. We use this link to communicate with the node. Can be ``connected`` or
    ``disconnected``.
:param slots: A hash slot number or range. There may be up to 16384 entries in
    total (limit never reached). This is the list of hash slots served by this
    node. If the entry is just a number, is parsed as such. If it is a range,
    it is in the form start-end, and means that the node is responsible for
    all the hash slots from start to end including the start and end values.
:type slots: list(tuple(int, int))

"""


class ClusterMixin(object):
    """Redis Cluster Commands Mixin"""

    def cluster_add_slots(self, *slots):
        pass

    def cluster_count_failure_report(self, node):
        pass

    def cluster_count_keys_in_slot(self, slot):
        pass

    def cluster_del_slots(self, slot):
        pass

    def cluster_failover(self, method):
        pass  # force, takeover

    def cluster_forget(self, node_id):
        pass

    def cluster_get_keys_in_slot(self, slot, count):
        pass

    def cluster_info(self):
        """``CLUSTER INFO`` provides ``INFO`` style information about Redis
        Cluster vital parameters.

        .. versionadded:: 0.7.0

        :returns: A dictionary of current cluster information
        :rtype: dict

        :key cluster_state: State is ok if the node is able to receive
            queries. fail if there is at least one hash slot which is unbound
            (no node associated), in error state (node serving it is flagged
            with ``FAIL`` flag), or if the majority of masters can't be
            reached by this node.
        :key cluster_slots_assigned: Number of slots which are associated to
            some node (not unbound). This number should be ``16384`` for the
            node to work properly, which means that each hash slot should be
            mapped to a node.
        :key cluster_slots_ok: Number of hash slots mapping to a node not in
            ``FAIL`` or ``PFAIL`` state.
        :key cluster_slots_pfail: Number of hash slots mapping to a node in
            ``PFAIL`` state. Note that those hash slots still work
            correctly, as long as the ``PFAIL`` state is not promoted to
            ``FAIL`` by the failure detection algorithm. ``PFAIL``
            only means that we are currently not able to talk with the node,
            but may be just a transient error.
        :key cluster_slots_fail: Number of hash slots mapping to a node in
            ``FAIL`` state. If this number is not zero the node is not
            able to serve queries unless cluster-require-full-coverage is set
            to no in the configuration.
        :key cluster_known_nodes: The total number of known nodes in the
            cluster, including nodes in ``HANDSHAKE`` state that may not
            currently be proper members of the cluster.
        :key cluster_size: The number of master nodes serving at least one
            hash slot in the cluster.
        :key cluster_current_epoch: The local Current Epoch variable. This is
            used in order to create unique increasing version numbers during
            fail overs.
        :key cluster_my_epoch: The Config Epoch of the node we are talking
            with. This is the current configuration version assigned to this
            node.
        :key cluster_stats_messages_sent: Number of messages sent via the
            cluster node-to-node binary bus.
        :key cluster_stats_messages_received: Number of messages received via
            the cluster node-to-node binary bus.
        :raises: :exc:`~tredis.exceptions.RedisError`

        """
        return self._execute(
            [b'CLUSTER', 'INFO'], format_callback=common.format_info_response)

    def cluster_key_slot(self, key):
        pass

    def cluster_meet(self, ip, port):
        pass

    def cluster_nodes(self):
        """Each node in a Redis Cluster has its view of the current cluster
        configuration, given by the set of known nodes, the state of the
        connection we have with such nodes, their flags, properties and
        assigned slots, and so forth.

        ``CLUSTER NODES`` provides all this information, that is, the current
        cluster configuration of the node we are contacting, in a serialization
        format which happens to be exactly the same as the one used by Redis
        Cluster itself in order to store on disk the cluster state (however the
        on disk cluster state has a few additional info appended at the end).

        Note that normally clients willing to fetch the map between Cluster
        hash slots and node addresses should use ``CLUSTER SLOTS`` instead.
        ``CLUSTER NODES``, that provides more information, should be used for
        administrative tasks, debugging, and configuration inspections. It is
        also used by ``redis-trib`` in order to manage a cluster.

        .. versionadded:: 0.7.0

        :rtype: list(:class:`~tredis.cluster.ClusterNode`)
        :raises: :exc:`~tredis.exceptions.RedisError`

        """

        def format_response(result):
            values = []
            for row in result.decode('utf-8').split('\n'):
                if not row:
                    continue
                parts = row.split(' ')
                slots = []
                for slot in parts[8:]:
                    if '-' in slot:
                        sparts = slot.split('-')
                        slots.append((int(sparts[0]), int(sparts[1])))
                    else:
                        slots.append((int(slot), int(slot)))
                ip_port = common.split_connection_host_port(parts[1])
                values.append(
                    ClusterNode(parts[0], ip_port[0], ip_port[1], parts[2],
                                parts[3], int(parts[4]), int(parts[5]),
                                int(parts[6]), parts[7], slots))
            return values

        return self._execute(
            ['CLUSTER', 'NODES'], format_callback=format_response)

    def cluster_replicate(self, node_id):
        pass

    def cluster_reset(self, method):
        pass  # hard, soft

    def cluster_save_config(self):
        pass

    def cluster_set_config_epoch(self, config_epoch):
        pass

    def cluster_set_slot(self, subcommand, node_id):
        pass

    def cluster_slaves(self, node_id):
        pass

    def cluster_slots(self):
        pass

    def cluster_readonly(self):
        pass

    def cluster_readwrite(self):
        pass
