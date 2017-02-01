"""Redis Cluster Commands Mixin"""


class ClusterMixin(object):
    """Redis Cluster Commands Mixin"""

    def add_slots(self, *slots):
        pass

    def count_failure_report(self, node):
        pass

    def count_keys_in_slot(self, slot):
        pass

    def del_slots(self, slot):
        pass

    def failover(self, method):
        pass  # force, takeover

    def forget(self, node_id):
        pass

    def get_keys_in_slot(self, slot, count):
        pass

    def info(self):
        pass

    def key_slot(self, key):
        pass

    def meet(self, ip, port):
        pass

    def nodes(self):
        pass

    def replicate(self, node_id):
        pass

    def reset(self, method):
        pass  # hard, soft

    def save_config(self):
        pass

    def set_config_epoch(self, config_epoch):
        pass

    def set_slot(self, subcommand, node_id):
        pass

    def slaves(self, node_id):
        pass

    def slots(self):
        pass

    def readonly(self):
        pass

    def readwrite(self):
        pass

