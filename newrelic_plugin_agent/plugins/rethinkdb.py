"""
RethinkDB plugin polls RethinkDB for stats

"""
import datetime
import rethinkdb as r
import logging

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)


class RethinkDB(base.Plugin):

    GUID = 'com.meetme.newrelic_rethinkdb_plugin_agent'

    def add_cluster_datapoints(self, stats):
        """Add all of the data points for a cluster

        :param dict stats: The stats data to add

        """
        query_engine = stats.get('query_engine', dict())
        self.add_gauge_value('Connections/Active', 'connections',
                             query_engine.get('clients_active', 0))
        self.add_gauge_value('Connections/Current', 'connections',
                             query_engine.get('client_connections', 0))

        self.add_gauge_value('Operations/Insert', 'ops',
                             query_engine.get('written_docs_per_sec', 0))
        self.add_gauge_value('Operations/Query', 'ops',
                             query_engine.get('queries_per_sec', 0))
        self.add_gauge_value('Operations/Read', 'ops',
                             query_engine.get('read_docs_per_sec', 0))

    def add_member_datapoints(self, stats):
        """Add all of the data points for a member

        :param dict stats: The stats data to add

        """
        query_engine = stats.get('query_engine', dict())
        self.add_gauge_value('Member/Connections/Active', 'connections',
                             query_engine.get('clients_active', 0))
        self.add_gauge_value('Member/Connections/Current', 'connections',
                             query_engine.get('client_connections', 0))

        self.add_gauge_value('Member/Operations/Insert', 'ops',
                             query_engine.get('written_docs_per_sec', 0))
        self.add_gauge_value('Member/Operations/Query', 'ops',
                             query_engine.get('queries_per_sec', 0))
        self.add_gauge_value('Member/Operations/Read', 'ops',
                             query_engine.get('read_docs_per_sec', 0))

        self.add_derive_value('Member/Operations/Inserts Processed', 'ops',
                              query_engine.get('written_docs_total', 0))
        self.add_derive_value('Member/Operations/Queries Processed', 'ops',
                              query_engine.get('queries_total', 0))
        self.add_derive_value('Member/Operations/Reads Processed', 'ops',
                              query_engine.get('read_docs_total', 0))

    def add_database_stats(self, member_id):
        """Add all of the data points for each table for each database

        :param string member_id: The member in the cluster to get stats from

        """
        tables = list(r.db('rethinkdb').table('table_status').run())
        for table in tables:
            stats = r.db('rethinkdb').table('stats').get(["table_server", table['id'], member_id]).run()
            ns = 'Database/%s/Table/%s' % (stats['db'], stats['table'])
            query_engine = stats.get('query_engine', dict())
            self.add_gauge_value('%s/Operations/Insert' % ns, 'ops',
                                 query_engine.get('written_docs_per_sec', 0))
            self.add_gauge_value('%s/Operations/Read' % ns, 'ops',
                                 query_engine.get('read_docs_per_sec', 0))

            self.add_derive_value('%s/Operations/Inserts Processed' % ns, 'ops',
                                  query_engine.get('written_docs_total', 0))
            self.add_derive_value('%s/Operations/Reads Processed' % ns, 'ops',
                                  query_engine.get('read_docs_total', 0))

            storage_engine = stats.get('storage_engine', dict())

            cache = storage_engine.get('cache', 0)
            self.add_gauge_value('%s/Storage/Cache' % ns, 'bytes',
                                 cache.get('in_use_bytes', 0))

            disk = storage_engine.get('disk', 0)
            self.add_gauge_value('%s/Storage/Disk/Write' % ns, 'bytes',
                                 disk.get('written_bytes_per_sec', 0))
            self.add_gauge_value('%s/Storage/Disk/Read' % ns, 'bytes',
                                 disk.get('read_bytes_per_sec', 0))
            self.add_derive_value('%s/Storage/Disk/Writes Processed' % ns, 'bytes',
                                  disk.get('written_bytes_total', 0))
            self.add_derive_value('%s/Storage/Disk/Reads Processed' % ns, 'bytes',
                                  disk.get('read_bytes_total', 0))

            space_usage = disk.get('space_usage', 0)
            self.add_gauge_value('%s/Storage/Space/Data' % ns, 'bytes',
                                 space_usage.get('data_bytes', 0))
            self.add_gauge_value('%s/Storage/Space/Garbage' % ns, 'bytes',
                                 space_usage.get('garbage_bytes', 0))
            self.add_gauge_value('%s/Storage/Space/Metadata' % ns, 'bytes',
                                 space_usage.get('metadata_bytes', 0))
            self.add_gauge_value('%s/Storage/Space/Preallocated' % ns, 'bytes',
                                 space_usage.get('preallocated_bytes', 0))


    def connect(self):
        auth_key = self.config.get('auth_key', '')
        try:
            if len(auth_key):
                r.connect(host = self.config.get('host', 'localhost'),
                    port = self.config.get('port', 28015),
                    db = 'rethinkdb').repl()
            else:
                r.connect(host = self.config.get('host', 'localhost'),
                    port = self.config.get('port', 28015),
                    db = 'rethinkdb',
                    auth_key = auth_key).repl()
        except RqlDriverError as error:
            LOGGER.error('Could not connect to RethinkDB: %s', error)

    def get_and_add_server_stats(self):
        LOGGER.debug('Fetching server stats')
        self.add_cluster_datapoints(r.db("rethinkdb").table("stats").get(["cluster"]).run())

    def get_and_add_member_stats(self):
        member_id = self.determine_member_id()
        self.add_member_datapoints(r.db('rethinkdb').table('stats').get(["server", member_id]).run())

    def get_and_add_db_stats(self):
        member_id = self.determine_member_id()
        self.addd_database_stats(member_id)

    def determine_member_id(self):
        server_status = list(r.db("rethinkdb").table("server_status").run())
        for status in server_status:
            for member in list(status['network']['canonical_addresses']):
                if member['host'] == self.config.get('host', 'localhost'):
                    return status['id']

    def poll(self):
        self.initialize()
        self.connect()
        self.get_and_add_server_stats()
        self.get_and_add_member_stats()
        self.get_and_add_db_stats()
        self.close()
        self.finish()