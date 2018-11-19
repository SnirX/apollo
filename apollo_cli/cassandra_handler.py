import os
import logging
from subprocess import (Popen, PIPE)
from constants import (NODETOOL_COMMAND, NODETOOL_FLUSH_ARG, NODETOOL_SNAPSHOT_ARG, NODETOOL_CLEAR_SNAPSHOT_ARG)
from apollo_exceptions import (CassandraFlushError, CassandraSnapshotError, CassandraOSError)
from cassandra_snapshot_exclude import (keyspaces_exclude, tables_exclude)

logger = logging.getLogger(__name__)


class CassandraHandler(object):

    def __init__(self, node, data_directory, bin_directory, snapshot_type="full"):
        self._node = node
        self._snapshot_type = snapshot_type
        self._data_directory = data_directory
        self.bin_directory = bin_directory
        self._nodetool = os.path.join(self.bin_directory, NODETOOL_COMMAND)
        self._snapshot_id = self.export_snapshot() if self._snapshot_type == "full" else None
        self._sstables_to_snapshot = self._return_sstables_list()
        self._flushdb()

    @property
    def node(self):
        return self._node

    @property
    def snapshot_type(self):
        return self._snapshot_type

    @property
    def sstables_to_snapshot(self):
        return self._sstables_to_snapshot

    @property
    def data_directory(self):
        return self._data_directory

    @property
    def snapshot_id(self):
        return self._snapshot_id

    def _flushdb(self):
        try:
            logger.info("Flushing Cassandra memtables")
            process = Popen([self._nodetool, NODETOOL_FLUSH_ARG], stdout=PIPE)
            process.communicate()
            exit_code = process.wait()
            logger.info("Cassandra memtables are flushed to disk")
        except Exception as e:
            raise CassandraFlushError(e)

        if exit_code == 0:
            return self
        else:
            raise CassandraFlushError("Failed to flush DB")

    def export_snapshot(self):
        try:
            logger.info("Performing snapshot to disk")
            process = Popen([self._nodetool, NODETOOL_SNAPSHOT_ARG], stdout=PIPE)
            snapshot_id = self.extract_snapshot_id(process.stdout.readlines())
            process.communicate()
            exit_code = process.wait()
            logger.info("Finished snapshot request - snapshot id: {snapshot_id}".format(snapshot_id=snapshot_id))
        except Exception as e:
            raise CassandraSnapshotError(e)

        if exit_code == 0:
            return snapshot_id
        else:
            raise CassandraSnapshotError("Failed to export snapshot")

    def clear_snapshot(self):
        try:
            process = Popen([self._nodetool, NODETOOL_CLEAR_SNAPSHOT_ARG, '-t', str(self._snapshot_id)], stdout=PIPE)
            exit_code = process.wait()
            logger.info("Snapshot id - {snapshot_id} is cleared from disk".format(snapshot_id=self._snapshot_id))
        except Exception:
            raise CassandraSnapshotError("failed to run {nodetool_command} {clearsnapshot_arg}".format(
                nodetool_command=self._nodetool, clearsnapshot_arg=NODETOOL_CLEAR_SNAPSHOT_ARG))

        if exit_code == 0:
            return self
        else:
            raise CassandraSnapshotError("Failed to flush DB")

    def _return_sstables_list(self):
        sstables_to_snapshot = self._sstables_fs_tree_crawler()
        return sstables_to_snapshot

    def _sstables_fs_tree_crawler(self):
        backup_dir_suffix = self._return_snapshot_suffix()
        sstables_fs_tree = dict()

        logger.info("Generating Cassandra data directory structure from path - {data_dir_path}".format(
            data_dir_path=self._data_directory))
        try:
            keyspaces = os.listdir(self.data_directory)

            for keyspace in keyspaces:
                if keyspace in keyspaces_exclude:
                    continue 
                sstables_fs_tree[keyspace] = dict()
                keyspace_path = os.path.join(self.data_directory, keyspace)
                tables = os.listdir(keyspace_path)

                for table in tables:
                    if keyspace in tables_exclude and table in tables_exclude[keyspace]:
                        continue
                    sstables_fs_tree[keyspace][table] = list()
                    table_path = os.path.join(keyspace_path, table)
                    sstables_snapshot_path = os.path.join(table_path, backup_dir_suffix)
                    sstables = os.listdir(sstables_snapshot_path)

                    for sstable in sstables:
                        sstable_path = os.path.join(sstables_snapshot_path, sstable)
                        sstables_fs_tree[keyspace][table].append(sstable_path)
        except OSError as e:
            raise CassandraOSError(e)

        logger.info("Finished generating data directory structure")
        return sstables_fs_tree

    def _return_snapshot_suffix(self):
        if self._snapshot_type == "full":
            backup_dir_suffix = os.path.join("snapshots", self._snapshot_id)
        elif self._snapshot_type == "incremental":
            backup_dir_suffix = "backups"
        else:
            raise CassandraSnapshotError("Please specify either full or incremental snapshots")

        return backup_dir_suffix

    @staticmethod
    def extract_snapshot_id(snapshot_message):
        return snapshot_message[1].split(' ')[2].rstrip()
