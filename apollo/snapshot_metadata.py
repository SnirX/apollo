import json
from utils import (generate_snapshot_timestamp, extract_keyspace_map)
from constants import INITIAL_FIELD_METADATA


class SnapshotMetadata(object):
    def __init__(self, cassandra_object, s3_repo_object, snapshot_status=INITIAL_FIELD_METADATA):
        self._snapshot_type = cassandra_object.snapshot_type
        self._snapshot_date = generate_snapshot_timestamp()
        self._node = cassandra_object.node
        self._keyspace_map = extract_keyspace_map(cassandra_object.sstables_to_snapshot)
        self._bucket = s3_repo_object.bucket
        self._status = snapshot_status

    @property
    def snapshot_date(self):
        return self._snapshot_date

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    def json(self):
        json_data = dict()
        json_data["snapshot_type"] = self._snapshot_type
        json_data["snapshot_date"] = self._snapshot_date
        json_data["node"] = self._node
        json_data["keyspaces"] = self._keyspace_map
        json_data["status"] = self._status

        return json.dumps(json_data)
