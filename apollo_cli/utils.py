import os
import logging
import datetime
from constants import (SNAPSHOT_METADATA_FILE, SUCCESS_FIELD_METADATA)
from apollo_exceptions import AWSCredentialsError

logger = logging.getLogger(__name__)


def cassandra_backup_to_s3(metadata, s3_repository, cassandra, verbose=True):
    snapshot_timestamp = metadata.snapshot_date
    snapshot_remote_base_path = os.path.join(snapshot_timestamp, cassandra.node, cassandra.snapshot_type)
    metadata_remote_path = os.path.join(snapshot_remote_base_path, SNAPSHOT_METADATA_FILE)
    s3_repository.save_metadata(metadata.json(), metadata_remote_path)

    logging.info("Starting upload snapshots to S3")
    for keyspace in cassandra.sstables_to_snapshot.keys():
        logger.info("Keyspace - {keyspace}".format(keyspace=keyspace))
        for table in cassandra.sstables_to_snapshot[keyspace].keys():
            logger.info("Column family - {table}".format(table=table))
            for sstable in cassandra.sstables_to_snapshot[keyspace][table]:
                sstable_name = os.path.basename(sstable)
                logger.debug("SSTable - {sstable}".format(sstable=sstable))
                full_repository_sstable_path = os.path.join(snapshot_remote_base_path,
                                                            keyspace, table, sstable_name)
                s3_repository.upload(sstable, full_repository_sstable_path, verbose=verbose)

    metadata.status = SUCCESS_FIELD_METADATA
    s3_repository.save_metadata(metadata.json(), metadata_remote_path)
    cassandra.clear_snapshot()
    logger.info("Finished backup successfully")


def generate_snapshot_timestamp():
    snapshot_timestamp = datetime.datetime.today().strftime('%Y-%m-%d')
    return snapshot_timestamp


def get_environment_variable(environment_variable_key):
    try:
        environment_variable_value = os.environ[environment_variable_key]
        return environment_variable_value
    except KeyError:
        pass


def validate_aws_permissions(access_key, secret_key):
    if access_key is None or secret_key is None:
        raise AWSCredentialsError("Please specify aws access key and aws secret key")
    else:
        pass


def extract_keyspace_map(cassandra_sstable_fs_tree):
    keyspace_map = dict()
    for keyspace in cassandra_sstable_fs_tree.keys():
        keyspace_map[keyspace] = list()
        for table in cassandra_sstable_fs_tree[keyspace].keys():
            keyspace_map[keyspace].append(table)
    return keyspace_map
