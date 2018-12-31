import sys
import click
import logging
from snapshot_repository import S3Handler
from cassandra_handler import CassandraHandler
from snapshot_metadata import SnapshotMetadata
from utils import (cassandra_backup_to_s3, get_environment_variable, validate_aws_permissions)

# Optional environment variables
HOSTNAME = get_environment_variable('HOSTNAME')
AWS_ACCESS_KEY = get_environment_variable('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = get_environment_variable('AWS_SECRET_ACCESS_KEY')


@click.group()
def cli():
    pass


@click.command()
@click.option('--log-level', default="info")
@click.option('--verbose', is_flag=True)
@click.option('--ssl-no-verify', is_flag=True)
@click.option('--node', default=HOSTNAME)
@click.option('--bucket', required=True)
@click.option('--aws-access-key', default=AWS_ACCESS_KEY)
@click.option('--aws-secret-key', default=AWS_SECRET_KEY)
@click.option('--cassandra-data-dir', default='/var/lib/cassandra/data')
@click.option('--cassandra-bin-dir', default='/bin')
@click.option('--snapshot-type', type=click.Choice(['full', 'incremental']), default='full')
@click.option('--upload-chunksize', default=10, type=int)
@click.option('--upload-concurrency', default=25*1024, type=int)
@click.option('--upload-workers', default=1, type=int)
@click.option('--s3-storage-class', type=click.Choice(['STANDARD', 'STANDARD_IA', 'REDUCED_REDUNDANCY']),
              default='STANDARD')
def snapshot(log_level, verbose, ssl_no_verify, node, bucket, aws_access_key, aws_secret_key, cassandra_data_dir,
             cassandra_bin_dir, snapshot_type, upload_chunksize, upload_concurrency, upload_workers, s3_storage_class):
    try:
        logging.basicConfig(level=logging.getLevelName(log_level.upper()),
                            format='[%(levelname)s] [%(asctime)s] %(message)s')
        validate_aws_permissions(aws_access_key, aws_secret_key)

        repository_handler = S3Handler(bucket, aws_access_key, aws_secret_key, ssl_no_verify, upload_chunksize,
                                       upload_concurrency, upload_workers, s3_storage_class)
        cassandra_handler = CassandraHandler(node, cassandra_data_dir, cassandra_bin_dir, snapshot_type)
        snapshot_metadata = SnapshotMetadata(cassandra_handler, repository_handler)

        cassandra_backup_to_s3(snapshot_metadata, repository_handler, cassandra_handler, upload_workers, verbose)
        cassandra_handler.clear_snapshot()
    except Exception as e:
        sys.exit(e)


cli.add_command(snapshot)

if __name__ == "__main__":
    cli()