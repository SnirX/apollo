import threading
import boto3
import os
import sys
import logging
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from abc import (ABCMeta, abstractmethod)
from apollo_exceptions import S3UploadError

logger = logging.getLogger(__name__)


class RepositoryTemplate(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def upload(self, *args, **kwargs):
        pass

    @abstractmethod
    def download(self, *args, **kwargs):
        pass


class S3Handler(RepositoryTemplate):
    def __init__(self, bucket_name, aws_access_key_id, aws_secret_key_id, ssl_no_verify, upload_chunksize,
                 upload_concurrency, upload_workers, storage_class):
        self._bucket_name = bucket_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_key_id = aws_secret_key_id
        self._session = boto3.Session(
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_key_id
        )
        self._upload_chunksize = upload_chunksize
        self._upload_workers = upload_workers
        self._s3_conn = self._session.resource('s3', verify=not ssl_no_verify,
                                               config=Config(max_pool_connections=self._upload_workers * 15))
        self._upload_concurrency = self.convert_byte_to_kb(upload_concurrency)
        self._storage_class =  storage_class

    @property
    def bucket(self):
        return self._bucket_name

    def upload(self, local_file_path, s3_key_path, verbose=True):
        if self._upload_concurrency > 1:
            threaded_upload = True
        else:
            threaded_upload = False

        config = TransferConfig(multipart_threshold=self._upload_chunksize, max_concurrency=self._upload_concurrency,
                                multipart_chunksize=self._upload_chunksize, use_threads=threaded_upload)

        logger.debug("Uploading - {sstable}".format(sstable=local_file_path))

        if verbose:
            self._s3_conn.meta.client.upload_file(local_file_path, self._bucket_name, s3_key_path,
                                                  Config=config,
                                                  Callback=_UploadProgressPercentage(local_file_path),
                                                  ExtraArgs={'StorageClass': self._storage_class}
                                                  )
        else:
            self._s3_conn.meta.client.upload_file(local_file_path, self._bucket_name, s3_key_path, Config=config,
                                                  ExtraArgs={'StorageClass': self._storage_class})

    def download(self):
        pass

    def save_metadata(self, metadata, remote_path):
        logger.info("Saving snapshot metadata to S3, remote path - {remote_path}".format(remote_path=remote_path))
        try:
            response = self._s3_conn.meta.client.put_object(
                Bucket=self._bucket_name,
                Body=metadata,
                Key=remote_path
            )
            logger.info("Metadata is saved")
            return response
        except Exception as e:
            raise S3UploadError(e)

    @staticmethod
    def convert_byte_to_kb(num):
        return num * 1024


class _UploadProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._file_size = float(os.path.getsize(filename))
        self._uploaded = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._uploaded += bytes_amount
            percentage = (self._uploaded / self._file_size) * 100
            sys.stdout.write(
                "\r%s  %s bytes / %s bytes (%.2f%%)" % (
                    self._filename, self._uploaded, self._file_size,
                    percentage))
            sys.stdout.write("\n")
            sys.stdout.flush()

