class CassandraFlushError(Exception):
    pass


class CassandraSnapshotError(Exception):
    pass


class CassandraOSError(Exception):
    pass


class S3UploadError(Exception):
    pass


class AWSCredentialsError(Exception):
    pass
