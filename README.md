Apollo - Cassandra snapshot/restore tool
========================================

Apollo is a snapshot/restore tool for Cassandra database using snapshots and incremental backups and storing them in AWS S3.

Installation
------------
``` bash
pip install apollo-cli
```

Usage
-----
``` bash
apollo snapshot --bucket "example_bucket" \
                --node "node1"  \ # optional - default is hostname
		--aws-access-key "XXXX" \ # Can be taken from environment variable AWS_ACCESS_KEY_ID
		--aws-secret-key "ZZZZ" \ # Can be taken from environment variable AWS_SECRET_ACCESS_KEY
		--cassandra-data-dir "/data" \ # Optional - default is /var/lib/cassandra/data
		--cassandra-bin-dir "/bin" \ # Optional - default is /bin
		--snapshot-type "full" \ # Optional - default is full, options are full/incremental
		--verbose \ # Optional - prints uploads statistics per SSTable
		--log-level "info" # Optional - default is info

```
