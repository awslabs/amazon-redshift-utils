# Generate Unload commands and Execute
This utility generates Unload to Parquet commands with partition keys and sort keys(optional). If config set to Execute, the utility also executes the unload commands.The `config.ini` files needs to have the information necessary to generate the commands.
Please look for example `config.ini` in the current directory.

Requirements:
```
python3
boto3
pgpasslib
pg8000
```
Follow .pgpass file requirements for pgpasslib from `https://pgpasslib.readthedocs.io/en/latest/`

The `PGPASSFILE` env variable must be set and point to a valid .pgpass file 

Default section has some basic configs set to default values
```
[DEFAULT]
debug=True
execute=False
schema=public
```

`cluster` section covers cluster and table details. `identifier` is host external IP, `table` is tablename to be unloaded, `partition_key` is the partition column (takes only one column), `sort_keys` are optional, `column_list` if specified will use the columns for unload, if not specified then it uses all columns( except for partition column) in the unload command. `debug` is True by default and generates additional information. `execute` if set to True will execute the generated unload commands, while set to False will not. First time set to False and check the generated unload statements for correctness.
```
[cluster]
identifier = 54.161.151.1
dbuser = dbadmin
database = dev
port = 5439
schema = public
table = store_dim
partition_key = c1
sort_keys = c1,c2
column_list = c1,c2
debug=True
execute=False
```

`PATH` - s3 path for unload to write , no single quotes needed
```
[S3]
PATH = s3://mybucket/myprefix/tablename/
```

`IAM_ROLE` - iam role to use for unload
```
[CREDS]
IAM_ROLE = arn:aws:iam::XXXXXX9322:role/Redshift-S3-Write
```
## Usage
```python3 genunload3.0.py```

## Limitations
Doesnt support nested partitions like (2019/01/01)
