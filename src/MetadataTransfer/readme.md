# Metadata transfer utility
This utility enables the end user to automate the transfer of metadata from one cluster to another. Metadata includes users, user profiles, groups, databases, schemas and related privileges. The utility is compatible with python 2.7.

The utility requries [`psycopg2`](https://pypi.org/project/psycopg2/) and the `boto3` SDK. Additionally, the utility requires that you configure the [default region](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-region) for `boto3` to successfully send requests.

## Usage
```python metadatatransfer.py --tgtcluster <target cluster endpoint> --srccluster <source cluster endpoint> --tgtuser <target superuser> --srcuser <source superuser> --tgtdbname <target cluster dbname> --srcdbname <source cluster dbname>``` 

## Limitations
While copying object privileges, all or none of the privileges will be transferred to the target. If any privilege fail to apply at target, all privileges will be rolled back for that transaction.