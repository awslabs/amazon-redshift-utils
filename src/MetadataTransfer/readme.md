# Metadata Transfer Utility
This utility enables the end user to automate the transfer of metadata from one cluster to another. Metadata includes users, user profiles, groups, databases, schemas and related privileges. The utility is compatible with Python 3.

The utility requries [`redshift_connector`](https://pypi.org/project/psycopg2/) and the [`boto3`](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html)  Python3 libraries. Additionally, the utility requires that you configure the [default region](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-region) for `boto3` to successfully send requests.

The objects which are transfered are:

| Privilege Type | Metadata Copy | User Privileges |
|---|:---:|---|
|Language|Y|Y|
|Database|Y|Y|



## Usage

```sh
python3 metadatacopy.py \
--tgtcluster <target cluster endpoint> \
--srccluster <source cluster endpoint> \
--tgtuser <target superuser> \
--srcuser <source superuser> \
--tgtdbname <target cluster dbname> \
--srcdbname <source cluster dbname> \
[--dbport <database port>]
```

### User Privileges
The 'user_privs.py' script can be used to just transfer the user privileges:

```

```

## Limitations

* While copying object privileges, all or none of the privileges will be transferred to the target. If any privilege fail to apply at target, all privileges will be rolled back for that transaction.

* When copying users, the utility uses a command similar to `create user <username> password disable;`. The password field is set to `disable` because it is not possible (due to security concerns) to extract plain text passwords for users in a cluster.
