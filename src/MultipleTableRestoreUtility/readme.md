# Multiple Table restore utility
This utility enables the end user to automate the restore of multiple tables onto a Redshift cluster from a saved snapshot of that cluster. The list of tables is defined in a JSON formatted file. See the example JSON below:

```
{
    "TableRestoreList": [
        {
            "SrcSchemaname": "src_schema1",
            "SrcTablename": "src_table1",
            "TgtSchemaname": "tgt_schema1",
            "TgtTablename": "tgt_table1"
        },
        {
            "SrcSchemaname": "src_schema2",
            "SrcTablename": "src_table2",
            "TgtSchemaname": "tgt_schema2",
            "TgtTablename": "tgt_table2"
        },
        {
            "SrcSchemaname": "src_schema3",
            "SrcTablename": "src_table3",
            "TgtSchemaname": "tgt_schema3",
            "TgtTablename": "tgt_table3"
        }
    ]
}
```

The keys 'TableRestoreList' , 'SrcSchemaname', 'SrcTablename', 'TgtSchemaname', and 'TgtTablename' must be used in order for the utility to work. The utility is compatible with Python 3. 
## Setup
The AWS credentials can be provided with one of 3 methods:

1. Shared credential file (~/.aws/credentials)
1. AWS config file (~/.aws/config)
1. Setting environment variables

### Setting Environment Variables
The utility uses the Boto3 library which will check these environment variables for credentials:

* AWS_ACCESS_KEY_ID - The access key for your AWS account.
* AWS_SECRET_ACCESS_KEY - The secret key for your AWS account.
* AWS_SESSION_TOKEN - The session key for your AWS account. This is only needed when you are using temporary credentials.

## Usage
```python3 multitablerestore.py --cluster-identifier <cluster name> --snapshot-identifier <snapshot name> --source-database-name <source database> --target-database-name <target database> --listfile <filename>```

Note: The source and target databases can be the same.

## Limitations
1. The table cannot already exist on the target database
1. The table can only be restored to the same cluster the snapshot was taken from
1. Script does not currently check if cluster has enough space, make sure enough space is available when restoring several tables
