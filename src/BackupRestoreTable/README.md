# Redshift Backup Restore Table

A simple table-only backup and restore utility.

## Description:
       
1. To Backup Table level Data from Redshift Cluster to S3
2. To Restore Table level Data from S3 to Redshift Cluster

## Notes:

1. S3 Bucket should be in the same region as Redshift cluster

## DEPENDENCIES:
	    
1. Please run the following command to install the necessary python modules in order to run the script:

```bash
pip install boto3 psycopg2-binary
``` 

2. Run the script:

```bash
python BackupAndRestore_v3.0.py
```

Please follow the instructions in the script.

## AWS Services involved in the script:

1. AWS Redshift
2. AWS IAM
3. AWS S3

## Input Parameters:

The script takes the below parameters:

| Input Name              | Examples       | Description                                              |
| -------------------     | ---------      | -------------------------------------------------------- |
| redshift_regions        | ap-south-1     | Region where redshift cluster is located                 |
| cluster_identifier      | my-redshift    | The unique name used to identify a cluster.              |
| cluster_identifier_pass | ********       | Password used to login to the cluster                    |
| Backup/Restore          | 1. Backup<br>2. Restore  | Select input 1 for backup and 2 for restore           |
| table_name_list         | emp,cust       | Enter all the table name. comma separated without space  |
| s3_location             | redshift-bucket| Enter the name of the of the s3 bucket to store data     |