# PROGRAM NAME:
#       BackupAndRestore_v3
#
# DESCRIPTION:
#       1. To Backup Table level Data from Redshift Cluster to S3
#       2. To Restore Table level Data from S3 to Redshift Cluster
#
# DEPENDENCIES:
#	    
#		Please run the following command to install the necessary python modules in order to run the script:
#
#	    # pip install boto3 psycopg2-binary 
#
# ASSUMPTIONS: 
#       1. S3 Bucket should be in the same region as Redshift cluster
#
# TRIGGER:
#       python BackupAndRestore_v2.py
#
# SERVICES:
#       AWS Redshift
#       AWS IAM
#       AWS S3
#
# INPUT PARAMETERS:
#       | ------------------------| ---------------| -------------------------------------------------------- |
#       | Input Name              | Examples       | Description                                              |
#       | -------------------     | ---------      | -------------------------------------------------------- |
#       | redshift_regions        | ap-south-1     | region where redshift cluster is located                 |
#       | -------------------     | ---------      | -------------------------------------------------------- |
#       | cluster_identifier      | my-redshift    | the unique name used to identify a cluster.              |
#       | -------------------     | ---------      | -------------------------------------------------------- |
#       | cluster_identifier_pass | ********       | password used to login to the cluster                    |
#       | -------------------     | ---------      | -------------------------------------------------------- |
#       | a                       | 1. Backup      | select an input 1 for backup and 2 for restore           |
#       |                         | 2. Restore     |                                                          |
#       | -------------------     | ---------      | -------------------------------------------------------- |
#       | table_name_list         | emp,cust.      | enter all the table name. comma separated without space  |
#       | -------------------     | ---------      | -------------------------------------------------------- |
#       | s3_location             | redshift-bucket| enter the name of the of the s3 bucket to store data     |
#       | -------------------     | -------------- | -------------------------------------------------------- |
#
