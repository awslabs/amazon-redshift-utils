# Redshift Encryptor

This script will help to encrypt an unencrypted Amazon Redshift cluster to an Encrypted cluster. As part of the script, it will copy your database (objects) from an unencrypted cluster Amazon Redshift to an encrypted Amazon Redshift cluster. The user will need to create a fresh Amazon Redshift cluster with encryption enabled before executing the script which will be used as the destination. The script also gives an option to use the Simple Notification Service(SNS) to monitor the progress of the script.

## AWS Services involved in the script

1. AWS Redshift
2. AWS IAM
3. AWS S3
4. AWS SNS (Optional)

As resources of these services will be spun, there might be some additional charges for using these services. Please refer to the documentation [1] for details about the charges.

## Notes

1. The destination Encrypted cluster should be of higher or same configuration than the source unencrypted cluster.
2. Only one database can be migrated at a time.
3. You will need to create an s3 bucket which will be used to unload and copy the cluster data. The S3 bucket should be in the same region as the source.
4. Please note that passwords cannot be migrated and hence must be reset once the migration is completed. In the case where the destination cluster is not a new cluster, the script will not create duplicate users and groups, and before migrating starts the users(except the super user) with the same name as in the source cluster will be removed from the destination cluster. This is to avoid any collisions in the migration process.
5. Make sure no user in source cluster except master user has the same name as a master user of the destination cluster.
6. Master user from source will not be copied to the destination as the destination will have its own master user. Also, the superuser will not be migrated.
7. Historic information that is stored in STL and SVL tables is not migrated to or retained in the new cluster.
8. Amazon S3 log settings are not migrated, so be sure to enable database audit logging on the new cluster.


## Setting up the environment

1. Please run the following commands in terminal to install python3 and the necessary dependencies in order to run the script:
```sh
$ sudo yum install python36 postgresql postgresql-devel gcc python36-devel libffi-devel
$ curl -O https://bootstrap.pypa.io/get-pip.py
$ python3 get-pip.py --user
$ pip3.6 install PyGreSQL boto3 pytz --user
```

2. Clone the repository using the following command:
```sh
$ git clone https://github.com/#####PATH########
```

3. Change directory to amazon-redshift-utils/src/#####PATH########/ and run the script “migration.py” using python3.
```sh
$ python3 migration.py
```
Please also make sure you make the cluster read only during the process i.e. cut all the write inputs.
Please follow the instructions in the script.


## Running the script

### 1. Select your source Amazon Redshift cluster, and choose Manage IAM Roles.
Note all IAM roles that are associated with your cluster and save the roles to be used in the new cluster.
Choose the Details pane of your cluster, and note the following configurations:
- Node Type
- VPC ID
- VPC security groups
- Cluster Parameter Group
- Enhanced VPC Routing
- Cluster Database Properties
- Port
- Publicly accessible
 
### 2. Run the Script
Please provide the relevant details when prompted during the script runtime otherwise it can impact the migration.
 
### 3. Post Restore
At the time of restore, Make sure the following properties are the same compared to the source.
- Node Type
- Cluster Identifier
- Port
- Cluster subnet group
- Publicly accessible
- Enhanced VPC Routing
- Cluster Parameter Group
- VPC security groups.
 
### 4. Associate IAM roles
- Open the Amazon Redshift console, and choose Clusters from the navigation pane.
- Select the new cluster and choose Manage IAM Roles.
- From the Available roles, choose the roles associated with the source cluster.
- Choose Apply changes.

## References 
[1] https://aws.amazon.com/pricing/services/ 
