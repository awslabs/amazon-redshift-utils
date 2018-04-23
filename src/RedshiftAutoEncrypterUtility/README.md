# Redshift Auto Encrypter Utility

An automated script which takes care of Redshift migration from unencrypted to encrypted cluster.

## Running the Script

You can follow the below instructions on you local linux machine but we recommend launching a Amazon linux EC2 Instance and attach a role to it with admin access in order to simplify the process of installing dependencies for the script. For instructions on how to do this, please refer below links:

https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/launching-instance.html<br>
https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

Also, copy the script (migrate-production.py) to one of your S3 buckets.

1. Download the script:

```bash
wget https://s3.<region>.amazonaws.com/<bucket-name>/<path>/migrate-production.py
```

Alternatively you can use scp linux utility to download it to the EC2 instance from your local machine.

scp : https://www.computerhope.com/unix/scp.htm

2. Install python 3 and dependencies for running the script:

```bash
sudo yum install python36 postgresql postgresql-devel gcc python36-devel libffi-devel
curl -O https://bootstrap.pypa.io/get-pip.py
python3 get-pip.py --user
pip3.6 install PyGreSQL boto3 pytz --user
```

3. Run the script

```bash
python3 migrate-production.py
```

Please follow the instructions in the script to complete the migration.

## User Notes

1. User must have a source unencrypted cluster and destination Encrypted cluser with higher or same configuration.
2. User can only migrate one database at a time 
3. Passwords cannot be migrated and hence must be reset once migration is completed.
4. An S3 bucket needs to be provided to the script for Unload and Copy.
5. Moreover, S3 bucket should be in same region as source.
6. Master user from source will not be copied to destination as destination will have its own master user.
7. A temporary role is created by this script which will have S3 access, so please do not modify the role while the script is running.
8. Amazon Simple Storage Service (Amazon S3) log settings are not migrated, so be sure to enable database audit logging on the new cluster.
9. Historic information that is stored in STL and SVL tables is not migrated to or retained in the new cluster.
10. Make sure your AWS CLI is configured with proper credentials.
11. Make sure you install the dependencies for the script as mentioned above.

## Limitations:

1. Use new cluster as a destination cluster is recommended to avoid namespace clashes which may result in migration failure.

2. Duplicate users and groups are not supported by the script on the new cluster. So, if for some reason the script failed after creating users and groups on the new cluster and the second time you run the script it throws the below error:

```bash
'USER EXISTS OR GROUP EXISTS WITH THE SAME NAME'
```

Then please comment the below lines in the script:

Under getting users from source:
------------------------------

```bash
<!-- for queries in q.getresult():
	for query in queries:
		dqueries.append(query+" with password '"+commonkey+"';") -->
```

Under getting groups from source:
-------------------------------

```bash
<!-- for queries in q.getresult():
	 for query in queries:
	 	dqueries.append(query) -->
```
