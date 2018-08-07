# On Demand Redshift

This script will help you to run your Redshift cluster in specific hours of the day based on your requirement. As part of the script, at the end of your scheduled window, this script will take a backup of the cluster before deleting the cluster and will restore it from the latest backup based on your requirement. It is automated using Amazon Lambda and Amazon Cloudwatch rules. This is a one time schedule operation.

## Setting the environment
1. Make sure the user running the script has required service access. For easy setup, you can use a user with full admin access.
2. Configure aws credentials using "aws configure" in terminal. Enter the default region same as where your Amazon Redshift cluster is.


## AWS Services involved in the script

1. AWS Redshift
2. AWS IAM
3. AWS S3
4. AWS Cloudwatch Events
5. AWS Lambda

As resources of these services will be spun, there might be some additional charges for using these services. Please refer the documentation [1] for details about the charges.


## Running the script

1. Please run the following commands in terminal to install the necessary python modules in order to run the script:
```bash
pip3 install boto3
pip3 install botocore
pip3 install awscli
pip3 install requests
```

2. Clone the repository using the following command:
```bash
git clone https://github.com/suvenduk/amazon-redshift-utils.git
```

3. Change directory to amazon-redshift-utils/src/ondemand_redshift/ and run the script “Ondemand_Lamb.py” using python3.
```bash
python3 Ondemand_Lamb.py
```

Please follow the instructions in the script.


## Input Parameters:

The script takes the below parameters:

| Input Name              | Examples       | Description                                              |
| -------------------     | ---------      | -------------------------------------------------------- |
| cluster_region          | ap-south-1     | Region where redshift cluster is located                 	    |
| clusterName		  | my-redshift-cluster     | Name of your Amazon Redshift cluster                  |
| deleteTime(hour)          	  | 09		   | Enter the hour in UTC when you want to delete the cluster(0-23)      |
| deleteMin	          | 05             | Enter the minute in UTC when you want to delete the cluster(0-59)    |
| createTime(hour)              | 14		   | Enter the hour in UTC when you want to restore the cluster(0-23)    |
| createMin               | 50		   | Enter the minute in UTC when you want to restore the cluster(0-59)   |



## Notes:
1. The code will not work if the number of snapshots exceed the snapshot limit allotted to your account. Please refer the documentation [2][3] for more information on snapshot limits. You can reach [AWS Premium support](https://aws.amazon.com/premiumsupport/) to increase the snapshot limits.

2. Do not delete the following resources created by the code, they are required to manage the cluster availability schedule. <br>
	a. Lambda function: Redshift-ondemand-function <br>
	b. Cloudwatch rules: Ondemand-createCluster-rule   AND  Ondemand-deleteCluster-rule <br>
	c. Policy: Ondemand_CW_log_policy  <br>
	d. Role: ondemand-redshift-role-do-not-delete <br>

## References 
[1] https://aws.amazon.com/pricing/services/ <br>
[2] https://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html#limits_redshift <br>
[3] https://docs.aws.amazon.com/redshift/latest/mgmt/amazon-redshift-limits.html
