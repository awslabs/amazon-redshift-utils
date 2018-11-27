# On Demand Redshift

This CloudFormation will help you to run your Redshift cluster in specific hours of the day based on your requirement. As part of this template, at the end of your scheduled window, this will take a backup of the cluster before deleting the cluster and will restore it from the latest backup based on your requirement. It is automated using Amazon Lambda and Amazon Cloudwatch rules. This is a one time schedule operation.

:warning: Warning: As a part of this CloudFormation template your specified Redshift cluster will be DELETED and again restored from the latest snapshot at the timeline defined by you.

## AWS Services involved in the script

1. AWS Redshift
2. AWS IAM
3. AWS S3
4. AWS Cloudwatch Events
5. AWS Lambda
6. AWS CloudFormation

As resources of these services will be spun, there might be some additional charges for using these services. Please refer the documentation [1] for details about the charges.


## Workflow

![onDemand Workflow](onDemandFlow.png)


## Input Parameters:

Make sure the user creating the cloud formation stack has required service access. For easy setup, you can use a user with full admin access.

The cloud formation templates takes the below parameters:

| Input Name              | Examples       | Description                                              |
| -------------------     | ---------      | -------------------------------------------------------- |
| Stack Name          | onDemandStack    | Name for your CloudFromation stack                 	    |
| Cluster Name		  | my-redshift-cluster     | Name of your Amazon Redshift cluster                  |
| Delete Schedule          	  | 0 6 * * ? *		   | Corn expression for deleting the cluster [2]     |
| Creation Hour             | 14		   | Enter the hour in UTC when you want to restore the cluster(0-23)    |
| Creation Minutes               | 50		   | Enter the minute in UTC when you want to restore the cluster(0-59)   |



## Notes:
1. The code will not work if the number of snapshots exceed the snapshot limit allotted to your account. Please refer the documentation [3][4] for more information on snapshot limits. You can reach [AWS Premium support](https://aws.amazon.com/premiumsupport/) to increase the snapshot limits.

2. Do not delete the following resources created by the code, they are required to manage the cluster availability schedule. <br>
	a. Lambda function <br>
	b. Cloudwatch rules: (create & delete) <br>
	c. Policy  <br>
	d. IAM Role <br>

## References 
[1] https://aws.amazon.com/pricing/services/ <br>
[2] https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html <br>
[3] https://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html#limits_redshift <br>
[4] https://docs.aws.amazon.com/redshift/latest/mgmt/amazon-redshift-limits.html
