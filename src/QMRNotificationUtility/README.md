# Amazon Redshift WLM Query Monitoring Rule (QMR) Action Notification Utility

## Release Notes
Version 2.0 has had significant changes to the code.
* Redshift queries submitted via Data API, not via ODBC connection
* No password details required. Data API calls obtain temporary database credentials. Permission to call the `redshift:GetClusterCredentials` operation is required. See the [Data API Documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html)
* The cluster identifier used is NOT the same as the cluster endpoint. Instead of creating the stack with the `RedshiftClusterIdentifier` having the value of `cluster-name.random-letters.region.redshift.amazonaws.com` only the cluster identifier such as `cluster-name` is required.
* Future updates will allow the utility to be utilised with serverless clusters. This release is limited to provisioned clusters only.

## Goals
This utility uses a scheduled Lambda function to pull records from the QMR action system log table (`stl_wlm_rule_action`) and publish them to an SNS topic. This utility can be used to send periodic notifications based on the WLM query monitoring rule actions taken for your unique workload and rules configuration.

In Amazon Redshift workload management (WLM), query monitoring rules define metrics-based performance boundaries for WLM queues and specify what action to take when a query goes beyond those boundaries. For example, for a queue dedicated to short running queries, you might create a rule that aborts queries that run for more than 60 seconds. To track poorly designed queries, you might have another rule that logs queries that contain nested loops.  The rule actions are captured in `stl_wlm_rule_action` system table. For more information about Redshift workload management (WLM) query monitoring rules and how to configure it, please refer to Redshift [Documentation](http://docs.aws.amazon.com/redshift/latest/mgmt/workload-mgmt-config.html)

The utility periodically scans `stl_wlm_rule_action.actions` (log/hop/abort) recorded by WLM query monitoring rules and sends the records as SNS notifications. In summary, a Lambda function is invoked on a scheduled interval, connects to your Redshift cluster, reads events from `stl_wlm_rule_action` and publishes them to an SNS topic as a JSON string.

## Installation Notes:

### Prerequisites
This utility requires the following items:

* VPC: A VPC which currently contains your Amazon Redshift resource and will contain this utility's Lambda function. ***NOTE: VPC ID***

* Private Subnets with NAT route: At least two private subnets within that VPC with private routes to the target Amazon Redshift cluster. You should have a NAT Gateway to give access to the Internet for those subnets' routing tables. You cannot use public subnets. You can read more information on this Lambda requirement here: [AWS blog](https://aws.amazon.com/blogs/aws/new-access-resources-in-a-vpc-from-your-lambda-functions/). ***NOTE: Subnet IDs***

* Security Group: A VPC security group which allows the Lambda function access to your Amazon Redshift cluster on the port specified for SQL connections. ***NOTE: VPC Security Group ID***

* An Amazon Redshift cluster in the above VPC. The cluster identifier is shown on the 'General Information' page on the AWS Redshift console ***NOTE: Amazon Redshift cluster identifier, Port, Database***

* Amazon Redshift username with access to [`STL_WLM_RULE_ACTION`](http://docs.aws.amazon.com/redshift/latest/dg/r_STL_WLM_RULE_ACTION.html). A superuser will be able to see all rows in this table, and a non-privileged user will be able to see only their own rows. More on visibility here: [Visibility of Data in System Tables and Views](http://docs.aws.amazon.com/redshift/latest/dg/c_visibility-of-data.html). Note that only the username is required here. Database access uses the Redshift Data API which handles authentication through the IAM roles created as part of the stack.

* An active WLM configuration with QMR enabled ([Documentation](http://docs.aws.amazon.com/redshift/latest/mgmt/workload-mgmt-config.html)).

* Access to an IAM user with privileges to create and modify the necessary CloudFormation, IAM, SNS, and CloudWatch Events resources.

* A locally cloned amazon-redshift-utils project containing this utility and AWS CLI and/or AWS Console access. 

### Installation from CloudFormation Template:

The quickest way to get up and running with the QMRNotificationUtility is by leveraging the packaged CloudFormation template and the AWS CLI. Making sure AWS CLI default region is configured correctly to be matched with Redshift cluster region otherwise, use --region switch in front of aws command to set the correct region in each step going forward.

#### 1. Navigate to the QMRNotificationUtility's directory within the amazon-redshift-utils project:

```
git clone/pull amazon-redshift-utils
cd amazon-redshift-utils/src/QMRNotificationUtility
```

#### 2. Copy the zipped python Deployment Package for the Lambda function to a location of your choosing in S3:

```bash
aws s3 cp ./lambda/dist/qmr-action-notification-utility-2.0.zip s3://yourbucket/qmr-action-notification-utility-2.0.zip
```

#### 3. Gather the necessary identifiers noted in the prerequistes section above:

* VPC ID
* Subnet ID(s)
* Security Group ID(s)
* Cluster Identifier
* Cluster Port
* Cluster Database
* Cluster Credentials (Username)
* Bucket to host the Lambda Deployment Package
* Email address to be notified of WLM actions

#### 4. Create the Lambda Function

Use the AWS CLI to create a stack containing the necessary dependencies and Lambda function:

```bash
aws cloudformation create-stack \
--stack-name qmr-action-notification-utility \
--template-body file://./cloudformation/qmr-action-notification-utility.yaml \
--parameters \
  ParameterKey=S3Bucket,ParameterValue=yourbucket \
  ParameterKey=S3Key,ParameterValue=qmr-action-notification-utility-2.0.zip \
  ParameterKey=SNSEmailParameter,ParameterValue=test@email.com \
  ParameterKey=VPC,ParameterValue=vpc-abcd1234 \
  ParameterKey=SubnetIds,ParameterValue=subnet-abcdid1\\,subnet-abcdid2 \
  ParameterKey=SecurityGroupIds,ParameterValue=sg-abcd1234 \
  ParameterKey=RedshiftMonitoringUser,ParameterValue=monitoring_user \
  ParameterKey=RedshiftClusterPort,ParameterValue=cluster_port \
  ParameterKey=RedshiftClusterIdentifier,ParameterValue=examplecluster \
  ParameterKey=RedshiftClusterDatabase,ParameterValue=db_name \
--capabilities CAPABILITY_IAM
```

#### 5. Verify creation is complete

It may take a few mintues for the stack's resources to be provisioned, and is completed when the following command returns "CREATE_COMPLETE":

```bash
aws cloudformation describe-stacks --stack-name qmr-action-notification-utility --query 'Stacks[0].StackStatus' --output text
```

#### 6. Check the inbox of the email address you included for SNSEmailParameter. 
There should be an "AWS Notification - Subscription Confirmation" from no-reply@sns.amazonaws.com asking that you confirm your subscription. Click the link if you wish to receive updates on this email address.  

#### 7. Verify the email address receives an email notification within 5 minutes

By purposely triggering a QMR action by manually running SQL that is known to violate a rule defined in your active WLM configuration. Below is one example SNS notification email message:

```json
[
  {
     "clusterid":"examplecluster",
     "database":"dev",
     "userid":100,
     "query":1186777,
     "service_class":6,
     "rule":"Rule1_Nested_Loop",
     "action":"abort",
     "recordtime":"2017-06-12T06:51:57.167052"
  },
  {
      "clusterid":"examplecluster",
      "database":"dev",
      "userid":100,
      "query":1186999,
      "service_class":6,
      "rule":"Rule2_high_Return_Rows",
      "action":"log",
      "recordtime":"2017-06-12T06:53:48.935375"
   }
]
```

### Rebuilding Lambda Function

If you wish to rebuild the Lambda function yourself, you can use `lambda/build.sh` to create a zipped Deployment Package to upload to your S3 bucket. This utility requires `pip` and `virtualenv` python dependencies. This script will initialize a transient virtual environment, download python dependencies from `requirements.txt`, and zip the lambda function source code with dependencies into a versioned archive for uploading to S3.
