# Amazon Redshift Snapshot Manager

Amazon Redshift is a fast, fully managed, petabyte-scale data warehouse that makes it simple and cost-effective to analyze all your data using your existing business intelligence tools. A Redshift Cluster is made up of a single leader node, and multiple compute nodes which communicate via a high performance 10Gb network. This cluster is automatically backed up to Amazon S3, and snapshots of the cluster are retained for the amount of time you specify. You can restore into new clusters at any time from existing snapshots, without having to use any third-party backup/recovery software.

This module gives you the ability to coordinate the Automatic Snapshot mechanism in your Amazon Redshift Clusters so that you can meet your disaster recovery requirements. You don't have to write any code or manage any servers; all execution is done within [AWS Lambda](https://aws.amazon.com/lambda), and scheduled with Amazon CloudWatch Events.

## Addressing your Disaster Recovery requirements

There are two dimensions to disaster recovery which must be carefully considered when running a system at scale:

* RTO: Recovery Time Objective - how long does it take to recovery from  disaster recovery scenario
* RPO: Recovery Point Objective - when you have recovered, at what point in time will the system be consistent to?

A comprehensive overview of how to build systems which implement best practices for disaster recovery can be found [here](https://aws.amazon.com/blogs/aws/new-whitepaper-use-aws-for-disaster-recovery/).

## Recovery Time Objective in Redshift

When using Amazon Redshift, your RTO is dictated by the size of the cluster, and the node type you are using. It is vital that you restore from the snapshots created on the cluster to correctly determine the time it will take to bring up a new cluster from a snapshot, and ensure you re-test any time you resize the clsuter or your data volume changes significantly.

## Recovery Point Objective in Redshift

Amazon Redshift's automatic recovery snapshots are created every 8 hours, or every 5GB of changed data on disk, whichever comes first. For some customers, an 8 hour RPO is too long, and they require the ability to take snapshots more frequently. That's where this module comes in - by supplying a simple configuration, you can ensure that snapshots are taken on a specified basis that meets your needs for data recovery.

## Getting Started

### Deploy the Lambda Function

You can now generate an AWS Lambda compatible archive by running:

```build.sh```

This will build RedshiftSnapshotManager-\<version\>.zip, which you can deploy once with a configuration that manages multiople clusters, or you can have a single Lambda function per config location on S3.

You can now deploy this AWS Lambda function by hand using the AWS Console or Command Line tools, or alternatively to the above command, you can run

```build.sh deploy <role-arn>```

This command will automatically build the Lambda function as before, and then deploy it to AWS Lambda in your configured account and configure it to assume the IAM role indicated. The deployed module will be published with:

* Timeout: 60 Seconds
* Memory Size: 128 MB
* Runtime: Node.js 4.3

### Schedule Execution

This Lambda function can be run by any scheduler, but [AWS CloudWatch Scheduled Events](http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/WhatIsCloudWatchEvents.html) are a great way to ensure your function runs periodically. You can configure CloudWatch Events to be used as the event source for your function, and this includes the configuration for which cluster to work on!


```build.sh deploy <role-arn> schedule```

* ```snapshotIntervalHours``` The Recovery Point Objective that is used to ensure you take snapshots on the specified interval
* ```snapshotRetentionDays``` How long snapshots should be retained before being deleted. Manual snapshots (which can be restored into new clusters) will be kept forever by default.

```
[
	{
		// the name of your redshift cluster, minus the '<region>.redshift.amazonaws.com' suffix
		"clusterIdentifier": "my-redshift-cluster",
		// the region in which your cluster has been created
		"region": "us-east-1",
		// the frequency in hours that the snapshot manager should create snapshots
		"snapshotIntervalHours": 2,
		// the number of days after which the snapshots created by this process should be deleted
		"snapshotRetentionDays": 60
	}
	// you can create additional cluster configurations here
]
```

This will create a CloudWatch Events Schedule that runs your function every __15 Minutes__. This means that your snapshots will be taken around 15 minutes from the specified snapshots interval. If you require more frequent execution due to differing snapshot intervals, or to limit the duration between your snapshot interval and snapshot creation, then you can run:

```build.sh deploy <role-arn> schedule <N>```

Where N is the number of minutes between each schedule invocation (minimum 5 minutes).


### Confirm Execution

Once running, you will see that existing automatic snapshots, or new manual snapshots are created within the ```snapshotIntervalHours```. These snapshots are called ```redshift-utils-snapman-<yyyy>-<mm>-<dd>t<hh><mi><ss>```, and are tagged with ```createdBy=AWS Redshift Utils Snapshot Manager```, values which can be modified by updatin ```constants.json``` and redploying. __Only snapshots which are tagged using this scheme will be deleted by this utility - other snapshots are not affected.__ You can review the CloudWatch Log Streams for execution to see debug output about what the function is doing.

----

Amazon Redshift Utils Snapshot Manager

Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

	http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
