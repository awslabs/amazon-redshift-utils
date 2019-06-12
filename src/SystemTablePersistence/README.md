
# Amazon Redshift System Object Persistence Utility
Amazon Redshift, like most databases, contains monitoring and diagnostic information in the form of internal tables and views that can be queried to understand system behaviour better. Redshift system tables and views, numbering over 100, have a system-controlled retention that is variable but tends to be less than a week for common Redshift use-cases.

This [link](https://docs.aws.amazon.com/redshift/latest/dg/c_types-of-system-tables-and-views.html) outlines the most important tables and views for diagnostic performance information.

* The **stl_** prefix denotes system table logs. stl_ tables contain logs about operations that happened on the cluster in the past few days.
* The **stv_** prefix denotes system table snapshots. stv_ tables contain a snapshot of the current state of the cluster.
* The **svl_** prefix denotes system view logs. svl_ views join some number of system tables to provide more descriptive info.
* The **svv_** prefix denotes system view snapshots. Like the svl_ views, the svv_ views join some system tables to provide more descriptive info.

To persist the tables for a longer amount of time, this project provides an example implementation to create, populate, and use five of the most common objects that we see requiring long term retention. This mix of tables and views will highlight some of the edge cases users will encounter when applying tuning techniques techniques to their own list of tables.

## Deploying

The [Redshift Automation project](https://github.com/awslabs/amazon-redshift-utils/tree/master/src/RedshiftAutomation) can be used to host and run this utility, plus others including table analysis and vacuum, and can be setup with a one-click deployment to AWS Lambda. 

We have provided the following AWS SAM templates so that you can deploy this function as stand-alone, without the other functions from the wider RedshiftAutomation modules (please note that we currently only support deploying into VPC):

| Region | Template |
| ------ | ---------- |
|ap-northeast-1 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-1#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3-ap-northeast-1.amazonaws.com/awslabs-code-ap-northeast-1/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |
|ap-northeast-2 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-2#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3-ap-northeast-2.amazonaws.com/awslabs-code-ap-northeast-2/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |
|ap-south-1 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=ap-south-1#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3-ap-south-1.amazonaws.com/awslabs-code-ap-south-1/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |
|ap-southeast-1 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=ap-southeast-1#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3-ap-southeast-1.amazonaws.com/awslabs-code-ap-southeast-1/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |
|ap-southeast-2 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=ap-southeast-2#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3-ap-southeast-2.amazonaws.com/awslabs-code-ap-southeast-2/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |
|ca-central-1 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=ca-central-1#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3-ca-central-1.amazonaws.com/awslabs-code-ca-central-1/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |
|eu-central-1 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=eu-central-1#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3-eu-central-1.amazonaws.com/awslabs-code-eu-central-1/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |
|eu-west-1 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=eu-west-1#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3-eu-west-1.amazonaws.com/awslabs-code-eu-west-1/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |
|eu-west-2 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=eu-west-2#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3-eu-west-2.amazonaws.com/awslabs-code-eu-west-2/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |
|sa-east-1 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=sa-east-1#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3-sa-east-1.amazonaws.com/awslabs-code-sa-east-1/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |
|us-east-1 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3.amazonaws.com/awslabs-code-us-east-1/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |
|us-east-2 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=us-east-2#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3-us-east-2.amazonaws.com/awslabs-code-us-east-2/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |
|us-west-1 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=us-west-1#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3-us-west-1.amazonaws.com/awslabs-code-us-west-1/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |
|us-west-2 |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/new?stackName=RedshiftAutomationSystemTablePersistence&templateURL=https://s3-us-west-2.amazonaws.com/awslabs-code-us-west-2/LambdaRedshiftRunner/deploy-systable-standalone.yaml) |

We’ve also run across some customers who already have an EC2 host for cron/scheduling related activities. If you wish to use ec2 or other runners with cron, then the Redshift Automation command line provides an option to run this application:

```
./ra --utility SystemTablePersistence --config s3://mybucket/prefix/config.json
```

## Manual Setup Actions (optional if you are using the above Lambda function): ##

### Creating the HISTORY Schema: ###

Creating a separate schema is a convenient way to sequester the history tables and views away from other objects. Any preferred schema name can be used; a case-insensitive search-and-replace of "HISTORY" will correctly update all the schema references in the included SQL.

```
CREATE SCHEMA IF NOT EXISTS history;
```

### Creating the History Tables: ###

The persisted data will be stored in direct-attached storage tables in Redshift. For the tables, the `CREATE TABLE {tablename} LIKE` technique is an easy way to inherit the column names, datatypes, encoding, and table distribution from system tables. For system views, an Internet search on the view name (for example: `SVL_QUERY_SUMMARY`) will direct the user to the Redshift documentation which includes the column-level description. This table will frequently copy-and-paste well into a spreadsheet program, allow for easy extraction of the column name and data type information.

History tables are created in the `HISTORY` schema, and will be verified on each run of the table persistence system. You can view the table creation statements in [`lib/history_table_creation.sql`](lib/history_table_creation.sql).


### Creating the Views to Join the Historical and Current Information: ###

The views return data from both the current system objects and the historical tables. The anti-join pattern is used to accomplish the deduplication of rows.

```
CREATE OR REPLACE VIEW history.all_stl_load_errors AS
(
SELECT le.*  FROM stl_load_errors le
UNION ALL
SELECT h.* FROM stl_load_errors le
RIGHT OUTER JOIN history.hist_stl_load_errors h ON (le.query = h.query AND le.starttime = h.starttime)
WHERE le.query IS NULL
);
 
CREATE OR REPLACE VIEW history.all_stl_query AS
(
SELECT q.* FROM stl_query q
UNION ALL
SELECT h.* FROM stl_query q
RIGHT OUTER JOIN history.hist_stl_query h ON (q.query = h.query AND q.starttime = h.starttime)
WHERE q.query IS NULL
);
 
CREATE OR REPLACE VIEW history.all_stl_wlm_query AS
(
SELECT wq.* FROM stl_wlm_query wq
UNION ALL
SELECT h.* FROM stl_wlm_query wq
RIGHT OUTER JOIN history.hist_stl_wlm_query h ON (wq.query = h.query AND wq.service_class_start_time = h.service_class_start_time)
WHERE wq.query IS NULL
);
 
CREATE OR REPLACE VIEW history.all_stl_explain AS
(
SELECT e.* FROM stl_explain e
UNION ALL
SELECT h.* FROM stl_explain e
RIGHT OUTER JOIN history.hist_stl_explain h ON (e.query = h.query AND e.userid = h.userid AND e.nodeid = h.nodeid AND e.parentid = h.parentid AND e.plannode = h.plannode)
WHERE e.query IS NULL
);
 
CREATE OR REPLACE VIEW history.all_svl_query_summary AS
(
SELECT qs.* FROM svl_query_summary qs
UNION ALL
SELECT h.* FROM svl_query_summary qs
RIGHT OUTER JOIN history.hist_svl_query_summary h ON (qs.query = h.query AND qs.userid = h.userid AND qs.stm = h.stm AND qs.seg = h.seg AND qs.step = h.step AND qs.maxtime = h.maxtime AND qs.label = h.label)
WHERE qs.query IS NULL
);
```

## Populating the History Tables: ##

This can be done Daily or on a User-Selected Frequency (we recommend populating the history tables daily).

This utility will insert only the new rows using the model as described above, with an anti-join:

```
INSERT INTO history.hist_stl_load_errors (
  SELECT le.* FROM stl_load_errors le, (SELECT NVL(MAX(starttime),'01/01/1902'::TIMESTAMP) AS max_starttime FROM history.hist_stl_load_errors) h WHERE le.starttime > h.max_starttime);
```

You can view the statements that will be run by the utility in [`lib/history_table_config.json`](lib/history_table_config.json);


### Querying the Views in the History Schema: ###
The history schema views can be queried in exactly the same way that users have interacted with the existing system objects.

```
SELECT * FROM history.all_stl_load_errors WHERE UPPER(err_reason) LIKE '%DELIMITER NOT FOUND%';
SELECT * FROM history.all_stl_query WHERE query = 1121;
SELECT COUNT(*) FROM history.all_stl_wlm_query WHERE service_class = 6;
SELECT * FROM history.all_stl_explain WHERE query = 1121 ORDER BY nodeid;
SELECT * FROM history.all_svl_query_summary WHERE bytes > 1000000;
```

## Long term archival of System Table data

If required, this utility can archive data from the internal HISTORY tables to Amazon S3. Add the following configuration values to your configuration file:

```
"s3_unload_location":"s3://mybucket/prefix/redshift/systables/archive"
"s3_unload_role_arn":"arn:aws:iam::<acct>:role/<role name>"
```

Please note that the `s3_unload_role_arn` should be linked to the Redshift cluster to enable S3 access [as outlined here](https://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html).

The structure of the exported data will make it suitable for querying via the AWS Glue Data Catalog. Both the cluster-name and the datetime of data export will be configured as partitions. The structure of the data in the exported location will be:

* `Configured output location`
	* `<table name>` - such as `hist_svl_query_summary`, `hist_stl_explain`
		* `cluster=<your cluster name>`
			* `datetime=<exported date time information>`

For example: 
![image](exported_s3_structure.png)

All exported data is quoted, compressed csv data.

## Other Considerations: ##
As with any table in Redshift, it's a best practice to analyze (even just a handful of columns) on a weekly basis. This will help inform the query planner of the attributes of the table. Users may also want to enhance the performance of the history views by adding sort keys to the underlying history tables. It is recommended to consider columns used in the filter condition on the associated view for good sort key candidates.


