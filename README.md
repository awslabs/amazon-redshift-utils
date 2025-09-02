# Amazon Redshift Utilities

Amazon Redshift is a fast, fully managed, petabyte-scale data warehouse solution 
that uses columnar storage to minimise IO, provide high data compression rates, 
and offer fast performance. This GitHub provides a collection of scripts and utilities
that will assist you in getting the best performance possible from Amazon Redshift.

# Admin Scripts

In the AdminScripts directory, you will find a [collection of utilities](src/AdminScripts) for running
diagnostics on your Cluster

# Admin Views

In the AdminViews directory, you will find a [collection of views](src/AdminViews) for managing
your Cluster, generating Schema DDL, and ...

# Stored Procedures

In the StoredProcedures directory, you will find a [collection of stored procedures](src/StoredProcedures) for managing
your Cluster or just to use as examples

# Column Encoding Utility

In order to get the best performance from your Redshift Database, you must ensure 
that database tables have the correct Column Encoding applied (http://docs.aws.amazon.com/redshift/latest/dg/t_Compressing_data_on_disk.html). 
Column Encoding specifies which algorithm is used to compress data within a column, and is chosen on the basis of the datatype, the unique number of discrete values in the column, and so on. When the COPY command (http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) is used to load data into a table, column encoding will be analyzed and applied by default. 
Other tables may be loaded via Extract/Load/Transform/Load (ELT) processes, and 
these tables may require having the column encoding updated at some point.

The [Redshift Column Encoding Utility](src/ColumnEncodingUtility) gives you the ability to apply optimal Column Encoding to an established Schema with data already loaded. When run, it will analyze an entire schema or individual tables. The ANALYZE COMPRESSION (http://docs.aws.amazon.com/redshift/latest/dg/r_ANALYZE_COMPRESSION.html) command is used to determine if any of the columns in the table require updating, and if so a script is generated to convert to the optimal structure.

# Analyze & Vacuum Utility

The [Redshift Analyze Vacuum Utility](src/AnalyzeVacuumUtility) gives you the ability to automate VACUUM and ANALYZE operations. 
When run, it will analyze or vacuum an entire schema or individual tables. This Utility Analyzes 
and Vacuums table(s) in a Redshift Database schema, based on certain parameters like unsorted, 
stats off and size of the table and system alerts from stl_explain & stl_alert_event_log. 
By turning on/off '--analyze-flag' and  '--vacuum-flag' parameters, you can run it as  'vacuum-only' 
or  'analyze-only' utility. This script can be scheduled to run VACUUM and ANALYZE as part of 
regular maintenance/housekeeping activities, when there are less database activities (quiet period).

# Cloud Data Warehousing Benchmark

The [Cloud DW Benchmark](src/CloudDataWarehouseBenchmark) consists of a set of workloads used to characterize and study the performance of Redshift running a variety of analytic queries.   The DDL to set up the databases, including COPY utility commands  to load the data from a public S3 directory,  as well as the queries for both single user and multi-user throughput testing are provided.

# Unload/Copy Utility

The [Redshift Unload/Copy Utility](src/UnloadCopyUtility) helps you to migrate data between Redshift Clusters or Databases. It exports data from a source cluster to a location on S3, and all data is encrypted with Amazon Key Management Service. It then automatically imports the data into the configured Redshift Cluster, and will cleanup S3 if required. This utility is intended to be used as part of an ongoing scheduled activity, for instance run as part of a Data Pipeline Shell Activity (http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-shellcommandactivity.html).

# Manifest Generator

Data loads into Amazon Redshift tables that have a Sort Key are resource intensive, requiring the system to consume large amounts of memory to sort data. For very large loads, you may observe the run times of `COPY` commands grow non-linearly relative to the data size. Instead of single large multi-TB operations, breaking down the load into chunks could yield faster overall ingestion.

This utility script is to generate Redshift manifest files to be used for `COPY` command to split the ingestion into batches.

# Simple Replay Utility

The [Simple Replay Utility](src/SimpleReplay) helps you to collect and replay cluster workloads. It reads the user activity log files (when audit is enabled) and generates sql files to be replayed. There are two replay tools. One that replays at a arbitrary concurrency and other that tries to reproduce the original cadence of work.


# Automation Module

This [project](src/RedshiftAutomation) includes code that is able to run the Amazon Redshift Utilities via AWS Lambda. By using a Lambda function scheduled via a CloudWatch Event (http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/WhatIsCloudWatchEvents.html), you can ensure that these valuable utilities run automatically and keep your Redshift cluster running well.

# Snapshot Manager

This [project](src/SnapshotManager) is now deprecated. The automatic capture and management of cluster snapshots is handled by AWS. Documentation is [available](https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-snapshots.html)

# WLM Query Monitoring Rule (QMR) Action Notification Utility

This [project](src/QMRNotificationUtility) enables a scheduled Lambda function to pull records from the QMR action system log table (stl_wlm_rule_action) and publish them to an SNS topic. This utility can be used to send periodic notifications based on the WLM query monitoring rule actions taken for your unique workload and rules configuration.

# Redshift to IDC migration utility
Some of the existing Redshift customers rely on local users, roles, groups, and permissions, which presents a significant challenge when adopting the SageMaker Lakehouse, Lakeformation, and other cross-service integration features. This [utility](src/RedshiftIDCMigrationUtility) streamlines the process by automatically creating IDC users, groups, and roles that mirror the existing Redshift configuration. It then assigns users to the appropriate groups and grants the necessary permissions to the IDC roles, ensuring a seamless transition to the centralized identity management system.  

# Redshift to Lake Formation migration utility

This [utility](src/LakeFormationMigrationUtility) streamlines the process of migrating data access permissions from Amazon Redshift to AWS Lake Formation through several key functions. It automatically extracts user permissions from both Redshift datashare and local databases by analyzing table-level grants. The utility then generates the necessary AWS CLI commands to establish equivalent permissions in Lake Formation supporting AWS Identity Center (IDC) authentication. Additionally, it creates rollback scripts that enable easy cleanup of permissions if needed.


# Investigations
This project includes a number of detailed investigations into various types of Redshift edge cases, nuances, and workload scenarios. 

# Authentication

You can provide a Redshift password as a base64 encoded KMS encrypted string in most tool configurations, or alternatively you can use `.pgpass` file or `$PGPASS` environment variable based authentication. In each module, or to package all of the modules for Lambda based automation, the use of `.pgpass`  will require that you rebuild the module using the `build.sh` script, but then should work as expected.

Please note that this feature was added due to requests by customers, but does not represent the most secure solution. It stores the password in plaintext, which depending on how modules are deployed may be a security threat. Please use with caution!

# Running utilities

From the command line, you can run the utilities from the `src` directory with:

```
python3 ./<folder>/<utility> <args>
```
# Docker executions
The Dockerfile provides an environment to execute the following utilities without having to install any dependencies locally:
* Analyze & Vacuum Utility
* Unload/Copy Utility
* Column Encoding Utility

You can do this by building the image like so:
```bash
docker build -t amazon-redshift-utils .
```

And then executing any one of the 3 following commands (filling in the -e parameters as needed):
```bash
docker run --net host --rm -it -e DB=my-database .... amazon-redshift-utils analyze-vacuum
docker run --net host --rm -it -e DB=my-database .... amazon-redshift-utils column-encoding
docker run --net host --rm -it -e CONFIG_FILE=s3://.... amazon-redshift-utils unload-copy
```

The docker [entrypoint scripts](src/bin/) work off of environment variables, so you'd want to provide those in your run scripts above.

For convenience, you can create a `.env` file locally and upload them to the docker container via the `--env-file` argument. For example if your environment variables file is named redshift_utils.env then you could execute with:

```bash
docker run --net host --rm -it --env-file redshift_utils.env .... amazon-redshift-utils analyze-vacuum

docker run --net host --rm -it --env-file redshift_utils.env -e CONFIG_FILE=s3://<bucket_name>/<config_file>.json amazon-redshift-utils unload-copy
```

Please see the [entrypoint scripts](src/bin/) for the environment variable configuration references that are needed.

----

## License

This project is licensed under the Apache-2.0 License.

