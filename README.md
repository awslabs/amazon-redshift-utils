# Amazon Redshift Utilities

Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl/

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

# Unload/Copy Utility

The [Redshift Unload/Copy Utility](src/UnloadCopyUtility) helps you to migrate data between Redshift Clusters or Databases. It exports data from a source cluster to a location on S3, and all data is encrypted with Amazon Key Management Service. It then automatically imports the data into the configured Redshift Cluster, and will cleanup S3 if required. This utility is intended to be used as part of an ongoing scheduled activity, for instance run as part of a Data Pipeline Shell Activity (http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-shellcommandactivity.html).

# Lambda Runner

This [project](src/LambdaRunner) includes code that is able to run a subset of the Amazon Redshift Utilities via AWS Lambda. By using a Lambda function scheduled via a CloudWatch Event (http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/WhatIsCloudWatchEvents.html), you can ensure that these valuable utilities run automatically and keep your Redshift cluster running well.

# Snapshot Manager

This [project](src/SnapshotManager) includes a Lambda function that will ensure that your Redshift cluster is backed up as frequently as you require, and that the snapshots that it creates are cleaned up automatically when they are no longer needed.

# WLM Query Monitoring Rule (QMR) Action Notification Utility

This [project](src/QMRNotificationUtility) enables a scheduled Lambda function to pull records from the QMR action system log table (stl_wlm_rule_action) and publish them to an SNS topic. This utility can be used to send periodic notifications based on the WLM query monitoring rule actions taken for your unique workload and rules configuration.

# Presentation
We included a presentation which describes main features of the Amazon-Redshift-Utils including some examples, tips and best practices: Redshift_DBA_Commands.pptx

# Investigations
This project includes a number of detailed investigations into various types of Redshift edge cases, nuances, and workload scenarios. 

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

For convenience, you can create a `.env` file locally and upload them to the docker container via the `--env-file` argument. E.g.:

```bash
docker run --net host --rm -it --env-file .env .... amazon-redshift-utils analyze-vacuum
```

Please see the [entrypoint scripts](src/bin/) for the environment variable configuration references that are needed.

----
Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl/
