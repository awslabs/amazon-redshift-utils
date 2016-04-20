# AWS Lambda Utility Runner

This project includes code that is able to run a subset of the Amazon Redshift Utilities via AWS Lambda. By using a Lambda function scheduled via a CloudWatch Event (http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/WhatIsCloudWatchEvents.html), you can ensure that these valuable utilities run automatically and keep your Redshift cluster running well.

## Supported Utilities

Currently only the [Column Encoding Utility](src/ColumnEncodingUtility) is supported, but in time we will also hope to support the [Analyze/Vacuum Utility](src/AnalyzeVacuumUtility).

## Setup

Because these utilities need to access your Redshift cluster, they require a username and password for authentication. This function reads these values from a configuration file, and expects that the database password is a base64 encoded string that has been encrypted by [AWS KMS](https://aws.amazon.com/kms). In order to authenticate when the utility is run by AWS Lambda, the IAM role granted to AWS Lambda must have rights to decrypt data using KMS, and must also include this application's internal encryption context (which you may change if you desire).

To encrypt your password for use by this function, please run the [encrypt_password.py](encrypt_password.py) script, and type your password as the first argument:

```
> export AWS_REGION=my-aws-region
> (python) encrypt_password.py MY_SECRET_PASSWORD
```

This will create the required Customer Master Key in KMS, along with a key alias that makes it easy to work with (called ```alias/RedshiftUtilsLambdaRunner```) and so must be run as a user with rights to do so. It will then encrypt the supplied password and output the encrypted ciphertext as a base64 encoded string:

```
$ ./encrypt_password.py MY_SECRET_PASSWORD
Encryption Complete
Encrypted Password: CiChAYm4goRPj1CuMlY+VbyChti8kHVW4kxInA+keC7gPxKZAQEBAgB4oQGJuIKET49QrjJWPlW8gobYvJB1VuJMSJwPpHgu4D8AAABwMG4GCSqGSIb3DQEHBqBhMF8CAQAwWgYJKoZIhvcNAQcBMB4GCWCGSAFlAwQBLjARBAwdVzuq29SCuPKlh9ACARCALY1H/Tb4Hw73yqLyL+Unjp4NC0F5UjETNUGPtM+DTHG8urILNTKvdv1t9S5zuQ==
```

Copy the value after ```Encrypted Password: ``` an use it for the creation of the config file

## Configuration

This lambda function uses an included ```config.json``` file to get information about which cluster to connect to, which utilities to run, and other information it needs to accomplish its task. You configure which utility to run in the ```'utilities'``` array - currently only the value ```ColumnEncodingUtility``` is supported

The required configuration items are placed into the ```configuration``` part of the config file, and include:

```
{
  "analyzeTable": "The name of a specific table to analyze",
  "analyzeSchema": "The name of the schema to be analyzed. Either analyzeSchema or analyzeTable must be provided",
  "comprows": Int - set to -1 for default. This is the number of rows that will be considered per slice for Column Encoding,
  "db": "master",
  "dbHost": "Hostname of the Redshift Cluster master node to connect to",
  "dbPassword": "base64 encoded password obtained by running encrypt_password.py",
  "dbPort": Int - the database port,
  "dbUser": "The Database Username to connect to",
  "dropOldData": Boolean - should the current version of the table be dropped after operations complete?,
  "ignoreErrors": Boolean - should the system keep running even if errors are encountered?,
  "queryGroup": "The database query group setting to use for WLM",
  "querySlotCount": Int - the number of WLM slots to request for the given queryGroup,
  "targetSchema": "The schema name where newly encoded tables should be placed. Leave as "" for using the analyzeSchema",
  "force": Boolean - do you want to force the utility to run even if there are no likely changes?,
  "outputFile":"The path of the file to create on the filesystem. AWS Lambda can only write to /tmp",
  "debug":Boolean - turn on debug logging of actions and SQL run on the cluster
  }
```

## Building

This module runs as an AWS Lambda function, and imports the required utilities from other parts of this GitHub project as required. It also imports its required dependencies and your ```config.json``` and builds a zipfile that is suitable for use by AWS Lambda. To build this module after customising your config file, just run ```build.sh```. This will result in zipfile ```lambda-redshift-util-runner.zip``` being created in the root of the ```LambdaRunner``` project. You can then deploy this zip file to AWS Lambda , but be sure to set your runtime language to 'python', and the timeout to a value long enough to accomodate running against all your tables.

Also, because this function connects to a specific Redshift cluster with a specific configuration of username and password, we encourate you to use a Function Name that will be easy to understand which instance you have deployed. For instance, you might name it ```RedshiftUtilitiesMyClusterMyUser```

## Running the Modules

These utilites are designed to be run via a schedule, and don't use any information from the incoming event. Given this, you can just press the 'Test' button on the AWS Lambda console to run the function and use CloudWatch Logs to determine that you are happy with the configuration, the level of debug output, and the timeout.

Once done, you can create a CloudWatch Events Event Source that will run your function on the required schedule. To do this, open the function in the AWS Lambda web console, and then select the 'Event Sources' tab. ```Add event source``` and then select Type = ```CloudWatch Events - Schedule ```. Select a rule name, and then enter a schedule expression that will cause your function to run when required. Enable it, and you are ready to go. You can review the function running over time using CloudWatch Logs for the function from the Monitoring tab.
