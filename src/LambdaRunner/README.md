# AWS Lambda Utility Runner

This project includes code that is able to run a subset of the Amazon Redshift Utilities via AWS Lambda. By using a Lambda function scheduled via a CloudWatch Event (http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/WhatIsCloudWatchEvents.html), you can ensure that these valuable utilities run automatically and keep your Redshift cluster running well.

## Supported Utilities

Currently the [Column Encoding Utility](src/ColumnEncodingUtility) and [Analyze/Vacuum Utility](src/AnalyzeVacuumUtility) are supported for automated invocation.

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

This lambda function uses a configuration file to get information about which cluster to connect to, which utilities to run, and other information it needs to accomplish its task. An example `config-example.json` is included to get you started. You configure which utility to run in the ```'utilities'``` array - currently the values ```ColumnEncodingUtility, AnalyzeVacuumUtility``` are supported

The required configuration items are placed into the ```configuration``` part of the config file, and include:

```
{"utilities":[("ColumnEncodingUtility"|"AnalyzeVacuumUtility")], - list of utilities to run. Can provide multiple values
"configuration": {
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
  "debug":Boolean - turn on debug logging of actions and SQL run on the cluster,
  "analyze_col_width": 1 - analyze colums of this size or larger during AnalyzeVaccum,
  "ssl-option":"" - whether ssl is required to connect to the cluster,
  "doVacuum": "(true|false) Should the Analyze Vacuum utility run Vacuum?",
  "doAnalyze":"(true|false) Should the Analyze Vacuum utility run Analyze?",
  "tableBlacklist":"comma separated list of tables to suppress running the analyze vacuum utility against"  
  }
}
```

You can include the file in the distribution as 'config.json', which will automatically be imported if it is found during the build phase. However, we do not recommend using this configuration mechanism, and instead recommend that you supply the configuration location as part of the CloudWatch scheduled event (see section "Running the Modules").

## Building

If you are using an S3 based configuration, then there should be no need to rebuild the project. You can just deploy from the `dist` directory, or use the following S3 locations (md5 checksum `49b2457b7760051598988b7872a7f1d0`):

| Region | Lambda Zip |
| ------ | ---------- |
| ap-northeast-1 | s3://awslabs-code-ap-northeast-1/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip || ap-northeast-2 | s3://awslabs-code-ap-northeast-2/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip || ap-south-1 | s3://awslabs-code-ap-south-1/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip || ap-southeast-1 | s3://awslabs-code-ap-southeast-1/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip || ap-southeast-2 | s3://awslabs-code-ap-southeast-2/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip || ca-central-1 | s3://awslabs-code-ca-central-1/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip || eu-central-1 | s3://awslabs-code-eu-central-1/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip || eu-west-1 | s3://awslabs-code-eu-west-1/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip || eu-west-2 | s3://awslabs-code-eu-west-2/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip || sa-east-1 | s3://awslabs-code-sa-east-1/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip || us-east-1 | s3://awslabs-code-us-east-1/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip || us-east-2 | s3://awslabs-code-us-east-2/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip || us-west-1 | s3://awslabs-code-us-west-1/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip || us-west-2 | s3://awslabs-code-us-west-2/LambdaRedshiftRunner/lambda-redshift-util-runner-1.1.zip |

If you do need to rebuild, this module imports the required utilities from other parts of this GitHub project as required. It also imports its required dependencies and your ```config.json``` and builds a zipfile that is suitable for use by AWS Lambda. To build this module after customising your config file or the code, just run ```build.sh```. This will result in zipfile ```lambda-redshift-util-runner-$version.zip``` being created in the root of the ```LambdaRunner``` project. You can then deploy this zip file to AWS Lambda , but be sure to set your runtime language to 'python(2.7|3.5)', and the timeout to a value long enough to accomodate running against all your tables.

Also, when you include a config.json, this function connects to only one Redshift cluster. If you do this, we encourate you to use a Lambda function name that will be easy to understand which instance you have pointed to. For instance, you might name it ```RedshiftUtilitiesMyClusterMyUser```.


## Running the Modules

These utilites are designed to be run via a schedule, and if you've included a local `config.json` file, then they don't extract any information from the incoming event. However, our recommendation is to upload your `config.json` to S3, and we'll use it as part of the CloudWatch Scheduled Event.

Next create a CloudWatch Events Schedule that will run your function on the required schedule. To do this, open the function in the AWS Lambda web console, and then select the 'Event Sources' tab. ```Add event source``` and then select Type = ```CloudWatch Events - Schedule ```. Select a rule name, and then enter a schedule expression that will cause your function to run when required. Under 'Targets', change the value for 'Configure Input' to 'Constant (JSON Text)', and add a value that runs the appropriate module, and points to the configuration file on S3. For example:

__To run the Column Encoding Utility__

```javascript
{"ExecuteUtility":"ColumnEncodingUtility","ConfigLocation":"s3//mybucket/myprefix/config.json"}
```

__To run the Analyze/Vacuum Utility__

```javascript
{"ExecuteUtility":"AnalyzeVacuumUtility","ConfigLocation":"s3//mybucket/myprefix/config.json"}
```

Now you can enable the Scheduled Event, and you are ready to go. You can review the function running over time using CloudWatch Logs for the function from the Monitoring tab.

----

Amazon Redshift Utils - Lambda Runner

Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Amazon Software License: https://aws.amazon.com/asl