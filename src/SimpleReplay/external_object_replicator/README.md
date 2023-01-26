# External Object Replicator README

## Introduction
External Object Replicator replicates COPY commands objects, Copy manifest objects, and Spectrum 
objects in source cluster to the target s3 location.

External object replicator clones/replicates the following objects to target s3 location:
1. COPY objects accessed by the source cluster when user runs COPY command. 
   - For example, when `copy category
   from 's3://mybucket/custdata' ` is run on source cluster, `s3://mybucket/custdata` will be cloned to target location by external object replicator.
2. COPY manifest files accessed by source cluster when user uses a manifest files to load multiple files
   - Files specified in a manifest file will all be replicated to target s3 location
3. Spectrum objects queried by source cluster
   - Spectrum files will be replicated in the targe s3 location
   - Spectrum objects(tables and schemas) will be replicated in a new cloned Glue database

The utility is currently only supported on provisioned Redshift. We plan to increase the functionality to support 
serverless endpoint in a future release.

## Preparation

### External Object Replicator setup

1. Create an EC2 instance
    1. Recommended EC2 instance type: **m5.8xlarge**, 32GB of SSD storage, Amazon Linux AMI
    2. The cluster must be accessible from where External object replicator is being run. This may entail modifying the security group inbound rules or running Simple Replay on the same VPC as the Redshift replica cluster. 
2. Install Simple Replay and libraries dependencies on the provided EC2 machine

In the newly created EC2 machine:

2.1 Install Python3

```
sudo yum install python3

sudo yum install python3-pip
```

2.2 Install ODBC dependencies

```
sudo yum install gcc gcc-c++ python3 python3-devel unixODBC unixODBC-devel
```

2.3 Clone Simple Replay scripts

   ```
   git clone https://github.com/awslabs/amazon-redshift-utils.git
   ```

2.4 Install Python libraries 

If you have executed Simple Replay and installed packages in requirements.txt, skip this step and go to 2.5

If you are running External Object Replicator on its own, and you have not executed Simple Replay before, do the following:
Navigate to Simple Replay root directory and run the following command:

```
sudo pip3 install -r requirements.txt
```

2.5 Install ODBC Driver for Linux

Follow the steps provided by the documentation and install ODBC Driver for Linux
https://docs.aws.amazon.com/redshift/latest/mgmt/configure-odbc-connection.html

2.6 AWS CLI

Check if AWS CLI is configured in the machine. If it’s not configured, follow the steps in [installation guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)

2.7 Configure AWS CLI

```
aws configure
```
            * Provided IAM user should have Redshift and S3 permissions. If temporary IAM credentials are being used, ensure they do not expire before the external replicator ends.
            * The IAM user needs to have permission to read S3 buckets where COPY and Spectrum objects are stored.
            * The IAM user needs to have permission to write into S3 bucket of the provided target_s3_location in external_replicato.yaml.


## Running External Object Replicator

* External Object Replicator currently only supports Redshift Provisioned cluster
* External Object Replicator will replicate any files copied using the COPY command or a MANIFEST file, and Spectrum tables queried within the starttime and endtime provided in the external_replicator.yaml (See below).
* The source cluster should be accessible from wherever External Object Replicator is being run. This may entail modifying the security group inbound rules to include “My IP”, or running Simple Replay on an EC2 instance in the same VPC.

### Configuration file parameters for `extraction.yaml` :

| Configuration value          | Required?   | Details                                                                                                                                                                                                       | Example                                                                                      |
|------------------------------|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| source_cluster_endpoint      | Required    | Redshift source cluster endpoint                                                                                                                                                                              | "<redshift-cluster-name>.<identifier>.<region>.redshift.amazonaws.com:<port>\<databasename>" |
| region                       | Required    | Region of the source cluster                                                                                                                                                                                  | "us-east-1"                                                                                  |
| redshift_user                | Required    | Username to access the cluster and database                                                                                                                                                                   | "awsuser"                                                                                    |
| start_time                   | Required    | Start time of the workload to be replicated. [Default timezone is UTC, if timezone is not given in end_time then the value of end_time will be converted to UTC timezone based on Machine's current timezone] | “2020-07-26T21:45:00+00:00”                                                                  |
| end_time                     | Required    | Start time of the workload to be replicated. [Default timezone is UTC, if timezone is not given in end_time then the value of end_time will be converted to UTC timezone based on Machine's current timezone] | “2020-07-27T21:45:00+00:00”                                                                  |
| target_s3_location           | Required    | A S3 bucket location where you want the replicator to store cloned objects                                                                                                                                    | "s3://mybucket/myworkload"                                                                   |
| log_level                    | Required    | Specify desired log level - you have the option of INFO and DEBUG                                                                                                                                             | "DEBUG"                                                                                      |

### Command

Edit 
Once the above configuration parameters are set in external_replicator.yaml, the external replicator can be run using the following command

```
python3 external_replicator.py external_replicator.yaml
```

### Output

External Replicator produces the following outputs:
  
In the target_S3_location provided in external_replicator.yaml:
* COPY objects in copyfiles/
  * COPY objects cloned by the external replicator are all located within the directory of copyfiles/
* Final_Copy_Objects.csv in copyfiles/
  * A CSV file containing details of COPY objects cloned
* Spectrum files in Spectrumfiles/ 
  * Spectrum files cloned by the external replicator are all located within the directory of Spectrumfiles/
* Spectrum_objects_copy_report.csv in Spectrumfiles/ 
  *  A CSV file containing details of Spectrum objects cloned

In the local directory:
* external_replicator.log
    * Logs produced by the execution of external replicator. 

