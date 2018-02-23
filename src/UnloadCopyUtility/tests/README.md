# Tests

## Cloudformation tests

The cloudformation directory contains an AWS Cloudformation stack (located in `RedshiftCFTemplate.json`) that will 
perform integration tests for the Unload Copy Utility which can verify that a git repository has code that is working 
as expected.  The stack takes numerous parameters which are documented inside the stack for most cases the default 
values will suffice.  The stack will spawn up 2 clusters one from snapshot and one empty cluster.  It will also create
 an EC2 instance which will be bootstrapped to be a client host for the Copy Unload utility and on which the different
tests can run.  This instance will run the different scenario's one by one and log the output.  From this log output a 
test report can be build which will show how the code behaves.  The scenarios are examples of how Unload Copy utility 
can be issued and can in that respect act as documentation.

### Pre-requisites

In order to run the Cloudformation stack you need a few requirements set up this section goes through them:

#### SourceClusterSnapshot

The parameter `SourceClusterSnapshot` should point to a snapshot that contains the tables that are used in the 
scenario's.  The file `bootstrap_source.sql` contains SQL on how to create such a cluster starting from an empty
cluster (you will need to replace `<arn-of-your-copy-role>` with the ARN of a role that is associated with your 
cluster and which is allowed to use S3).

#### ReportBucket

The parameter `ReportBucket` should be the name of a bucket that is in the same region as where you will spawn the 
stack.  After all the tests have run the script on the EC2 instance will push log files to this bucket.  In order for 
this to work the user that will spawn up the stack needs to be able to create an IAM role that allows access to this 
bucket.

### How to run

The CloudFormation template can be used as is but using run_stack.py is likely the easiest way.  It takes a few 
 parameters to have different behavior:
 
| Parameter-name     | Description |
| ------------------ | ----------- |
| --stack-name       | Resulting stack would be named `unload-copy-<stack_name>` if provided else just `unload-copy` |
| --ssh-key          | Path to PEM key (private key) that corresponds to the `SSHKeyForEc2` provided in the cloudformation template.  This is used to check on the instance whether all tests have completed. |
| --(no-)auto-delete | Whether after running tests the stack should be attempted to be deleted automatically.|
| --(no-)create      | Whether this run should create the stack or not |
| --await-tests      | If you have `--no-auto-delete` but you do want to only stop `run_stack.py` once tests have completed |
| --debug            | Verbose logging of `run_stack.py` |

So for example if you want to just run the test and check logs afterwards from S3:
```
python run_stack.py --auto-delete --ssh-key /path/to/ssh/key.pem
```

As dependencies for cloudformation tests you can just limit to `requirements-cloudformation-only.txt`.


## Other tests
Other files ending in tests.py will contain tests that are Python implemented using the TestCase class of the unittest 
library.  Unittest will require Python 3 to run.

| Filename                                 | Description |
| ---------------------------------------- | ----------- |
| global_config_unittests.py               | Unittests to validate the functionality of global_config |
| redshift_unload_copy_regression_tests.py | Tests to follow up with behavior while changing internals to more classes |
| redshift_unload_copy_unittests.py        | Group of simple unittests that do not have their own group |
| redshift_unload_copy_cluster_tests       | Tests that run against a real cluster.  When running these an environment variable `TEST_CLUSTER` needs to be set with the FQN hostname of a test cluster.  Then the first `.pgpass` entry that matches this hostname will be used to connect to the test cluster.|
| stack_parameters_builder_unittests       | Unittests for the helper app to run cloudformation tests.|