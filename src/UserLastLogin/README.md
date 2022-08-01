# UserLastLogin utility
This utility tracks the last login time of all the database users on a Redshift cluster. The timestamp of each users last login is recorded in a table on the cluster. The script is compatible with Python 3, and uses the AWS redshift_connector.

For customers with PCI-DSS compliance requirements to track inactive users, this utility can help to identify inactive Redshift users and provide the information necessary for the customer to take action. 

## Usage

Execute the script from the command line:
 
```
python3 user_last_login.py --cluster <cluster dns end point> --dbPort <port> --dbName <database> --dbUser <dbuser>
``` 


## Results

The results are stored in a table on the cluster. The schema and table are created if they do not already exist, or otherwise are truncated and repopulated on each execution of the script. View the results of the run by executing the following SQL query on the cluster:

```
Select * from history.user_last_login;
```

## Working Details and Limitations 
1. The Python script uses IAM DB Authentication to connect to the cluster. Ensure that the host has either a profile configured with IAM user or AWS Resource has a suitable role with the required [IAM policy](https://docs.aws.amazon.com/redshift/latest/mgmt/generating-iam-credentials-role-permissions.html) attached to execute get_cluster_credentials API call. 
2. The script extracts region of the cluster from DNS End point to generate the tempororary credentials
3. **dbUser** specified with this utility should be a cluster user that is already created, the script doesn't AutoCreate the user. 

# License
Amazon Redshift Utils - UserLastLogin

Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

This project is licensed under the Apache-2.0 License.
