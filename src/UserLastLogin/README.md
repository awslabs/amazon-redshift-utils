# UserLastLogin utility
This utility tracks the last login time of all the database users on a Redshift cluster or endpoint. The timestamp of each users last login is recorded in a table on the cluster. The script is compatible with Python 3.

For customers with PCI-DSS compliance requirements to track inactive users, this utility can help to identify inactive Redshift users and provide the information necessary for the customer to take action. 

## Usage

Execute the script from the command line using one of two methods. For a provisioned cluster specify the `cluster` and the `dbUser`:
 
```
python3 user_last_login.py --cluster <cluster name> --dbPort <port> --dbName <database> --dbUser <dbuser> --region <aws_region>
``` 

Note that the cluster name is **NOT** the full endpoint, but only the cluster name

For a serverless cluster specify the `workgroup`:

```
python3 user_last_login.py --workgroup <workgroup name> --dbPort <port> --dbName <database> --region <aws_region>
```

Note that the `region` parameter is always required for both provisioned and serverless. Serverless does not require a database username to be specified

## Results

The results are stored in a table on the cluster. The schema and table are created if they do not already exist, or otherwise are truncated and repopulated on each execution of the script. View the results of the run by executing the following SQL query on the cluster:

```
Select * from history.user_last_login;
```

## Working Details and Limitations 
1. The Python script uses the Redshift Data API to execute all the commands. No database drivers are required
2. The script extracts region of the cluster from DNS End point to generate the tempororary credentials
3. **dbUser** specified for the provisioned cluster connection with this utility should be a cluster user that is already created, the script doesn't AutoCreate the user. 

# License
Amazon Redshift Utils - UserLastLogin

Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

This project is licensed under the Apache-2.0 License.
