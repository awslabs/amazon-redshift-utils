# UserLastLogin utility
This utility helps in tracking cluster's users last login information. For customers having PCI-DSS compliance requirements to track inactive users can consider using this utility to identify inactive Redshift users and take necessary actions. 

## Usage
```python user_last_login.py --cluster <cluster dns end point> --dbPort <port> --dbName <database> --dbUser <dbuser>``` 

## Working details and Limitations 
1. The Python script uses IAM DB Authentication to connect to the cluster and eliminates the . So, ensure that the host has either profile configured with IAM user or AWS Resource has suitable role with the required [IAM policy](https://docs.aws.amazon.com/redshift/latest/mgmt/generating-iam-credentials-role-permissions.html) attached to execute get_cluster_credentials API call. 
2. The script extracts region of the cluster from DNS End point to generate the tempororary credentials
3. **dbUser** specified with this utility should be a cluster user that is already created, the script doesn't AutoCreate the user. 

# License
Amazon Redshift Utils - UserLastLogin

Copyright 2017-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Amazon Software License: https://aws.amazon.com/asl
