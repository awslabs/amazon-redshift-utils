Use the role with full admin access to run the script.

Dependencies:

1. Download Python3.

2. Make sure you have the following packages:
    1. boto3
    2. botocore
    3. awscli
    4. requests
            If not, install them using pip3,

            # pip3 install boto3
            # pip3 install botocore
            # pip3 install awscli
            # pip3 install requests

3. Configure aws credentials using <aws configure> in terminal. Make sure you enter the region where your redshift cluster is.


Running the script:

Download the Ondemand_Redshift_Final.zip file. (from redshift utilities)
1. Only extract the Ondemand_Redshift_Final.zip file in a folder.
2. Run the script Ondemand_Lamb.py file using python3.
# python3 Ondemand_Lamb.py

Note:
1. The code will not work if there are already more than 20 manual snapshots present in that region.
2. Do not delete the following resources created by the code, they are required to create and delete cluster everyday at the time provided by you:
1. Lambda function: Redshift-ondemand-function
2. Cloudwatch rules: Ondemand-createCluster-rule   AND  Ondemand-deleteCluster-rule
3. Policy: Ondemand_CW_log_policy 
