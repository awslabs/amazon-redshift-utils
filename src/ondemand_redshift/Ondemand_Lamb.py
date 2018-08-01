import boto3
import os
import botocore
import awscli
import random
import time
from boto3.s3.transfer import S3Transfer
from botocore.exceptions import ClientError
import awscli.customizations.datapipeline.translator as trans
import requests
import json


cluster_region = input('Please enter the region in which Redshift cluster is present and make sure same region is set as default region in aws configure: ')
user_AccNo = boto3.client('sts').get_caller_identity()['Account']
random_no =  random.randint(100,999)
bucket_name = 'ondemand-redshift-'+str(user_AccNo)+str(random_no)+'-do-not-delete'


filename = 'Redshift-ondemand-function.py.zip'

#Creating S3 bucket
s3=boto3.client('s3')
if(cluster_region == 'us-east-1'):
    s3.create_bucket(Bucket=bucket_name, ACL ='private')
else:
    s3.create_bucket(Bucket=bucket_name, ACL ='private', CreateBucketConfiguration={
        'LocationConstraint': cluster_region})

#Uploading the file to the bucket
response = s3.list_buckets()
buckets = [bucket['Name'] for bucket in response['Buckets']]

flag=0
for i in range(0,len(buckets)):
    if(buckets[i] == bucket_name):
        flag=1
        s3.upload_file(filename, bucket_name, filename)
        
if(flag==0):
    print("Bucket could not be created")
    exit();


#Creating the role "ondemand-redshift-role-do-not-delete" with AWS trust relations
with open('trustRelation.json') as f:
    data = json.load(f)
role_data = json.dumps(data)
iam=boto3.client('iam')
try:
    iam.create_role(RoleName='ondemand-redshift-role-do-not-delete', AssumeRolePolicyDocument=role_data)
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityAlreadyExists':
        print ("Role already exists")
    else:
        print ("Unexpected error: %s") % e

#Creating policy role
with open('CloudWatch_log_policy.json') as f1:
    data1 = json.load(f1)
role_data1 = json.dumps(data1)
try:
    policy_arn = iam.create_policy(PolicyName='Ondemand_CW_log_policy',PolicyDocument=role_data1)
    policy_arn= policy_arn['Policy']['Arn']

    #Attaching pplicy to role "ondemand-redshift-role-do-not-delete"
    iam.attach_role_policy(RoleName='ondemand-redshift-role-do-not-delete',PolicyArn=policy_arn)

    #Attaching VPC access to the role for the lambda function to have acccess to the cluster
    iam.attach_role_policy(RoleName='ondemand-redshift-role-do-not-delete',PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole')
    iam.attach_role_policy(RoleName='ondemand-redshift-role-do-not-delete',PolicyArn='arn:aws:iam::aws:policy/CloudWatchEventsFullAccess')

except ClientError as e:
    if e.response['Error']['Code'] == 'EntityAlreadyExists':
        print('*** Policy already exists. Please go to IAM and get the policy ARN for policy: "Ondemand_CW_log_policy" ***')
        policy_arn = input('Please enter the Policy ARN : ')
    else:
        print ("Unexpected error: %s") % e


#Getting ARN of the role
response= iam.get_role(RoleName='ondemand-redshift-role-do-not-delete')
response= response['Role']
role_arn= response['Arn']


#Creating Lambda function
iam=boto3.client('iam')
lambda_client=boto3.client('lambda')

time.sleep(20)
try:
    lambda_client.create_function(FunctionName='Redshift-ondemand-function',Runtime='python3.6',Role=role_arn,Handler='Redshift-ondemand-function.lambda_handler',Code=dict(S3Bucket=bucket_name,S3Key=filename),Timeout=300)
except ClientError as e:
    if e.response['Error']['Code'] == 'ResourceConflictException':
        print('Function with name "Redshift-ondemand-function" Already Exist . So skipping its creation')
    else:
        print ("Unexpected error: %s") % e


#Creating Cloudwatch event
clusterName= input("Enter your cluster Name: ")
event = boto3.client('events')
lamb = boto3.client('lambda')

deleteTime = input('Enter the hour in UTC when you want to delete the cluster (eg: 10): ')
deleteMin = input('Enter the minute in UTC when you want to delete the cluster (eg: 10): ')
deleteCron = 'cron('+deleteMin + ' '+deleteTime + ' * * ? *)'

createTime = input('Enter the hour in UTC when you want to restore the cluster (eg: 10): ')
createMin = input('Enter the minute in UTC when you want to restore the cluster (eg: 10): ')


delete_rule_response= event.put_rule(Name='Ondemand-deleteCluster-rule',ScheduleExpression=deleteCron,State='ENABLED')
delete_rule_response1= delete_rule_response['RuleArn']
try:
    lamb.add_permission(FunctionName='Redshift-ondemand-function',StatementId='Ondemand_delete_redshift',Action='lambda:InvokeFunction',Principal='events.amazonaws.com',SourceArn=delete_rule_response1)
except ClientError as e:
    if e.response['Error']['Code'] == 'ResourceConflictException':
        print('Skipped adding permission to lambda . StatementId "Ondemand_delete_redshift" already exist')
    else:
        print ("Unexpected error: %s") % e
response_ARN=lamb.get_function(FunctionName='Redshift-ondemand-function')
response_ARN=(response_ARN['Configuration']['FunctionArn'])

event.put_targets(Rule='Ondemand-deleteCluster-rule',Targets=[
        {
            'Arn': response_ARN,
            'Id': 'MyCloudWatchEventsTargetDelete',
            'Input':json.dumps({'Cluster': clusterName, 'action': 'delete', 'creationHour': createTime , 'creationMin': createMin })
        }
    ]
)
print('Script ran successsfully.')
