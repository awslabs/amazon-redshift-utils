import boto3
import os
import json
import awscli.customizations.datapipeline.translator as trans
import getpass
import botocore
from botocore.exceptions import ClientError
import sys
import re
from boto3.s3.transfer import S3Transfer
# function to replace the parameters in the template file
def replace_words(base_text, device_values):
    for key, val in device_values.items():
        base_text = base_text.replace(key, val)
    return base_text

def delete_cluster_script(password,ep,username,dbname,s3,rc,region):
    path="/home/ec2-user/output"
    s3l="s3://"+s3+"/"
    f = open( 'dd.sh', 'w' )
    f.write("sudo yum install postgresql -y \n")
    f.write("PGPASSWORD="+password+" psql -h "+ep+" -U "+username+" -p 5439 -d "+dbname+" -c \"select count(*) from pg_tables;\">"+path+"\n")
    f.write("aws s3 cp "+path+" "+s3l+"\n")
    f.write("name=\""+rc+"\"`date +'%Y-%m-%d'`"+"\n")
    f.write("aws redshift delete-cluster --cluster-identifier "+rc+" --final-cluster-snapshot-identifier $name --region "+region+"\n")
    f.close()    
    client = boto3.client('s3')
    transfer = S3Transfer(client)
    transfer.upload_file("dd.sh",s3,"dd.sh")

def restore_cluster_script(password,ep,username,dbname,s3,rc,region,arn,pg,sgr,iam):
    path="/usr/bin/jq"
    path1="/home/ec2-user/output1"
    hd="/home/ec2-user/"
    msgl="file:///home/ec2-user/message"
    s3l="s3://"+s3+"/output"
    f = open( 'cc.sh', 'w' )
    f.write("sudo yum install jq -y \n")
    f.write("LASTSNAPSHOTTIME=`aws redshift describe-cluster-snapshots --cluster-identifier "+rc+" --region "+region+"|"+path+" '.Snapshots[] |select(.SnapshotType == \"manual\")| .SnapshotCreateTime' | tail -1`\n")
    f.write("LASTSNAPSHOT=`aws redshift describe-cluster-snapshots --cluster-identifier "+rc+" --region "+region+"| "+path+" '.Snapshots[]  | select(.SnapshotCreateTime == '$LASTSNAPSHOTTIME') |.SnapshotIdentifier' | tr -d '\"'`\n")
    f.write("a=`aws redshift restore-from-cluster-snapshot --cluster-identifier "+rc+" --snapshot-identifier $LASTSNAPSHOT --region "+region+" --cluster-parameter-group-name "+pg+" --vpc-security-group-ids "+sgr+" --iam-roles "+iam+"`\n")
    f.write("sleep 10\n")
    f.write("cs=`aws redshift describe-clusters --cluster-identifier "+rc+" --region "+region+"`\n")
    f.write("while [[ $cs != *\"available\"* ]]\n")
    f.write("do\n")
    f.write("\tsleep 1\n")
    f.write("\tcs=`aws redshift describe-clusters --cluster-identifier "+rc+" --region "+region+"`\n")
    f.write("done\n")
    f.write("sleep 120\n")
    f.write("sudo yum install postgresql -y \n")
    f.write("PGPASSWORD="+password+" psql -h "+ep+" -U "+username+" -p 5439 -d "+dbname+" -c \"select count(*) from pg_tables;\">"+path1+"\n")
    f.write("aws s3 cp "+s3l+" "+hd+"\n")
    f.write("diff output1 output\n")
    f.write("echo \"Output before deletion\">"+hd+"t1\n")
    f.write("echo \"Output after deletion\">"+hd+"/t2\n")
    f.write("cat "+hd+"t1 "+hd+"output "+hd+"t2 "+hd+"output1>"+hd+"message\n")
    f.write("aws sns publish --topic-arn \""+arn+"\" --message "+msgl+" --region "+region+"\n")
    f.close()
    client = boto3.client('s3')
    transfer = S3Transfer(client)
    transfer.upload_file("cc.sh",s3,"cc.sh")

def pipelinecreation(s3bucket,sg,dsc,rsc,period,ami):
    s3n="s3://"+s3bucket
    device = {}
    device["scriptUri\": \"s3://xyz/cc.sh"] = "scriptUri\": \""+s3n+"/cc.sh"
    device["scriptUri\": \"s3://xyz/dd.sh"] = "scriptUri\": \""+s3n+"/dd.sh"
    device["securityGroupIds\": \"sg-XXXXXXXX"] = "securityGroupIds\": \""+sg  
    device["period\": \"1 Days"] = "period\": \""+period
    device["startDateTime\": \"2018-01-01T00:00:00"] = "startDateTime\": \""+rsc
    device["startDateTime\": \"2018-01-01T01:00:00"] = "startDateTime\": \""+dsc
    device["imageId\": \"ami-XXXXXXXX"] = "imageId\": \""+ami
    # Open your desired file as 't' and read the lines into string 'tempstr'
    t = open('latest.json', 'r')
    tempstr = t.read()
    t.close()
    output = replace_words(tempstr, device)
    # Write out the new config file
    fout = open('latest.json', 'w')
    fout.write(output)
    fout.close()
    definition = json.load(open('latest.json', 'r'))
    client = boto3.client('datapipeline')
    pipelineObjects = trans.definition_to_api_objects(definition)
    parameterObjects = trans.definition_to_api_parameters(definition)
    parameterValues = trans.definition_to_parameter_values(definition)
    pname=raw_input("\nEnter Pipeline Name")
    qid=raw_input("\nEnter Unique Id")
    desc=raw_input("\nEnter Pipeline description")
    response = client.create_pipeline(
        name=pname,
        uniqueId=qid,
        description=desc
    )
    print(response)
    pId= response['pipelineId']
    response1 = client.put_pipeline_definition(
        pipelineId=pId,
        pipelineObjects=pipelineObjects,
        parameterObjects=parameterObjects,
        parameterValues=parameterValues
    )
    response = client.activate_pipeline(
        pipelineId=pId)


# Prompt user to enter in the values
print("Please enter the following data")
s3_bucket=raw_input("Enter the bucket name for the creation and deletion scripts : ")
s3 = boto3.resource('s3')
response=s3.Bucket(s3_bucket) in s3.buckets.all()
if response == False:
    print "Bucket does not exist"
    s3_bucket=raw_input("Enter the bucket name for the creation and deletion scripts : ")
    s3 = boto3.resource('s3')
    response=s3.Bucket(s3_bucket) in s3.buckets.all()

s3_bucketl=raw_input("Enter the Logging bucket name : ")
response=s3.Bucket(s3_bucketl) in s3.buckets.all()
if response == False:
    print "Bucket does not exist"
    s3_bucketl=raw_input("Enter the Logging bucket name : ")
    s3 = boto3.resource('s3')
    response=s3.Bucket(s3_bucketl) in s3.buckets.all()

region=raw_input("Enter the region where the redshift cluster exists.Please ensure that the datapipeline service is available in the region: ")
dict = {'us-east-1': 'ami-21ffc85a', 'us-west-1': 'ami-f5664c95', 'us-west-2': 'ami-275dbe5f','eu-west-1':'ami-74d0230d','ap-southeast-1':'ami-58bb213b','ap-southeast-2':'ami-e4061e87','ap-northeast-1':'ami-ed897e8b','sa-east-1':'ami-dc7203b0','eu-central-1':'ami-6a78d105'}
ami=dict[region]

rscluster=raw_input("Enter the cluster name: ")
client = boto3.client('redshift',region)   
try:
    response = client.describe_clusters(ClusterIdentifier=rscluster)
    pg=response['Clusters'][0]['ClusterParameterGroups'][0]['ParameterGroupName']
    ep=response['Clusters'][0]['Endpoint']['Address']
    length=len(response['Clusters'][0]['VpcSecurityGroups'])
    sgr=" "
    if length != 0:
        temp=[]
        for i in range(length):
            temp.append(response['Clusters'][0]['VpcSecurityGroups'][i]['VpcSecurityGroupId'])
        sgr=' '.join(temp)
    length=len(response['Clusters'][0]['IamRoles'])
    iam=" "
    if length != 0:
        temp=[]
        for i in range(length):
            temp.append(response['Clusters'][0]['IamRoles'][i]['IamRoleArn'])
        iam=' '.join(temp)
        print iam
except ClientError as e:
    print("Cluster does not exist")
    sys.exit()
    
sg=raw_input("Enter security group that has access to redshift ")
ec2 = boto3.client('ec2')
try:
    response=ec2.describe_security_groups(GroupIds=[sg])
except ClientError as e:
    print("Security Group does not exist")
    sys.exit()
    
dsc=raw_input("Enter deletion schedule as YYYY-MM-DDTHH:MM:SS : ")
if re.match('^[0-9][0-9][0-9][0-9][\-][0-9][0-9][\-][0-9][0-9][T][0-9][0-9][:][0-9][0-9][:][0-9][0-9]$',dsc):
    print(" ")
else:
    print("Wrong format")
    sys.exit()    
rsc=raw_input("Enter restore schedule as YYYY-MM-DDTHH:MM:SS : ")
if re.match('^[0-9][0-9][0-9][0-9][\-][0-9][0-9][\-][0-9][0-9][T][0-9][0-9][:][0-9][0-9][:][0-9][0-9]$',rsc):
    print(" ")
else:
    print("Wrong format")
    sys.exit()
print rsc
print dsc
period=raw_input("Enter the period of schedule as 1 Hours/Days/Months/Years ")
username=raw_input("Enter username : ")
password=getpass.getpass("Enter Redshift Password :")
#region=raw_input("Enter the region where the redshift cluster exists: ")
dbname=raw_input("Enter the database name: ")

arn=raw_input("Enter SNS ARN: ")
sns = boto3.client('sns',region)
try:
    response = sns.get_topic_attributes(TopicArn=arn)
except ClientError as e:
    print("Topic does not exist")
    sys.exit()

delete_cluster_script(password,ep,username,dbname,s3_bucket,rscluster,region)
restore_cluster_script(password,ep,username,dbname,s3_bucket,rscluster,region,arn,pg,sgr,iam)
pipelinecreation(s3_bucket,sg,dsc,rsc,period,ami)
