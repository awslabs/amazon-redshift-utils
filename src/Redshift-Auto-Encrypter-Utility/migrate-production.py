 #Script to migrate metadata from unecnrypted redshift cluster to encrypted redshift cluster
import os
import boto3
from pg import DB
from time import sleep
from random import *
from botocore.exceptions import ClientError
from botocore.exceptions import EndpointConnectionError
import subprocess
import urllib.request
import getpass
import sys
class Credentials:
    ename=''
    uname=''
    dname=''
    pwd=''
    port=5439
    region=''
    def readCreds(self):
      print('enter endpoint')
      self.ename = input().strip()
      print('enter database :')
      self.dname=input().strip()
      print('enter MasterUser name :')
      self.uname = input().strip()
      print('Enter admin password :')
      self.pwd = getpass.getpass().strip()
      print('Enter hosted port(default 5439) :')
      temp = input()
      if temp is '':
        self.port = 5439
      else:
        self.port=int(temp)
      #print("Please enter the region ex:(north virginia is us-east-1)\n (Please check correct region here :https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html )")
      
      
        
        
#     def readDetails(self):
#       print('Enter bucket name where you want to store and load your backup from:')
#       self.bucketname=input()
#       print('Enter role arn assigned to your redshift clusters and has S3-READ-WRITE-ACCESS:')
#       self.rolearn=input()


regions=['us-east-2','us-east-1','us-west-1','us-west-2','ap-northeast-1','ap-northeast-2','ap-northeast-3','ap-south-1','ap-southeast-1','ap-southeast-2','ca-central-1','cn-north-1','cn-northwest-1','eu-central-1','eu-west-1','eu-west-2','eu-west-3','sa-east-1']

def isSNSValid(x):
  valid = True
  try:
    region=x.split(':')[3]
    snsClient = boto3.client('sns',region)
    response = snsClient.get_topic_attributes(TopicArn=x)
  except IndexError:
    print("Topic does not exist with arn")
    sys.exit()
  except ClientError as e:
    print("Topic does not exist")
    valid = False
    sys.exit()
  except EndpointConnectionError as ece :
    print("Topic does not exist with arn")
    sys.exit()
  global snsRegion
  snsRegion=region
  return valid

def isEndpointValid(endpoint='',srn=''):
  valid = False;
  for region in regions:
    redshift = boto3.client('redshift',region)
    try:
      for clusters in redshift.describe_clusters()['Clusters']:
        if(clusters['Endpoint']['Address']==endpoint):
          srn=region
          valid=True;
          break;
    except ClientError as ce:
      continue
    if(valid):
      break;
  return valid

def isBucketValid(s3bucket=''):
  s3Client = boto3.client('s3')
  for bucket in s3Client.list_buckets()['Buckets']:
    if(bucket['Name']==s3bucket):
      return True;
  return False;
print("/n Dear user, Welcome to redshift migration process")
print('If you wish to recieve notifications of various stages in the process please enter an ARN of a sns topic below.(please leave it empty if you dont want any notifications)\nARN:')
snsArn=input().strip()
snsProvided=False

if(snsArn!='' and isSNSValid(x=snsArn)):
  snsProvided=True
  print(snsRegion)

if(snsProvided):
  snsClient = boto3.client('sns',snsRegion)
  snsClient.get_topic_attributes(TopicArn=snsArn)
  snsClient.publish(TopicArn=snsArn,Message="Dear User, You will now recieve notifications for migration process.")

print("\n\t\t\t\t\t\tPlease enter source details (unencrypted cluster) : ")
print("\t\t\t\t\t\t===============================================")
source = Credentials()
source.readCreds()

print("\n\t\t\t\t\t\tPlease enter Destination details (encrypted cluster) : ")
print("\t\t\t\t\t\t===============================================")
destination = Credentials()
destination.readCreds()
print("Validating Endpoints")
srv =isEndpointValid(endpoint=source.ename,srn=source.region)
dsv =isEndpointValid(endpoint=destination.ename,srn=destination.region)
if( srv and drv ):
  print("\nCommon Key\n--------------")
  print("\n please enter a common password to assign for all the users intially in Encrypted cluster as we cannoot migrate passwords")
  print("\nNote:\n=====\nShould atleast contain 8 ascii characters with one upper case  and one lower case letter\n")
  print("Enter:")
  commonkey = getpass.getpass()

  print("\nloading source cluster ......")

  #print("please enter the below details to proceed further : \n")
  #source.readDetails()

  client = boto3.client('iam')


  RoleTrustpolicy='''{
  "Version": "2012-10-17",
  "Statement": {
  "Effect": "Allow",
  "Principal":{"Service": "redshift.amazonaws.com"},
  "Action": "sts:AssumeRole"
  }
  }'''
  rolename = source.ename.split('.')[0]+destination.ename.split('.')[0]+"migrationRole"+randint(1,10000000000000).__str__()

  response = client.create_role(
      AssumeRolePolicyDocument=RoleTrustpolicy,
      Path='/',
      RoleName=rolename,
  )
  roleArn=response["Role"]["Arn"]
  #print(response["Role"]["Arn"])

  accessPolicy='''{
      "Version": "2012-10-17",
      "Statement": [
          {
              "Effect": "Allow",
              "Action": "s3:*",
              "Resource": "*"
          }
      ]
  }'''
  accesspolicyname ='s3fullonreadwriteandsavecopyunloadmyredshiftdata'+rolename
  response = client.create_policy(
      PolicyName=accesspolicyname,
      Path='/',
      PolicyDocument=accessPolicy,
      Description='to see and check s3 data'
  )
  policyarn=response['Policy']['Arn']
  #print(policyarn)

  response = client.attach_role_policy(
      RoleName=rolename,
      PolicyArn=policyarn
  )

  src = boto3.client('redshift',source.region)

  response = src.modify_cluster_iam_roles(
                  ClusterIdentifier= source.ename.split('.')[0],
                  AddIamRoles=[roleArn
                  ]
              )

  dst = boto3.client('redshift',destination.region)
  response = dst.modify_cluster_iam_roles(
                  ClusterIdentifier= destination.ename.split('.')[0],
                  AddIamRoles=[roleArn
                  ]
              )

  while(src.describe_clusters(ClusterIdentifier=source.ename.split('.')[0])['Clusters'][0]['ClusterStatus']!='available' or dst.describe_clusters(ClusterIdentifier=destination.ename.split('.')[0])['Clusters'][0]['ClusterStatus']!='available' ) :
      sleep(15)
      print('One or more Clusters in Modifying State')
      
  print('attached role with s3  access policy to both redshift clusters, role name '+rolename+' please dont modify it, it will be autodeleted once the process is ComPleted')

  #role = 'arn:aws:iam::067065364242:role/myredshiftrole'
  role = roleArn

  print('Enter bucket name where you want to store and load your backup from:')
  s3bucket = input()
  if(isBucketValid(s3bucket)):
    print('\n Fetching required scripts for migration, dont press any key')
  # Get the AdminViews sql scripts in the current working directory
    urllib.request.urlretrieve('https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_generate_schema_ddl.sql', 'v_generate_schema_ddl.sql')
    urllib.request.urlretrieve('https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_generate_group_ddl.sql', 'v_generate_group_ddl.sql')
    urllib.request.urlretrieve('https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_get_users_in_group.sql', 'v_get_users_in_group.sql')
    urllib.request.urlretrieve('https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_generate_tbl_ddl.sql', 'v_generate_tbl_ddl.sql')
    urllib.request.urlretrieve('https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_generate_view_ddl.sql', 'v_generate_view_ddl.sql')
    urllib.request.urlretrieve('https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_get_schema_priv_by_user.sql', 'v_get_schema_priv_by_user.sql')
    urllib.request.urlretrieve('https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_get_tbl_priv_by_user.sql', 'v_get_tbl_priv_by_user.sql')
    urllib.request.urlretrieve('https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_get_view_priv_by_user.sql', 'v_get_view_priv_by_user.sql')

     # Connect to source unencrypted database
    db = DB(dbname=source.dname, host=source.ename, port=source.port,user=source.uname, passwd=source.pwd)

    if(snsProvided):
      snsClient.publish(TopicArn=snsArn,Message="Logged in successfully in"+source.ename.split('.')[0]+" cluster ")



    # Create the admin views using the above scripts
    # Below redundant statements can be replaced by a simple for loop

    # Create admin schema (if running on newly created source cluster)
    db.query("CREATE SCHEMA IF NOT EXISTS admin;") # already present error only once

    # Creating the schemas, views, tables and groups using ddl statements is very important
    # Otherwise how and where would you copy the data ?
    # TODO: generate the DDL for schemas, tables and views


    temp_views= ['v_generate_tbl_ddl.sql','v_generate_view_ddl.sql','v_generate_group_ddl.sql','v_get_users_in_group.sql','v_get_schema_priv_by_user.sql','v_get_view_priv_by_user.sql','v_generate_schema_ddl.sql', 'v_get_tbl_priv_by_user.sql']
    print("\nRunnning backup scripts ......")
    for file in temp_views:
      file = open(file , 'r')
      str = file.read();
      db.query(str)



    print("\nFetching Details from Source ......")

    dqueries = []

    # Get DDL for Schema
    q = db.query("SELECT schemaname FROM admin.v_generate_schema_ddl where schemaname <>'admin';")
    #print(q.getresult())

    for queries in q.getresult():
      for query in queries:
        dqueries.append("CREATE SCHEMA IF NOT EXISTS " + query + ";")

    # Get DDL for user tables
    q = db.query("SELECT DISTINCT tablename as table, schemaname FROM admin.v_generate_tbl_ddl WHERE schemaname <> 'information_schema' AND schemaname <> 'pg_catalog';")

    print(" \n Tables in Source Cluster \n:")
    #print(q.getresult())

    user_tables=[]
    tables=[]

    for table in q.getresult():
      tables.append(table[0])
      user_tables.append(table[1]+"."+table[0])

    # print(user_tables)

    print("\nuploading source tables to S3 ......")
    #upload user tables to s3
    for table in user_tables:
      print('Uploading Table : '+table+" .......")
      db.query("UNLOAD('select * from "+table+"') TO 's3://"+s3bucket+"/"+table+"/part_1' iam_role '"+role+"' manifest ALLOWOVERWRITE;")
        
    print("Upload Complete")
    if(snsProvided):
      snsClient.publish(TopicArn=snsArn,Message="Copied data into s3 bucket"+s3bucket)

    for table in tables:
      q = db.query("SELECT ddl FROM admin.v_generate_tbl_ddl WHERE tablename='" + table + "'")
      #print(q.getresult())
      create_table_query = []
      for query in q.getresult():
        for ddl in query:
          create_table_query.append(ddl);
      dqueries.append(''.join(create_table_query[1:]))

    #q = db.query('CREATE TABLE IF NOT EXISTS public.fruits(\tid INTEGER NOT NULL  ENCODE lzo\t,name VARCHAR(256)   ENCODE lzo\t,PRIMARY KEY (id))DISTSTYLE EVEN;')

    # Get DDL for views
    # CREATE OR REPLACE VIEW admin.v_generate_tbl_ddl  ---- not executing properly, else all executing properly
    print("\nFectching Views in Source")
    q = db.query("SELECT DISTINCT viewname FROM admin.v_generate_view_ddl WHERE schemaname <> 'information_schema' AND schemaname <> 'pg_catalog' AND  schemaname <> 'admin';")

    print("\n Views in Source \n:")


    user_views=[]

    for views in q.getresult():
      for viewname in views:
        user_views.append(viewname)

    # print(user_views)

    for view in user_views:
      q = db.query("SELECT ddl FROM admin.v_generate_view_ddl WHERE viewname='" + view + "'")
      create_view_query = []
      for query in q.getresult():
        for ddl in query:
          create_view_query = ddl.splitlines()
      
      print(' '.join(create_view_query[1:]))
      dqueries.append(' '.join(create_view_query[1:]))
      
    print("\nGetting Users in Source")
    # Get the users
    # Maybe prompt for setting the password for users on the encrypted redshift cluster
    q=db.query("SELECT usename  FROM pg_user WHERE usename <> 'rdsdb';").getresult();
    #print(q)
    users=[]
    for u in q:
      for name in u:
        users.append(name);
    #print(users)
    q = db.query("SELECT 'CREATE USER '|| usename || '' FROM pg_user WHERE usename <> 'rdsdb' and usename <> '"+source.uname+"';")


    for queries in q.getresult():
        for query in queries:
            dqueries.append(query+" with password '"+commonkey+"';")

    print("\nGetting groups from Source")
    # Get the groups
    q=db.query("select groname FROM pg_group;").getresult();
    groups=[]
    for g in q:
      for name in g:
        groups.append(name);

    q = db.query("SELECT 'CREATE GROUP  '|| groname  ||';' FROM pg_group;")


    for queries in q.getresult():
      for query in queries:
        dqueries.append(query)

    print("\nGetting Users and Group correlations from Source")
    # Get users in the groups
    q = db.query("SELECT 'ALTER GROUP ' ||groname||' ADD USER '||usename||';' FROM admin.v_get_users_in_group;")
    #print(q.getresult())

    for queries in q.getresult():
      for query in queries:
        dqueries.append(query)


    # Python udf used to generate permissions for schema as concatenated strings
    udf = """create or replace function 
    f_schema_priv_granted(cre boolean, usg boolean) returns varchar
    STABLE 
    AS $$
       priv = ''
       prev = False;
       if cre:
           priv = str('create')
           prev = True
       if usg:
        if prev:
            priv = priv + str(', usage')
        else :
            priv = priv + str(' usage')
           
       return priv
    $$LANGUAGE plpythonu;"""

    db.query(udf)

    print("\Fetching Schema previleges")
    # Get schema privileges per user
    q = db.query("""SELECT 'GRANT '|| f_schema_priv_granted(cre, usg) ||' ON schema '|| schemaname || ' TO ' || usename || ';' 
    FROM admin.v_get_schema_priv_by_user 
    WHERE schemaname NOT LIKE 'pg%' 
    AND schemaname <> 'information_schema'
    AND schemaname <> 'admin'
    AND usename <> 'rdsdb';""")



    for queries in q.getresult():
      for query in queries:
        dqueries.append(query)
    print("\nFetching Permissions Per USER for tables and views ")
    # Python udf used to generate permissions for table and views as concatenated strings
    udf = """create or replace function 
    f_table_priv_granted(sel boolean, ins boolean, upd boolean, delc boolean, ref boolean) returns varchar
    STABLE 
    AS $$
       priv = ''
       prev = False;
       if sel:
            priv = str('select')
            prev = True;
            
       if ins:
        if prev :
            priv = priv + str(', insert')
        else :
            priv = priv + str(' insert')
            prev = True
       if upd:
        if prev:
            priv = priv + str(', update')
        else:
            priv = priv + str(' update')
            prev = True
           
       if delc:
        if prev:
            priv = priv + str(', delete')
        else :
            priv = priv + str(' delete')
            prev = True
           
       if ref:
        if prev:
            priv = priv + str(', references ')
        else:
            priv = priv+str(' references')
            prev = True
       return priv
    $$LANGUAGE plpythonu;"""

    db.query(udf)

    # Get table privileges per user
    q = db.query("""SELECT 'GRANT '|| f_table_priv_granted(sel, ins, upd, del, ref) || ' ON '|| 
    schemaname||'.'||tablename ||' TO '|| usename || ';' FROM admin.v_get_tbl_priv_by_user 
    WHERE schemaname NOT LIKE 'pg%' 
    AND schemaname <> 'information_schema'
    AND schemaname <> 'admin'
    AND usename <> 'rdsdb';""")



    for queries in q.getresult():
      for query in queries:
        dqueries.append(query)

    # Get view privileges per user
    q = db.query("""SELECT 'GRANT '|| f_table_priv_granted(sel, ins, upd, del, ref) || ' ON '|| 
    schemaname||'.'||viewname ||' TO '|| usename || ';' FROM admin.v_get_view_priv_by_user 
    WHERE schemaname NOT LIKE 'pg%' 
    AND schemaname <> 'information_schema'
    AND schemaname <> 'admin'
    AND usename <> 'rdsdb';""")
    #print(q)


    for queries in q.getresult():
      for query in queries:
        dqueries.append(query)


            
    db.close();

    print("\nClose connection to source and connect to destination")
    print('\nloading')
    if(snsProvided):
      snsClient.publish(TopicArn=snsArn,Message="Fetching data from "+source.ename.split('.')[0]+" complete,Closing connection with "+source.ename.split('.')[0])
    #now in destination 

    db =DB(dbname=destination.dname, host=destination.ename, port=destination.port,user=destination.uname, passwd=destination.pwd)
    print("\n\n\n\nExecuting Queries in Encrypted cluster:")
    print('======================================')

    # for u in users:
    #   if destination.uname!=u:
    #     db.query("drop user IF Exists "+u+" ;")
    # for g in groups:
    #   db.query("drop group "+g+" ;")

    if(snsProvided):
      snsClient.publish(TopicArn=snsArn,Message="successfully logged in into destination cluster("+destination.ename.split('.')[0]+") , executing required queries")
      
    count=1;

    #print(dqueries)


    #print(dqueries)
    for query in dqueries:
        print("\t\t\t\tQuery No: "+count.__str__()+"\t\t\t\t\t");
        print('\t\t\t\t-=-=-=-=-=-=-=-\t\t\t\t\t\n')
        print('\n')
        print(query)
        db.query(query)
        print('\n\n\n')
        print ('===successs====')
        print('\n')
        count+=1

    if(snsProvided):
      snsClient.publish(TopicArn=snsArn,Message="executing queries complete ,now copying data from s3: "+s3bucket+" to destination cluster : "+destination.ename.split('.')[0])

    print("Loading data from s3 into tables")
    for table in user_tables:
      if table!='lineorder':
        db.query("Copy "+table+" from 's3://"+s3bucket+"/"+table+"/part_1manifest' iam_role '"+role+"' manifest;") 

    if(snsProvided):
      snsClient.publish(TopicArn=snsArn,Message="Fetching data s3 completed")




    c=0;
    countTables = db.query("SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname <> 'information_schema' AND schemaname <> 'pg_catalog' ;").getresult()

    for i in countTables:
         for j in i:
                c=j;
    db.close()
     

    print('\nDeleting dependencies')
    print('\ncleanup in process \n')

    for file in temp_views:
      os.remove(file)

        
    response = src.modify_cluster_iam_roles(
                    ClusterIdentifier= source.ename.split('.')[0],
                    RemoveIamRoles=[roleArn
                    ]
                )


    response = dst.modify_cluster_iam_roles(
                    ClusterIdentifier= destination.ename.split('.')[0],
                    RemoveIamRoles=[roleArn
                    ]
                )

    while(src.describe_clusters(ClusterIdentifier=source.ename.split('.')[0])['Clusters'][0]['ClusterStatus']!='available' or dst.describe_clusters(ClusterIdentifier=destination.ename.split('.')[0])['Clusters'][0]['ClusterStatus']!='available' ) :
        sleep(15)
        print('detaching temporary role from Clusters and restoring to previous state')
        
        
    response = client.detach_role_policy(
        RoleName=rolename,
        PolicyArn=policyarn
        
    )
    print('detaching access policy from role')

    response = client.delete_policy(
        PolicyArn=policyarn
    )
    print('deleting access policy')
    response = client.delete_role(
        RoleName=rolename
    )
    print('deleting role')
    print('Closing open connections')

    if(c==len(tables)):
        print("==========================ENCRyPTION ComPleted=========================")
        print("\n NOTICE:\n========\nPlease check your data in your new cluster and old cluster and if everything seems to be fine feel free to delete the old cluster after taking a snapshot of it .\n Please also change the password of users from default using alter command.\n As a best practice we suggest you to enable audit logging on +"+destination.ename.split('.')[0]+" cluster if not already enabled .\n Thank you :)")
        if(snsProvided):
          snsClient.publish(TopicArn=snsArn,Message="Migration Process ComPleted")
    else:
      print("XXXXXXXXXX PROCESS FAILED XXXXXXXXXXXXXXXXX")
      if(snsProvided):
        snsClient.publish(TopicArn=snsArn,Message="Migration Process Failed ")
  else:
    print("S3 Bucket name invalid")
else:
  if(not srv):
    print("Source Endpoint not valid")
  elif(not dsv):
    print("Destination Endpoint not valid")

