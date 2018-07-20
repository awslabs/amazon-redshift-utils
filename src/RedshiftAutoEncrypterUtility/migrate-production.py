import boto3;
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
    ename = ''
    uname = ''
    dname = ''
    pwd = ''
    port = 5439

    def readCreds(self):
        print('enter endpoint :')
        self.ename = input().strip()
        print('enter database :')
        self.dname = input().strip()
        print('enter MasterUser name :')
        self.uname = input().strip()
        print('Enter admin password :')
        self.pwd = getpass.getpass().strip()
        print('Enter hosted port(default 5439) :')
        temp = input()
        if temp is '':
            self.port = 5439
        else:
            self.port = int(temp)


def commonList(src, dest):
    lst3 = [value for value in src if value in dest]
    return lst3


def isEndpointValid(endpoint):
    valid = False;
    redshift = boto3.client('redshift', endpoint.split('.')[-4])
    try:
        for clusters in redshift.describe_clusters()['Clusters']:
            if (clusters['Endpoint']['Address'] == endpoint):
                valid = True;
                break;
    except ClientError as ce:
        print(ce)

    return valid


def isBucketValid(s3bucket=''):
    s3Client = boto3.client('s3')
    for bucket in s3Client.list_buckets()['Buckets']:
        if (bucket['Name'] == s3bucket):
            return True;


def isSNSValid(x):
    valid = True
    try:
        region = x.split(':')[3]
        snsClient = boto3.client('sns', region)
        response = snsClient.get_topic_attributes(TopicArn=x)
    except IndexError:
        print("Topic does not exist with arn")
        sys.exit()
    except ClientError as e:
        print("Topic does not exist")
        valid = False
        sys.exit()
    except EndpointConnectionError as ece:
        print("Topic does not exist with arn")
        sys.exit()
    return valid


def migrateNow():
    snsClient = None
    print(
        'If you wish to recieve notifications of various stages in the process please enter an ARN of a sns topic below.(please leave it empty if you dont want any notifications)\nARN:')
    snsArn = input().strip()
    snsProvided = False

    if (snsArn != '' and isSNSValid(x=snsArn)):
        snsProvided = True

    if (snsProvided):
        snsClient = boto3.client('sns', snsArn.split(':')[3])
        snsClient.get_topic_attributes(TopicArn=snsArn)
        snsClient.publish(TopicArn=snsArn,
                          Message="Dear User, You will now now recieve notifications for migration process.")

    print("\n\t\t\t\t\t\tPlease enter source details (unencrypted cluster) : ")
    print("\t\t\t\t\t\t===============================================")

    source = Credentials()
    source.readCreds()

    print("\n\t\t\t\t\t\tPlease enter Destination details (encrypted cluster) : ")
    print("\t\t\t\t\t\t===============================================")

    destination = Credentials()
    destination.readCreds()
    print("Validating Endpoints")

    srv = isEndpointValid(source.ename)

    dsv = isEndpointValid(destination.ename)

    print('\n Fetching required scripts for migration, dont press any key')
    # Get the AdminViews sql scripts in the current working directory
    urllib.request.urlretrieve(
        'https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_generate_schema_ddl.sql',
        'v_generate_schema_ddl.sql')
    urllib.request.urlretrieve(
        'https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_generate_group_ddl.sql',
        'v_generate_group_ddl.sql')
    urllib.request.urlretrieve(
        'https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_get_users_in_group.sql',
        'v_get_users_in_group.sql')
    urllib.request.urlretrieve(
        'https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_generate_tbl_ddl.sql',
        'v_generate_tbl_ddl.sql')
    urllib.request.urlretrieve(
        'https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_generate_view_ddl.sql',
        'v_generate_view_ddl.sql')
    urllib.request.urlretrieve(
        'https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_get_schema_priv_by_user.sql',
        'v_get_schema_priv_by_user.sql')
    urllib.request.urlretrieve(
        'https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_get_tbl_priv_by_user.sql',
        'v_get_tbl_priv_by_user.sql')
    urllib.request.urlretrieve(
        'https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_get_view_priv_by_user.sql',
        'v_get_view_priv_by_user.sql')

    temp_views = ['v_generate_tbl_ddl.sql', 'v_generate_view_ddl.sql', 'v_generate_group_ddl.sql',
                  'v_get_users_in_group.sql', 'v_get_schema_priv_by_user.sql', 'v_get_view_priv_by_user.sql',
                  'v_generate_schema_ddl.sql', 'v_get_tbl_priv_by_user.sql']

    rolename = 'MigrationPolicy' + randint(1, 10000000000000).__str__()

    try:
        if (srv and dsv):

            print('Enter bucket name where you want to store and load your backup from:')
            s3bucket = input()


            print("\nCommon Key\n--------------")
            print(
                "\n please enter a common password to assign for all the users intially in Encrypted cluster as we cannoot migrate passwords")
            print(
                "\nNote:\n=====\nShould atleast contain 8 ascii characters with one upper case  and one lower case letter\n")
            print("Enter:")
            commonkey = getpass.getpass()

            print('Creating permissions for migration using role :' + rolename)
            client = boto3.client('iam')
            RoleTrustpolicy = '''{
             "Version": "2012-10-17",
             "Statement": {
             "Effect": "Allow",
             "Principal":{"Service": "redshift.amazonaws.com"},
             "Action": "sts:AssumeRole"
             }
             }'''

            response = client.create_role(
                AssumeRolePolicyDocument=RoleTrustpolicy,
                Path='/',
                RoleName=rolename,
            )
            roleArn = response["Role"]["Arn"]
            # print(response["Role"]["Arn"])

            accessPolicy = '''{
                 "Version": "2012-10-17",
                 "Statement": [
                     {
                         "Effect": "Allow",
                         "Action": "s3:*",
                         "Resource": "*"
                     }
                 ]
             }'''
            accesspolicyname = 's3fullonreadwriteandsavecopyunloadmyredshiftdata' + rolename
            response = client.create_policy(
                PolicyName=accesspolicyname,
                Path='/',
                PolicyDocument=accessPolicy,
                Description='to see and check s3 data'
            )
            policyarn = response['Policy']['Arn']
            # print(policyarn)

            response = client.attach_role_policy(
                RoleName=rolename,
                PolicyArn=policyarn
            )

            src = boto3.client('redshift', source.ename.split('.')[-4])

            response = src.modify_cluster_iam_roles(
                ClusterIdentifier=source.ename.split('.')[0],
                AddIamRoles=[roleArn
                             ]
            )

            dst = boto3.client('redshift', destination.ename.split('.')[-4])

            response = dst.modify_cluster_iam_roles(
                ClusterIdentifier=destination.ename.split('.')[0],
                AddIamRoles=[roleArn
                             ]
            )

            while (src.describe_clusters(ClusterIdentifier=source.ename.split('.')[0])['Clusters'][0][
                       'ClusterStatus'] != 'available' or
                   dst.describe_clusters(ClusterIdentifier=destination.ename.split('.')[0])['Clusters'][0][
                       'ClusterStatus'] != 'available'):
                print('One or more Clusters in Modifying State')
                sleep(15)
                print('One or more Clusters in Modifying State')

            print(
                'attached role with s3  access policy to both redshift clusters, role name ' + rolename + ' please dont modify it, it will be autodeleted once the process is ComPleted')
            role = roleArn


            if (isBucketValid(s3bucket)):

                # Connect to source unencrypted database
                db = DB(dbname=source.dname, host=source.ename, port=source.port, user=source.uname, passwd=source.pwd)

                if (snsProvided):
                    snsClient.publish(TopicArn=snsArn,
                                      Message="Logged in successfully in" + source.ename.split('.')[0] + " cluster ")

                # Create the admin views using the above scripts
                # Below redundant statements can be replaced by a simple for loop

                # Create admin schema (if running on newly created source cluster)
                db.query("CREATE SCHEMA IF NOT EXISTS admin;")  # already present error only once

                # Creating the schemas, views, tables and groups using ddl statements is very important
                # Otherwise how and where would you copy the data ?
                # TODO: generate the DDL for schemas, tables and views

                print("\nRunnning backup scripts ......")
                for filename in temp_views:
                    file = open(filename, 'r')

                    str = file.read();
                    db.query('drop view if exists admin.{0};'.format(filename.split('.')[0]))
                    db.query(str)

                print("\nFetching Details from Source ......")

                dqueries = []

                # Get DDL for Schema
                q = db.query("SELECT schemaname FROM admin.v_generate_schema_ddl where schemaname <>'admin';")
                # print(q.getresult())

                for queries in q.getresult():
                    for query in queries:
                        dqueries.append("CREATE SCHEMA IF NOT EXISTS " + query + ";")

                # Get DDL for user tables
                q = db.query(
                    "SELECT DISTINCT tablename as table, schemaname FROM admin.v_generate_tbl_ddl WHERE schemaname <> 'information_schema' AND schemaname <> 'pg_catalog';")

                print(" \n Tables in Source Cluster \n:")
                # print(q.getresult())

                user_tables = []
                tables = []

                for table in q.getresult():
                    tables.append(table[0])
                    user_tables.append(table[1] + "." + table[0])

                # print(user_tables)

                print("\nuploading source tables to S3 ......")
                # upload user tables to s3
                for table in user_tables:
                    print('Uploading Table : ' + table + " .......")
                    db.query(
                        "UNLOAD('select * from " + table + "') TO 's3://" + s3bucket + "/" + table + "/part_1' iam_role '" + role + "' manifest ALLOWOVERWRITE;")

                print("Upload Complete")
                if (snsProvided):
                    snsClient.publish(TopicArn=snsArn, Message="Copied data into s3 bucket" + s3bucket)

                for table in tables:
                    q = db.query("SELECT ddl FROM admin.v_generate_tbl_ddl WHERE tablename='" + table + "'")
                    # print(q.getresult())
                    create_table_query = []
                    for query in q.getresult():
                        for ddl in query:
                            create_table_query.append(ddl);
                    dqueries.append(''.join(create_table_query[1:]))

                # q = db.query('CREATE TABLE IF NOT EXISTS public.fruits(\tid INTEGER NOT NULL  ENCODE lzo\t,name VARCHAR(256)   ENCODE lzo\t,PRIMARY KEY (id))DISTSTYLE EVEN;')

                # Get DDL for views
                # CREATE OR REPLACE VIEW admin.v_generate_tbl_ddl  ---- not executing properly, else all executing properly
                print("\n Fectching Views in Source")
                q = db.query(
                    "SELECT DISTINCT viewname FROM admin.v_generate_view_ddl WHERE schemaname <> 'information_schema' AND schemaname <> 'pg_catalog' AND  schemaname <> 'admin';")

                #print("\n Views in Source \n:")

                user_views = []

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

                    # print(' '.join(create_view_query[1:]))
                    dqueries.append(' '.join(create_view_query[1:]))

                print("\nGetting Users in Source")
                # Get the users
                # Maybe prompt for setting the password for users on the encrypted redshift cluster
                q = db.query(
                    "SELECT usename  FROM pg_user WHERE usename <> 'rdsdb' and usename<>'" + source.uname + "';").getresult();
                # print(q)
                users = []
                for u in q:
                    for name in u:
                        users.append(name);
                # print(users)
                q = db.query(
                    "SELECT 'CREATE USER '|| usename || '' FROM pg_user WHERE usename <> 'rdsdb' and usename <> '" + source.uname + "';")

                for queries in q.getresult():
                    for query in queries:
                        dqueries.append(query + " with password '" + commonkey + "';")

                print("\nGetting groups from Source")
                # Get the groups
                q = db.query("select groname FROM pg_group;").getresult();
                groups = []
                for g in q:
                    for name in g:
                        groups.append(name);

                q = db.query("SELECT 'CREATE GROUP  '|| groname  ||';' FROM pg_group;")

                for queries in q.getresult():
                    for query in queries:
                        dqueries.append(query)

                print("\nGetting Users and Group correlations from Source")
                # Get users in the groups
                q = db.query(
                    "SELECT 'ALTER GROUP ' ||groname||' ADD USER '||usename||';' FROM admin.v_get_users_in_group;")
                # print(q.getresult())

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

                print("Fetching Schema previleges")
                # Get schema privileges per user
                q = db.query("""SELECT 'GRANT '|| f_schema_priv_granted(cre, usg) ||' ON schema '|| schemaname || ' TO ' || usename || ';' 
               FROM admin.v_get_schema_priv_by_user 
               WHERE schemaname NOT LIKE 'pg%' 
               AND schemaname <> 'information_schema'
               AND schemaname <> 'admin'
               AND usename <> 'rdsdb'AND usename <>'"""+source.uname+"';")

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
               AND usename <> 'rdsdb'
               AND usename <>'"""+source.uname+"';")

                for queries in q.getresult():
                    for query in queries:
                        dqueries.append(query)

                # Get view privileges per user
                q = db.query("""SELECT 'GRANT '|| f_table_priv_granted(sel, ins, upd, del, ref) || ' ON '|| 
               schemaname||'.'||viewname ||' TO '|| usename || ';' FROM admin.v_get_view_priv_by_user 
               WHERE schemaname NOT LIKE 'pg%' 
               AND schemaname <> 'information_schema'
               AND schemaname <> 'admin'
               AND usename <> 'rdsdb' AND usename <>'"""+source.uname+"';")
                # print(q)

                for queries in q.getresult():
                    for query in queries:
                        dqueries.append(query)

                db.close();

                print("\nClose connection to source and connect to destination")
                print('\nloading')
                if (snsProvided):
                    snsClient.publish(TopicArn=snsArn, Message="Fetching data from " + source.ename.split('.')[
                        0] + " complete,Closing connection with " + source.ename.split('.')[0])
                # now in destination

                db = DB(dbname=destination.dname, host=destination.ename, port=destination.port, user=destination.uname,
                        passwd=destination.pwd)
                print("\n\n\n\nExecuting Queries in Encrypted cluster:")
                print('======================================')
                db.query("CREATE SCHEMA IF NOT EXISTS admin;")
                urllib.request.urlretrieve(
                    'https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_generate_user_grant_revoke_ddl.sql',
                    'v_generate_user_grant_revoke_ddl.sql')
                filename = 'v_generate_user_grant_revoke_ddl.sql'
                file = open(filename, 'r')

                str = file.read();
                db.query('drop view if exists admin.{0};'.format(filename.split('.')[0]))
                db.query(str)
                os.remove(filename)

                # for u in users:
                #   if destination.uname!=u:
                #     db.query("drop user IF Exists "+u+" ;")
                # for g in groups:
                #   db.query("drop group "+g+" ;")

                if (snsProvided):
                    snsClient.publish(TopicArn=snsArn, Message="successfully logged in into destination cluster(" +
                                                               destination.ename.split('.')[
                                                                   0] + ") , executing required queries")

                q = db.query(
                    "SELECT usename  FROM pg_user WHERE usename <> 'rdsdb' and usename<>'" + destination.uname + "';").getresult();
                # print(q)
                users1 = []
                for u in q:
                    for name in u:
                        users1.append(name);

                commonUsers = []
                if (len(users1) > 0):
                    commonUsers = commonList(users, users1)
                # deleting common users.
                if (len(commonUsers) > 0):
                    for user in commonUsers:
                        q = db.query(
                            "select ddl from admin.v_generate_user_grant_revoke_ddl where (grantee='" + user + "' or grantor='" + user + "') and ddltype='revoke' order by ddl;")

                        for x in q.getresult():
                            for query in x:
                                db.query(query);
                        db.query("drop user \"" + user + "\";")

                # delete groups
                groups1 = []
                q = db.query("select groname from pg_group")
                for g in q.getresult():
                    for group in g:
                        groups1.append(group);

                commonGroups = []
                if (len(groups1) > 0):
                    commonGroups = commonList(groups, groups1)

                if (len(commonGroups) > 0):
                    for group in commonGroups:
                        db.query("drop group \"" + group + "\";")

                count = 1

                #print(dqueries)

                # print(dqueries)
                try:

                    for query in dqueries:
                        print("\t\t\t\tQuery No: " + count.__str__() + "\t\t\t\t\t");
                        print('\t\t\t\t-=-=-=-=-=-=-=-\t\t\t\t\t\n')
                        print('\n')
                        print(query)
                        db.query(query)
                        print('\n\n\n')
                        print('===successs====')
                        print('\n')
                        count += 1

                    if (snsProvided):
                        snsClient.publish(TopicArn=snsArn,
                                          Message="executing queries complete ,now copying data from s3: " + s3bucket + " to destination cluster : " +
                                                  destination.ename.split('.')[0])

                    print("Loading data from s3 into tables")

                    for table in user_tables:
                        if table != 'lineorder':
                            db.query(
                                "Copy " + table + " from 's3://" + s3bucket + "/" + table + "/part_1manifest' iam_role '" + role + "' manifest;")

                    if (snsProvided):
                        snsClient.publish(TopicArn=snsArn, Message="Fetching data s3 completed")

                except Exception as error:
                    #print("Inside The excpet block for internal errors")
                    for file in temp_views:
                        os.remove(file)
                    response = src.modify_cluster_iam_roles(ClusterIdentifier=source.ename.split('.')[0],
                                                            RemoveIamRoles=[roleArn])

                    response = dst.modify_cluster_iam_roles(
                        ClusterIdentifier=destination.ename.split('.')[0], RemoveIamRoles=[roleArn])

                    while (src.describe_clusters(ClusterIdentifier=source.ename.split('.')[0])['Clusters'][0][
                               'ClusterStatus'] != 'available' or
                           dst.describe_clusters(ClusterIdentifier=destination.ename.split('.')[0])['Clusters'][0][
                               'ClusterStatus'] != 'available'):
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
                    print(error)
                    db.close()
                    print('connections closed')
                    print('Migration Failure')
                    sys.exit()

                if (snsProvided):
                    snsClient.publish(TopicArn=snsArn,
                                      Message="executing queries complete ,now copying data from s3: " + s3bucket + " to destination cluster : " +
                                              destination.ename.split('.')[0])

                print("Loading data from s3 into tables")
                for table in user_tables:
                    if table != 'lineorder':
                        db.query(
                            "Copy " + table + " from 's3://" + s3bucket + "/" + table + "/part_1manifest' iam_role '" + role + "' manifest;")

                if (snsProvided):
                    snsClient.publish(TopicArn=snsArn, Message="Fetching data s3 completed")

                c = 0
                countTables = db.query(
                    "SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname <> 'information_schema' AND schemaname <> 'pg_catalog' ;").getresult()

                for i in countTables:
                    for j in i:
                        c = j;
                db.close()

                print('\nDeleting dependencies')
                print('\ncleanup in process \n')

                for file in temp_views:
                    os.remove(file)

                response = src.modify_cluster_iam_roles(
                    ClusterIdentifier=source.ename.split('.')[0],
                    RemoveIamRoles=[roleArn
                                    ]
                )

                response = dst.modify_cluster_iam_roles(
                    ClusterIdentifier=destination.ename.split('.')[0],
                    RemoveIamRoles=[roleArn]
                )

                while (src.describe_clusters(ClusterIdentifier=source.ename.split('.')[0])['Clusters'][0][
                           'ClusterStatus'] != 'available' or
                       dst.describe_clusters(ClusterIdentifier=destination.ename.split('.')[0])['Clusters'][0][
                           'ClusterStatus'] != 'available'):
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
                print('dependent files deleted')
                print('Closing open connections')

                if (c == len(tables)):
                    print("==========================ENCRyPTION ComPleted=========================")
                    print(
                        "\n NOTICE:\n========\nPlease check your data in your new cluster and old cluster and if everything seems to be fine feel free to delete the old cluster after taking a snapshot of it .\n Please also change the password of users from default using alter command.\n As a best practice we suggest you to enable audit logging on +" +
                        destination.ename.split('.')[0] + " cluster if not already enabled .\n Thank you :)")

                    if (snsProvided):
                        snsClient.publish(TopicArn=snsArn, Message="Migration Process Completed")
                else:

                    print("XXXXXXXXXX PROCESS FAILED XXXXXXXXXXXXXXXXX")

                    if (snsProvided):
                        snsClient.publish(TopicArn=snsArn, Message="Migration Process Failed ")
            else:
                print("S3 Bucket name invalid")


        else:
            if (not srv):

                for file in temp_views:
                    os.remove(file)
                print("Source Endpoint not valid")

            elif (not dsv):

                for file in temp_views:
                    os.remove(file)
                print("Destination Endpoint not valid")

    except Exception as error:
        print("\n\n Please delete IAM role from AWS console (also disassociate if still associated with redshift cluster) " + rolename);
        print(error)
        print('Migration Failure')


migrateNow()