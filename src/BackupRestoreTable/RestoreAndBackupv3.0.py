'''Purpose: To Restore and Backup table from/to Redshift cluster
*****6*************************************************************************
'''

import boto3
import psycopg2
import urllib.request
import time
import datetime
import getpass

class RestoreAndBackup:
    ''' 1. Initialize boto connections : s3, redshift, iam
        2. Get input for region. Check for valid redshift regions
        3. Get inputs for cluster identifier, cluster password
        4. Initialize a timestamp for storing and retrieving data
        5. Use describe_clusters to find cluster values like: dbname, port, masterusername, host
        6. Establish a connect to redshift cluster
        7. Initialize cursor '''
    def __init__(self):
        self.s3=boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.iam = boto3.client('iam')
        self.redshift_regions = ['us-east-2', 'us-east-1', 'us-west-1',
                            'us-west-2', 'ap-northeast-1', 'ap-northeast-2',
                            'ap-northeast-3', 'ap-south-1', 'ap-southeast-1',
                            'ap-southeast-2', 'ca-central-1', 'cn-north-1',
                            'cn-northwest-1', 'eu-central-1', 'eu-west-1',
                            'eu-west-1', 'eu-west-2', 'eu-west-3']
        self.cluster_region = input('enter the region (example: ap-south-1): ')
        while self.cluster_region not in self.redshift_regions:
            print("Enter correct region")
            self.cluster_region = input('enter the region (example: ap-south-1): ')
        self.redshift = boto3.client('redshift', self.cluster_region)
        self.cluster_identifier = input('enter the cluster identifier (example: my-redshift): ')
        self.cluster_identifier_pass = getpass.getpass('enter the cluster identifier password: ')
        self.timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        self.response = self.redshift.describe_clusters(
                ClusterIdentifier=self.cluster_identifier
            )
        self.cluster = self.response.get('Clusters')[0]
        self.conn_string = "dbname=" + self.cluster.get('DBName') + " port=" + str(
        self.cluster.get('Endpoint').get('Port')) + " user=" + self.cluster.get(
                'MasterUsername') + " password=" + self.cluster_identifier_pass + " host=" + self.cluster.get(
                'Endpoint').get('Address')
        print(self.conn_string)
        self.con = psycopg2.connect(self.conn_string)
        self.cursor = self.con.cursor()
        self.cursor.execute("SET statement_timeout TO 600000")
        self.con.commit()
        print("connection successful")
        print("=====================================================")


    '''1. Get the string of table names, split the string to a list
       2. Get all the Object of the given Bucket
       3. Check for table name in the Object name
       4. Get the input from the user for the Restore file
       5. Return a dictionary of the table name and the Restore file'''
    def get_all_backups(self, table_name_list, s3_location):
        table_name_all = table_name_list.split(",")
        file_loc = []
        new_table = []
        validity = True
        for table_name in table_name_all:
            list = []
            i = 1
            result = self.s3.list_objects(Bucket=s3_location,
                                     Delimiter='/')
            print("=====================================================")
            for o in result.get('CommonPrefixes'):
                if table_name in o.get('Prefix'):
                    print(str(i) + ". " + o.get('Prefix'))
                    list.append(o.get('Prefix'))
                    i = i + 1
            if len(list) > 0:
                a = int(input('Enter value for the index number corresponding to the bucket (example: 1) : '))
                new_table.append(table_name)
                file_location = list[a - 1]
                file_loc.append(file_location)
        zip_list = zip(new_table, file_loc)
        dict_backup = dict(zip_list)
        return dict_backup

    '''1. Get the table name string
       2. Split it by (') and store in a list
       3. Validate if the table is present in the cluster
       4. Return only the validated table'''
    def validate_table(self, table_name_list):
        table_name_all = table_name_list.split(",")
        list_table_valid=[]
        list_table_invalid=[]
        for table_name in table_name_all:
            try:
                self.con.commit()
                self.cursor.execute("SELECT 1 FROM {0} LIMIT 1;".format(table_name))
                list_table_valid.append(table_name)
            except Exception as e:
                list_table_invalid.append(table_name)
        print("======================================================")
        print("valid tables: ")
        print(list_table_valid)
        print("invalid tables: ")
        print(list_table_invalid)
        print("======================================================")
        return list_table_valid

    '''1. get the Bucket name
       2. Check if bucket is present or else throws error in main loop'''
    def validate_bucket(self, s3_location):
        bucket_location = self.s3.get_bucket_location(Bucket=s3_location)
        if bucket_location['LocationConstraint'] == self.cluster_region:
            return True
        elif bucket_location['LocationConstraint'] == None:
            return True
        else:
            return False

    '''1. Check for the role myRedshiftRoleToAccesss3
       2. Check for the policy AmazonS3FullAccess
       3. Check if the policy exists for the role'''
    def role_exits(self):
        valid = False
        try:
            response = self.iam.get_role(
                RoleName='myRedshiftRoleToAccesss3'
            )
            if response['Role']['RoleName']:
                response = self.iam.list_attached_role_policies(
                    RoleName='myRedshiftRoleToAccesss3')
                list = response['AttachedPolicies']
            for policy in list:
                if policy['PolicyName'] == 'AmazonS3FullAccess':
                    print('role- myRedshiftRoleToAccesss3 exists with policy- AmazonS3FullAccess ')
                    valid = True
                else:
                    print('role- myRedshiftRoleToAccesss3 exits. Attaching policy- AmazonS3FullAccess to role ')
                    role_policy = self.iam.attach_role_policy(
                        RoleName='myRedshiftRoleToAccesss3', PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess')
                    valid = True
        except Exception as e:
            valid = False
        return valid

    '''1. Check if role is attached to the redshift cluster
       2. check if cluster is available'''
    def check_role_attached(self):
        response = self.redshift.describe_clusters(
            ClusterIdentifier=self.cluster_identifier
        )
        cluster = response.get('Clusters')[0]
        res = cluster.get('IamRoles')
        l = []
        for i in res:
            l.append(i['IamRoleArn'])
        cluster_status = cluster.get('ClusterStatus')
        if cluster_status == 'available' and any('myRedshiftRoleToAccesss3' in word for word in l):
            return True
        else:
            return False

    '''1. Create Role myRedshiftRoleToAccesss3 with trust relationship of redshift
       2. Attach policy AmazonS3FullAccess to the role
       3. Get ROle ARN'''
    def create_role(self):
        self.role_create = self.iam.create_role(
            AssumeRolePolicyDocument='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":["redshift.amazonaws.com"]},"Action":["sts:AssumeRole"]}]}',
            Path='/',
            RoleName='myRedshiftRoleToAccesss3',
            )
        self.role_policy = self.iam.attach_role_policy(
            RoleName='myRedshiftRoleToAccesss3', PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess')
        self.role = self.role_create.get('Role')
        self.role_arn = self.role.get('Arn')
        return self.role_arn

    '''1. Attach role to redshift cluster'''
    def attach_role(self, role_name):
        try:
            self.role_name = role_name
            response = self.redshift.modify_cluster_iam_roles(
                ClusterIdentifier=self.cluster_identifier,
                AddIamRoles=[
                    self.role_name
                ]
            )
        except Exception as e:
            print(e)

    '''1. Check if cluster status is available or modifying'''
    def check_status(self):
        cluster_status = 'modifying'
        print('Kinldly be patient. It might take some time to attach/delete a role to your cluster. ')
        print("=====================================================")
        while cluster_status != 'available':
            response = self.redshift.describe_clusters(
                ClusterIdentifier=self.cluster_identifier
            )
            cluster = response.get('Clusters')[0]
            cluster_status = cluster.get('ClusterStatus')
            print('...cluster is %s state' % cluster_status)
            time.sleep(10)
        print('cluster available')
        print("=====================================================")

    '''1. Download the file v_generate_tbl_ddl.sql using urllib'''
    def download_ddl_file(self):
        try:
            self.url = "https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/AdminViews/v_generate_tbl_ddl.sql"
            self.filename, self.headers = urllib.request.urlretrieve(self.url, filename="v_generate_tbl_ddl.sql") #file name of how to retrieve ddl
            print ("download complete of the ddl file: ", self.filename)
            print("=====================================================")
        except urllib.error.URLError as e:
            print("File " + e.reason + " :" + self.url)

    '''1. Get the ddl of the table using the file v_generate_tbl_ddl.sql'''
    def get_ddl(self, table_name, ddl_file_name):
        self.table_name = table_name
        self.con.commit()
        self.cursor.execute("CREATE SCHEMA if not exists admin;")
        self.con.commit()

        self.cursor.execute(open("v_generate_tbl_ddl.sql", "r").read())
        self.table_ddl = self.cursor.execute("SELECT ddl from admin.v_generate_tbl_ddl where tablename = '{0}';"
                                             .format(self.table_name))
        self.con.commit()
        self.rows = self.cursor.fetchall()
        self.query=[]
        #print(self.rows)
        for row in self.rows:
            for r in row:
                self.query.append(r)
        for n, i in enumerate(self.query):
            if i == 'DISTSTYLE EVEN':
                self.query[n] = 'DISTSTYLE EVEN '
        file = open(ddl_file_name, 'w')
        self.query=self.query[1:]
        for r in self.query:
            file.write(r)
        print('ddl generated for table: ', self.table_name)
        print("=====================================================")

    '''1. Unload the data from the table to s3'''
    def unload(self, role_name, table_name, s3_location):
        self.s3_location = 's3://' + s3_location + '/RedshiftBackup_' + table_name + '_' + self.timestamp + '/' + table_name
        print('s3 location of data: ',self.s3_location)
        try:
            self.sql = """UNLOAD('select * from {0}') TO
            '{1}'
            iam_role '{2}'
            manifest ;""".format(table_name, self.s3_location, role_name)
            self.cursor.execute(self.sql)
            self.con.commit()
        except Exception as e:
            if 'IAM Role' in str(e):
                print("IAM Role not valid\n",e)
            if 'bucket' in str(e):
                print("Bucket not valid\n",e)
            else:
                print("unload")
                print(e)
        print('unload of data completed')
        print("=====================================================")

    '''1. Upload the table ddl to s3'''
    def upload_ddl_to_s3(self, s3_location, ddl_file_name, table_name):
        self.s3_location = s3_location
        data = open(ddl_file_name, 'rb')
        self.key_name = 'RedshiftBackup_' + table_name  + '_' + self.timestamp + '/' + ddl_file_name
        self.s3_resource.Bucket(self.s3_location).put_object(Key=self.key_name, Body=data)
        print('Upload of ddl completed. File name for ddl: ', self.key_name)
        print("=====================================================")

    '''1. Retrieve the URL of the Table ddl 
       2. Download the file'''
    def copy_ddl(self, table_name, s3_location, ddl_file_name, dict):
        file_location = dict[table_name]
        key = file_location + ddl_file_name
        print(key)
        try:
            bucket_location = self.s3.get_bucket_location(Bucket=s3_location)
            if bucket_location['LocationConstraint'] == 'None':
                object_url = "https://s3.amazonaws.com/{0}/{1}".format(
                    bucket_location['LocationConstraint'],
                    s3_location,
                    key)
            else:
                object_url = "https://s3-{0}.amazonaws.com/{1}/{2}".format(
                    bucket_location['LocationConstraint'],
                    s3_location,
                    key)
            print("=====================================================")
            print(object_url)
            #result = self.s3.put_object_acl(Bucket=s3_location, ACL="public-read", Key=key)
            self.filename, self.headers = urllib.request.urlretrieve(object_url, filename=ddl_file_name) #download the ddl file
        except urllib.error.URLError as e:
            print("File " + e.reason + " :" + object_url)
        print('Downloaded ddl file: ', ddl_file_name)
        print("=====================================================")
        return file_location

    '''1. Execute the table ddl'''
    def execute_ddl(self, table_name, ddl_file_name):
        print(ddl_file_name)
        try:
            self.file=open(ddl_file_name, "r").read()
            self.cursor.execute(self.file)
            self.con.commit()
        except Exception as e:
            print(e)
        print('Table ddl execution complete for table: ', table_name)
        print("=====================================================")

    '''1. Get the manifest file location
       2. Copy the data from the Manifets file'''
    def copy_data(self, role_name, table_name, s3_location, file_location):
        self.s3_manifest_location = 's3://' + s3_location + '/' + file_location + table_name + 'manifest'
        print(self.s3_manifest_location)
        key = file_location + table_name + 'manifest'
        #result = self.s3.put_object_acl(Bucket=s3_location, ACL="public-read", Key=key)
        print('s3 location of data: ', s3_location)
        print("=====================================================")
        try:
            self.sql = """Copy {0} from '{1}'
                iam_role '{2}' manifest;""".format(table_name, self.s3_manifest_location, role_name)
            self.cursor.execute(self.sql)
            self.con.commit()
        except Exception as e:
            if 'IAM Role' in str(e):
                print("IAM Role not valid\n",e)
            if 'bucket' in str(e):
                print("Bucket not valid\n",e)
            else:
                print(e)
        self.con.commit()
        print('Table data execution complete for table: ', table_name)
        print("=====================================================")

def main():
    iam=boto3.client('iam')
    try:
            x = RestoreAndBackup()
            a = input("Do you want to (press 1 or 2):\n1. Backup (get table from redshift to s3)\n2. Restore (move table back to redshift)")
            while a not in ("1","2"):
                a=input("Enter valid numbers: \n1. Backup (get table from redshift to s3)\n2. Restore (move table back to redshift)\n")
            s3_location = input("Provide the s3 bucket name (example: aws-redshift-bucket): ")
            while x.validate_bucket(s3_location) == False:
                s3_location = input("Provide the bucket in same location as cluster: ")

            table_name_list = input("Provide the table names (example: customer,employee): ")

            dict ={}
            if a== "1":
                    table_name_all = x.validate_table(table_name_list)
                    if len(table_name_all) == 0:
                        raise Exception('all tables are invalid tables')
            if a == "2":
                    dict = x.get_all_backups(table_name_list, s3_location)
                    if len(dict.keys()) == 0:
                        raise Exception('all tables are invalid tables')
                    else:
                        print("Valid Table names: ")
                        print(dict.keys())
                        table_name_all = dict.keys()

            role_name = ""
            if x.role_exits() == True:
                    response = iam.get_role(
                        RoleName='myRedshiftRoleToAccesss3'
                    )
                    role_name = response['Role']['Arn']

                    if x.check_role_attached() == False:
                        print("role is not attached to Redshift Cluster")
                        x.attach_role(role_name)
                        x.check_status()
                    else:
                        print("role is attached to Redshift Cluster")
            else:
                    role_name = x.create_role()
                    x.attach_role(role_name)
                    x.check_status()
            print("Role Name is: " + role_name)
            for table_name in table_name_all:
                        ddl_file_name = table_name + '_ddl.sql'
                        if a == '1':
                                x.download_ddl_file()
                                x.get_ddl(table_name, ddl_file_name)
                                print("=====================================================")
                                x.unload(role_name, table_name, s3_location)
                                x.upload_ddl_to_s3(s3_location, ddl_file_name, table_name)
                        elif a == '2':
                                file_location = x.copy_ddl(table_name, s3_location, ddl_file_name, dict)
                                x.execute_ddl(table_name, ddl_file_name)
                                x.copy_data(role_name,table_name, s3_location, file_location)
                        else:
                            print('enter a valid response')
                            print("=====================================================")
    except Exception as e:
        print("ERROR:")
        print(e)

if __name__ == "__main__":
    main()