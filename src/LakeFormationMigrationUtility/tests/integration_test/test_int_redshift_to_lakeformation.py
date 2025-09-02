
import configparser
import psycopg2
import os
import boto3
import uuid
import subprocess
import sys
import json
from datetime import datetime

def get_redshift_credentials(config):
    # Get AWS configuration
    region = config['aws']['region']
    profile = config['aws'].get('config_profile', None)
    secret_name = config['redshift']['secret_name']
    
    # Create boto3 session and clients
    session = boto3.Session(profile_name=profile, region_name=region) if profile else boto3.Session(region_name=region)
    secrets_client = session.client('secretsmanager')
    
    try:
        # Get secret value
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        
        # Create connection parameters
        conn_params = {
            'host': config['redshift']['host'],
            'port': config['redshift']['port'],
            'dbname': config['redshift']['dbname'],
            'user': secret['username'],
            'password': secret['password']
        }
        
        return conn_params
    except Exception as e:
        print(f"Error retrieving Redshift credentials from Secrets Manager: {e}")
        raise

def create_iam_users():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'test_int_config.ini')
    config.read(config_path)
    
    # Check auth_type - only run if IAM
    auth_type = config['aws']['auth_type']
    if auth_type.upper() != 'IAM':
        print("Skipping IAM users creation - auth_type is not set to IAM")
        return
    
    # Get AWS configuration
    region = config['aws']['region']
    profile = config['aws'].get('config_profile', None)
    
    # Create boto3 session and clients
    session = boto3.Session(profile_name=profile, region_name=region) if profile else boto3.Session(region_name=region)
    iam_client = session.client('iam')
    
    users = [
        'test_user_admin',
        'test_user_analyst1',
        'test_user_analyst2',
        'test_user_etl1',
        'test_user_etl2'
    ]
    
    roles = [
        'test_user_adhoc'
    ]
    
    users_created = 0
    roles_created = 0
    
    try:
        print("\n=== CREATING IAM USERS AND ROLES ===")
        
        # Create IAM users
        for username in users:
            try:
                iam_client.create_user(UserName=username)
                print(f"Created IAM user: {username}")
                users_created += 1
            except Exception as e:
                print(f"Error creating IAM user {username}: {e}")
        
        # Create IAM roles
        for rolename in roles:
            try:
                assume_role_policy = {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "redshift.amazonaws.com"
                            },
                            "Action": "sts:AssumeRole"
                        }
                    ]
                }
                iam_client.create_role(
                    RoleName=rolename,
                    AssumeRolePolicyDocument=str(assume_role_policy).replace("'", '"')
                )
                print(f"Created IAM role: {rolename}")
                roles_created += 1
            except Exception as e:
                print(f"Error creating IAM role {rolename}: {e}")
        
        print(f"IAM users and roles creation completed successfully! Created {users_created} users and {roles_created} roles.")
        
    except Exception as e:
        print(f"Error setting up IAM users: {e}")

def setup_redshift_users_and_roles():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'test_int_config.ini')
    config.read(config_path)
    
    # Check auth_type - skip user creation if IAM
    auth_type = config['aws']['auth_type']
    
    # Get connection parameters from Secrets Manager
    conn_params = get_redshift_credentials(config)
    
    datashare_db = config['parameters']['datashare_database_name']
    object_level_permissions = config.getboolean('parameters', 'object_level_permissions')
    
    users = [
        'test_user_admin',
        'test_user_analyst1',
        'test_user_analyst2',
        'test_user_analyst3',
        'test_user_etl1',
        'test_user_etl2',
        'test_user_adhoc'
    ]
    
    # Define roles
    roles = {
        'test_role_bn_admin': ['test_user_admin'],
        'test_role_bn_analyst': ['test_user_analyst1', 'test_user_analyst2'],
        'test_role_bn_etl': ['test_user_etl1', 'test_user_etl2'],
        'test_role_bn_adhoc': ['test_user_adhoc']
    }
    
    users_created = 0
    roles_created = 0
    
    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create users only if auth_type is not IAM
        if auth_type.upper() != 'IAM':
            for username in users:
                password = f"{username}_Pass123"
                try:
                    cursor.execute(f"CREATE USER {username} PASSWORD '{password}';")
                    print(f"Created user: {username}")
                    users_created += 1
                except Exception as e:
                    print(f"Error creating user {username}: {e}")
        else:
            print("Skipping normal user creation - auth_type is set to IAM")
        
        # Create roles and assign to users
        for role_name, role_users in roles.items():
            try:
                cursor.execute(f"CREATE ROLE {role_name};")
                print(f"Created role: {role_name}")
                roles_created += 1
                
                # Only grant roles to users if auth_type is not IAM
                if auth_type.upper() != 'IAM':
                    for username in role_users:
                        cursor.execute(f"GRANT ROLE {role_name} TO {username};")
                        print(f"Granted role {role_name} to user {username}")
            except Exception as e:
                print(f"Error with role {role_name}: {e}")
        
        # Create restricted role for ETL
        try:
            cursor.execute("CREATE ROLE test_role_tn_tickit_restricted;")
            print("Created role: test_role_tn_tickit_restricted")
            roles_created += 1
            
            # Grant restricted role to ETL role
            cursor.execute("GRANT ROLE test_role_tn_tickit_restricted TO ROLE test_role_bn_etl;")
            print("Granted test_role_tn_tickit_restricted to test_role_bn_etl")
        except Exception as e:
            print(f"Error creating restricted role: {e}")
        
        # Grant permissions to roles
        try:
            # Grant USAGE permissions based on object_level_permissions setting
            all_roles = ['test_role_bn_admin', 'test_role_bn_analyst', 'test_role_bn_etl', 'test_role_tn_tickit_restricted', 'test_role_bn_adhoc']
            for role in all_roles:
                cursor.execute(f"GRANT USAGE ON DATABASE {datashare_db} TO ROLE {role};")
            
                # Grant database USAGE to test_user_analyst3
                cursor.execute(f"GRANT USAGE ON DATABASE {datashare_db} TO test_user_analyst3;")
                if object_level_permissions:
                    cursor.execute(f"GRANT USAGE ON SCHEMA {datashare_db}.tickit TO ROLE {role};")
            if object_level_permissions:
                print(f"Granted USAGE on database {datashare_db} and schema tickit to all roles")
            else:
                print(f"Granted USAGE on database {datashare_db} to all roles")
            
            # Only grant table permissions when object_level_permissions is true
            if object_level_permissions:
                # Admin role - all permissions on all tables
                cursor.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA {datashare_db}.tickit TO ROLE test_role_bn_admin;")
                print(f"Granted ALL permissions on {datashare_db}.tickit to test_role_bn_admin")
                
                # Analyst role - select on all tables
                cursor.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {datashare_db}.tickit TO ROLE test_role_bn_analyst;")
                print(f"Granted SELECT permissions on {datashare_db}.tickit to test_role_bn_analyst")
                
                # Restricted role - specific table permissions
                cursor.execute(f"GRANT SELECT, UPDATE, DELETE, INSERT ON TABLE {datashare_db}.tickit.sales TO ROLE test_role_tn_tickit_restricted;")
                cursor.execute(f"GRANT SELECT, UPDATE, DELETE, INSERT ON TABLE {datashare_db}.tickit.event TO ROLE test_role_tn_tickit_restricted;")
                print(f"Granted specific permissions on sales and event tables to test_role_tn_tickit_restricted")
                
                # Adhoc role - select on sales table
                cursor.execute(f"GRANT SELECT ON TABLE {datashare_db}.tickit.sales TO ROLE test_role_bn_adhoc;")
                print(f"Granted SELECT permissions on sales table to test_role_bn_adhoc")
                
                # Direct grants to test_user_analyst3
                cursor.execute(f"GRANT USAGE ON SCHEMA {datashare_db}.tickit TO test_user_analyst3;")
                cursor.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {datashare_db}.tickit TO test_user_analyst3;")
                print(f"Granted USAGE on schema and SELECT permissions on {datashare_db}.tickit to test_user_analyst3")
            else:
                print("Skipping table-level permissions - object_level_permissions is set to false")
        except Exception as e:
            print(f"Error granting permissions: {e}")
        
        print(f"Setup completed successfully! Created {users_created} users and {roles_created} roles.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()



def create_iam_users_in_redshift():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'test_int_config.ini')
    config.read(config_path)
    
    # Get connection parameters from Secrets Manager
    conn_params = get_redshift_credentials(config)
    
    users = [
        'test_user_admin',
        'test_user_analyst1',
        'test_user_analyst2',
        'test_user_etl1',
        'test_user_etl2'
    ]
    
    # Define role mappings
    role_mappings = {
        'test_role_bn_admin': ['test_user_admin'],
        'test_role_bn_analyst': ['test_user_analyst1', 'test_user_analyst2'],
        'test_role_bn_etl': ['test_user_etl1', 'test_user_etl2'],
        'test_role_bn_adhoc': ['test_user_adhoc']
    }
    
    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("\n=== CREATING IAM USERS IN REDSHIFT ===")
        
        # Create IAM users with IAM: prefix
        for username in users:
            iam_username = f"IAM:{username}"
            try:
                cursor.execute(f'CREATE USER "{iam_username}" PASSWORD DISABLE;')
                print(f"Created Redshift IAM user: {iam_username}")
            except Exception as e:
                print(f"Error creating Redshift IAM user {iam_username}: {e}")
        
        # Create IAMR: user for test_user_adhoc
        try:
            cursor.execute('CREATE USER "IAMR:test_user_adhoc" PASSWORD DISABLE;')
            print("Created Redshift IAM role user: IAMR:test_user_adhoc")
        except Exception as e:
            print(f"Error creating Redshift IAM role user IAMR:test_user_adhoc: {e}")
        
        # Grant roles to IAM users
        for role_name, role_users in role_mappings.items():
            for username in role_users:
                if username == 'test_user_adhoc':
                    iam_username = f"IAMR:{username}"
                else:
                    iam_username = f"IAM:{username}"
                try:
                    cursor.execute(f'GRANT ROLE {role_name} TO "{iam_username}";')
                    print(f"Granted role {role_name} to {iam_username}")
                except Exception as e:
                    print(f"Error granting role {role_name} to {iam_username}: {e}")
        
        print("IAM users in Redshift setup completed successfully!")
        
    except Exception as e:
        print(f"Error setting up IAM users in Redshift: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def setup_idc_users_and_groups():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'test_int_config.ini')
    config.read(config_path)
    
    # Check auth_type
    auth_type = config['aws']['auth_type']
    if auth_type.upper() == 'IAM':
        # Create IAM users in Redshift when auth_type is IAM
        create_iam_users_in_redshift()
        return
    
    # Get IAM Identity Center configuration
    identity_store_id = config['aws']['identity_store_id']
    region = config['aws']['region']
    profile = config['aws'].get('config_profile', None)
    
    # Create boto3 session and clients
    session = boto3.Session(profile_name=profile, region_name=region) if profile else boto3.Session(region_name=region)
    identity_store_client = session.client('identitystore')
    
    users = [
        'test_user_admin',
        'test_user_analyst1',
        'test_user_analyst2',
        'test_user_analyst3',
        'test_user_etl1',
        'test_user_etl2',
        'test_user_adhoc'
    ]
    
    # Define groups (same as Redshift roles)
    groups = {
        'test_role_bn_admin': ['test_user_admin'],
        'test_role_bn_analyst': ['test_user_analyst1', 'test_user_analyst2'],
        'test_role_bn_etl': ['test_user_etl1', 'test_user_etl2'],
        'test_role_bn_adhoc': ['test_user_adhoc']
    }
    
    user_ids = {}
    group_ids = {}
    users_created = 0
    groups_created = 0
    
    try:
        # Create users in IAM Identity Center
        for username in users:
            try:
                # Create user
                response = identity_store_client.create_user(
                    IdentityStoreId=identity_store_id,
                    UserName=username,
                    Name={
                        'GivenName': username.split('_')[-1].capitalize(),
                        'FamilyName': 'TestUser'
                    },
                    DisplayName=username,
                    Emails=[{
                        'Value': f"{username}@example.com",
                        'Type': 'work',
                        'Primary': True
                    }]
                )
                user_ids[username] = response['UserId']
                print(f"Created IAM Identity Center user: {username} (ID: {user_ids[username]})")
                users_created += 1
            except Exception as e:
                print(f"Error creating IAM Identity Center user {username}: {e}")
        
        # Create groups in IAM Identity Center
        for group_name in groups.keys():
            try:
                # Create group
                response = identity_store_client.create_group(
                    IdentityStoreId=identity_store_id,
                    DisplayName=group_name,
                    Description=f"Group for {group_name} users"
                )
                group_ids[group_name] = response['GroupId']
                print(f"Created IAM Identity Center group: {group_name} (ID: {group_ids[group_name]})")
                groups_created += 1
            except Exception as e:
                print(f"Error creating IAM Identity Center group {group_name}: {e}")
        
        # Assign users to groups
        for group_name, group_users in groups.items():
            if group_name not in group_ids:
                continue
                
            for username in group_users:
                if username not in user_ids:
                    continue
                    
                try:
                    identity_store_client.create_group_membership(
                        IdentityStoreId=identity_store_id,
                        GroupId=group_ids[group_name],
                        MemberId={
                            'UserId': user_ids[username]
                        }
                    )
                    print(f"Added user {username} to group {group_name}")
                except Exception as e:
                    print(f"Error adding user {username} to group {group_name}: {e}")
        
        print(f"IAM Identity Center setup completed successfully! Created {users_created} users and {groups_created} groups.")
        
    except Exception as e:
        print(f"Error setting up IAM Identity Center: {e}")



def create_awsidc_roles():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'test_int_config.ini')
    config.read(config_path)
    
    # Check auth_type - skip if IAM
    auth_type = config['aws']['auth_type']
    if auth_type.upper() == 'IAM':
        print("Skipping AWSIDC roles creation - auth_type is set to IAM")
        return
    
    # Get connection parameters from Secrets Manager
    conn_params = get_redshift_credentials(config)
    
    datashare_db = config['parameters']['datashare_database_name']
    object_level_permissions = config.getboolean('parameters', 'object_level_permissions')
    
    # Define the mapping between local roles and AWSIDC roles
    role_mappings = {
        'test_role_bn_admin': 'AWSIDC:test_role_bn_admin',
        'test_role_bn_analyst': 'AWSIDC:test_role_bn_analyst',
        'test_role_bn_etl': 'AWSIDC:test_role_bn_etl',
        'test_role_bn_adhoc': 'AWSIDC:test_role_bn_adhoc'
    }
    
    awsidc_roles_created = 0
    
    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("\n=== CREATING AWSIDC ROLES ===")
        
        # Create AWSIDC roles
        for local_role, awsidc_role in role_mappings.items():
            try:
                cursor.execute(f'CREATE ROLE "{awsidc_role}";')
                print(f"Created AWSIDC role: {awsidc_role}")
                awsidc_roles_created += 1
            except Exception as e:
                print(f"Error creating AWSIDC role {awsidc_role}: {e}")
        
        # Grant existing technical role to AWSIDC ETL role
        try:
            cursor.execute('GRANT ROLE test_role_tn_tickit_restricted TO ROLE "AWSIDC:test_role_bn_etl";')
            print("Granted test_role_tn_tickit_restricted to AWSIDC:test_role_bn_etl")
        except Exception as e:
            print(f"Error granting technical role to AWSIDC ETL role: {e}")
        
        # Create AWSIDC user for test_user_analyst3@example.com
        try:
            cursor.execute('CREATE USER "AWSIDC:test_user_analyst3@example.com" PASSWORD DISABLE;')
            print("Created AWSIDC user: AWSIDC:test_user_analyst3@example.com")
            awsidc_roles_created += 1
        except Exception as e:
            print(f"Error creating AWSIDC user AWSIDC:test_user_analyst3@example.com: {e}")
            
        # Grant permissions to AWSIDC roles based on their local counterparts
        try:
            # Grant USAGE permissions based on object_level_permissions setting
            awsidc_roles_list = ['"AWSIDC:test_role_bn_admin"', '"AWSIDC:test_role_bn_analyst"', '"AWSIDC:test_role_bn_etl"', '"AWSIDC:test_role_bn_adhoc"']
            for role in awsidc_roles_list:
                cursor.execute(f"GRANT USAGE ON DATABASE {datashare_db} TO ROLE {role};")
                if object_level_permissions:
                    cursor.execute(f"GRANT USAGE ON SCHEMA {datashare_db}.tickit TO ROLE {role};")
            
            # Grant USAGE on schema to test_user_analyst3 regardless of object_level_permissions setting
            cursor.execute(f'GRANT USAGE ON DATABASE {datashare_db} TO "AWSIDC:test_user_analyst3@example.com";')
            if object_level_permissions:
                cursor.execute(f'GRANT USAGE ON SCHEMA {datashare_db}.tickit TO "AWSIDC:test_user_analyst3@example.com";')
            if object_level_permissions:
                print(f"Granted USAGE on database {datashare_db} and schema tickit to all AWSIDC roles")
            else:
                print(f"Granted USAGE on database {datashare_db} to all AWSIDC roles")
            
            # Only grant table permissions when object_level_permissions is true
            if object_level_permissions:
                # AWSIDC Admin role - same as local admin role
                cursor.execute(f'GRANT ALL ON ALL TABLES IN SCHEMA {datashare_db}.tickit TO ROLE "AWSIDC:test_role_bn_admin";')
                print(f"Granted ALL permissions on {datashare_db}.tickit to AWSIDC:test_role_bn_admin")
                
                # AWSIDC Analyst role - same as local analyst role
                cursor.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {datashare_db}.tickit TO ROLE "AWSIDC:test_role_bn_analyst";')
                print(f"Granted SELECT permissions on {datashare_db}.tickit to AWSIDC:test_role_bn_analyst")
                
                # AWSIDC Adhoc role - select on sales table
                cursor.execute(f'GRANT SELECT ON TABLE {datashare_db}.tickit.sales TO ROLE "AWSIDC:test_role_bn_adhoc";')
                print(f"Granted SELECT permissions on sales table to AWSIDC:test_role_bn_adhoc")
                
                # AWSIDC test_user_analyst3@example.com - direct grants
                cursor.execute(f'GRANT USAGE ON SCHEMA {datashare_db}.tickit TO "AWSIDC:test_user_analyst3@example.com";')
                cursor.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {datashare_db}.tickit TO "AWSIDC:test_user_analyst3@example.com";')
                print(f"Granted USAGE on schema and SELECT permissions on {datashare_db}.tickit to AWSIDC:test_user_analyst3@example.com")
            else:
                print("Skipping table-level permissions for AWSIDC roles - object_level_permissions is set to false")
        except Exception as e:
            print(f"Error granting permissions to AWSIDC roles: {e}")
        
        print(f"AWSIDC roles setup completed successfully! Created {awsidc_roles_created} IDC roles in Redshift.")
        
    except Exception as e:
        print(f"Error setting up AWSIDC roles: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def execute_ds_privs_to_lf():
    config_path = os.path.join(os.path.dirname(__file__), 'test_int_config.ini')
    ds_script_path = os.path.join(os.path.dirname(__file__), '../../src', 'rs_privs_to_lf.py')
    
    try:
        print("\n=== EXECUTING DS_PRIVS_TO_LF SCRIPT ===")
        result = subprocess.run(
            [sys.executable, ds_script_path, '--config', config_path],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(ds_script_path)
        )
        
        print(f"Return code: {result.returncode}")
        if result.stdout:
            print(f"Output:\n{result.stdout}")
        if result.stderr:
            print(f"Errors:\n{result.stderr}")
            
        if result.returncode == 0:
            print("rs_privs_to_lf.py executed successfully!")
        else:
            print(f"rs_privs_to_lf.py execution failed with return code {result.returncode}")
            
    except Exception as e:
        print(f"Error executing rs_privs_to_lf.py: {e}")



def generate_unified_cleanup_script():
    # Import the cleanup script generator
    from cleanup_script_generator import generate_unified_cleanup_script
    return generate_unified_cleanup_script()
   

if __name__ == "__main__":
    overall_start_time = datetime.now()
    print(f"Integration test started at: {overall_start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # create_iam_users
    start_time = datetime.now()
    print(f"Starting create_iam_users at: {start_time.strftime('%H:%M:%S')}")
    create_iam_users()
    end_time = datetime.now()
    print(f"Completed create_iam_users at: {end_time.strftime('%H:%M:%S')} (Duration: {end_time - start_time})\n")
    
    # setup_redshift_users_and_roles
    start_time = datetime.now()
    print(f"Starting setup_redshift_users_and_roles at: {start_time.strftime('%H:%M:%S')}")
    setup_redshift_users_and_roles()
    end_time = datetime.now()
    print(f"Completed setup_redshift_users_and_roles at: {end_time.strftime('%H:%M:%S')} (Duration: {end_time - start_time})\n")
    
    # setup_idc_users_and_groups
    start_time = datetime.now()
    print(f"Starting setup_idc_users_and_groups at: {start_time.strftime('%H:%M:%S')}")
    setup_idc_users_and_groups()
    end_time = datetime.now()
    print(f"Completed setup_idc_users_and_groups at: {end_time.strftime('%H:%M:%S')} (Duration: {end_time - start_time})\n")
    
    # create_awsidc_roles
    start_time = datetime.now()
    print(f"Starting create_awsidc_roles at: {start_time.strftime('%H:%M:%S')}")
    create_awsidc_roles()
    end_time = datetime.now()
    print(f"Completed create_awsidc_roles at: {end_time.strftime('%H:%M:%S')} (Duration: {end_time - start_time})\n")
    
    # execute_ds_privs_to_lf
    start_time = datetime.now()
    print(f"Starting execute_ds_privs_to_lf at: {start_time.strftime('%H:%M:%S')}")
    execute_ds_privs_to_lf()
    end_time = datetime.now()
    print(f"Completed execute_ds_privs_to_lf at: {end_time.strftime('%H:%M:%S')} (Duration: {end_time - start_time})\n")
    
    # generate_unified_cleanup_script
    start_time = datetime.now()
    print(f"Starting generate_unified_cleanup_script at: {start_time.strftime('%H:%M:%S')}")
    generate_unified_cleanup_script()
    end_time = datetime.now()
    print(f"Completed generate_unified_cleanup_script at: {end_time.strftime('%H:%M:%S')} (Duration: {end_time - start_time})\n")
    
    final_end_time = datetime.now()
    total_elapsed = final_end_time - overall_start_time
    print(f"Integration test completed at: {final_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total elapsed time: {total_elapsed}")
    print(f"Total elapsed time (seconds): {total_elapsed.total_seconds():.2f}s")