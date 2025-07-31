import configparser
import psycopg2
import os
import boto3
import json
import subprocess
import sys
from datetime import datetime

def get_redshift_credentials(config):
    region = config['aws']['region']
    profile = config['aws'].get('config_profile', None)
    secret_name = config['redshift']['secret_name']
    
    session = boto3.Session(profile_name=profile, region_name=region) if profile else boto3.Session(region_name=region)
    secrets_client = session.client('secretsmanager')
    
    response = secrets_client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    
    return {
        'host': config['redshift']['host'],
        'port': config['redshift']['port'],
        'dbname': config['redshift']['dbname'],
        'user': secret['username'],
        'password': secret['password']
    }

def create_schemas_and_tables():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'test_perf_config.ini')
    config.read(config_path)
    
    conn_params = get_redshift_credentials(config)
    number_of_schemas = config.getint('parameters', 'number_of_schemas')
    number_of_tables_per_schema = config.getint('parameters', 'number_of_tables_per_schema')
    
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    cursor = conn.cursor()
    
    print(f"Creating {number_of_schemas} schemas with {number_of_tables_per_schema} tables each...")
    
    for schema_num in range(1, number_of_schemas + 1):
        schema_name = f"test_schema_{schema_num}"
        
        # Create schema
        cursor.execute(f"CREATE SCHEMA {schema_name};")
        print(f"Created schema: {schema_name}")
        
        # Create tables in schema
        for table_num in range(1, number_of_tables_per_schema + 1):
            table_name = f"test_schema_{schema_num}_table_{table_num}"
            cursor.execute(f"""
                CREATE TABLE {schema_name}.{table_name} (
                    id_col INTEGER,
                    desc_col VARCHAR(100)
                );
            """)
            print(f"Created table: {schema_name}.{table_name}")
    
    conn.close()
    total_schemas = number_of_schemas
    total_tables = number_of_schemas * number_of_tables_per_schema
    print(f"Schema and table creation completed! Created {total_schemas} schemas and {total_tables} tables.")

def create_idc_users_and_groups():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'test_perf_config.ini')
    config.read(config_path)
    
    identity_store_id = config['aws']['identity_store_id']
    region = config['aws']['region']
    profile = config['aws'].get('config_profile', None)
    number_of_schemas = config.getint('parameters', 'number_of_schemas')
    list_types = [item.strip() for item in config['parameters']['list_types'].split(',')]
    number_users_per_type = config.getint('parameters', 'number_users_per_type')
    
    session = boto3.Session(profile_name=profile, region_name=region) if profile else boto3.Session(region_name=region)
    identity_store_client = session.client('identitystore')
    
    print(f"Creating IAM Identity Center users and groups for {number_of_schemas} schemas...")
    
    for schema_num in range(1, number_of_schemas + 1):
        # Create groups for each type
        for user_type in list_types:
            group_name = f"test_schema_{schema_num}_group_{user_type}"
            try:
                identity_store_client.create_group(
                    IdentityStoreId=identity_store_id,
                    DisplayName=group_name,
                    Description=f"Group for {user_type} users in schema {schema_num}"
                )
                print(f"Created group: {group_name}")
            except Exception as e:
                print(f"Error creating group {group_name}: {e}")
        
        # Create users for each type
        for user_type in list_types:
            for user_num in range(1, number_users_per_type + 1):
                username = f"test_schema_{schema_num}_user_{user_type}_{user_num}"
                try:
                    response = identity_store_client.create_user(
                        IdentityStoreId=identity_store_id,
                        UserName=username,
                        Name={
                            'GivenName': f"{user_type}{user_num}",
                            'FamilyName': f"Schema{schema_num}"
                        },
                        DisplayName=username,
                        Emails=[{
                            'Value': f"{username}@example.com",
                            'Type': 'work',
                            'Primary': True
                        }]
                    )
                    user_id = response['UserId']
                    print(f"Created user: {username}")
                    
                    # Assign user to group
                    group_name = f"test_schema_{schema_num}_group_{user_type}"
                    # Find group ID
                    groups_response = identity_store_client.list_groups(
                        IdentityStoreId=identity_store_id,
                        Filters=[{
                            'AttributePath': 'DisplayName',
                            'AttributeValue': group_name
                        }]
                    )
                    
                    if groups_response.get('Groups'):
                        group_id = groups_response['Groups'][0]['GroupId']
                        identity_store_client.create_group_membership(
                            IdentityStoreId=identity_store_id,
                            GroupId=group_id,
                            MemberId={'UserId': user_id}
                        )
                        print(f"Assigned user {username} to group {group_name}")
                        
                except Exception as e:
                    print(f"Error creating user {username}: {e}")
    
    total_users = number_of_schemas * len(list_types) * number_users_per_type
    total_groups = number_of_schemas * len(list_types)
    print(f"IAM Identity Center setup completed! Created {total_users} users and {total_groups} groups.")

def create_redshift_roles():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'test_perf_config.ini')
    config.read(config_path)
    
    conn_params = get_redshift_credentials(config)
    number_of_schemas = config.getint('parameters', 'number_of_schemas')
    list_types = [item.strip() for item in config['parameters']['list_types'].split(',')]
    iam_idc_namespace = config['aws']['iam_idc_namespace']
    
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    cursor = conn.cursor()
    
    print(f"Creating Redshift roles and granting permissions for {number_of_schemas} schemas...")
    
    for schema_num in range(1, number_of_schemas + 1):
        schema_name = f"test_schema_{schema_num}"
        
        for user_type in list_types:
            role_name = f"{iam_idc_namespace}:test_schema_{schema_num}_group_{user_type}"
            
            # Create role
            try:
                cursor.execute(f'CREATE ROLE "{role_name}";')
                print(f"Created role: {role_name}")
            except Exception as e:
                print(f"Error creating role {role_name}: {e}")
                continue
            
            # Grant permissions based on type
            try:
                if user_type == 'etl':
                    cursor.execute(f'GRANT USAGE ON SCHEMA {schema_name} TO ROLE "{role_name}";')
                    cursor.execute(f'GRANT INSERT, UPDATE, DELETE, SELECT ON ALL TABLES IN SCHEMA {schema_name} TO ROLE "{role_name}";')
                    print(f"Granted ETL permissions to {role_name}")
                elif user_type == 'admin':
                    cursor.execute(f'GRANT ALL ON SCHEMA {schema_name} TO ROLE "{role_name}";')
                    cursor.execute(f'GRANT ALL ON ALL TABLES IN SCHEMA {schema_name} TO ROLE "{role_name}";')
                    print(f"Granted ADMIN permissions to {role_name}")
                else:
                    cursor.execute(f'GRANT USAGE ON SCHEMA {schema_name} TO ROLE "{role_name}";')
                    cursor.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema_name} TO ROLE "{role_name}";')
                    print(f"Granted SELECT permissions to {role_name}")
            except Exception as e:
                print(f"Error granting permissions to {role_name}: {e}")
    
    conn.close()
    total_roles = number_of_schemas * len(list_types)
    print(f"Redshift roles and permissions setup completed! Created {total_roles} roles.")

def execute_rs_privs_to_lf():
    config_path = os.path.join(os.path.dirname(__file__), 'test_perf_config.ini')
    rs_script_path = os.path.join(os.path.dirname(__file__), '../../src', 'rs_privs_to_lf.py')
    
    try:
        print("\n=== EXECUTING RS_PRIVS_TO_LF SCRIPT ===")
        result = subprocess.run(
            [sys.executable, rs_script_path, '--config', config_path],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(rs_script_path)
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

def generate_cleanup_script():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'test_perf_config.ini')
    config.read(config_path)
    
    number_of_schemas = config.getint('parameters', 'number_of_schemas')
    
    cleanup_dir = os.path.dirname(__file__)
    cleanup_file = os.path.join(cleanup_dir, 'perf_unified_cleanup.py')
    
    list_types = [item.strip() for item in config['parameters']['list_types'].split(',')]
    number_users_per_type = config.getint('parameters', 'number_users_per_type')
    iam_idc_namespace = config['aws']['iam_idc_namespace']
    
    cleanup_content = f"""import boto3
import psycopg2
import configparser
import os
import json

def get_redshift_credentials(config):
    region = config['aws']['region']
    profile = config['aws'].get('config_profile', None)
    secret_name = config['redshift']['secret_name']
    
    session = boto3.Session(profile_name=profile, region_name=region) if profile else boto3.Session(region_name=region)
    secrets_client = session.client('secretsmanager')
    
    response = secrets_client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    
    return {{
        'host': config['redshift']['host'],
        'port': config['redshift']['port'],
        'dbname': config['redshift']['dbname'],
        'user': secret['username'],
        'password': secret['password']
    }}

def cleanup_all():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'test_perf_config.ini')
    config.read(config_path)
    
    print("Starting performance test cleanup...\\n")
    
    conn_params = get_redshift_credentials(config)
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Drop Redshift roles
    print("Dropping Redshift roles...")
    list_types = [item.strip() for item in config['parameters']['list_types'].split(',')]
    iam_idc_namespace = config['aws']['iam_idc_namespace']
    
    for schema_num in range(1, {number_of_schemas} + 1):
        for user_type in list_types:
            role_name = f"{{iam_idc_namespace}}:test_schema_{{schema_num}}_group_{{user_type}}"
            try:
                cursor.execute(f'DROP ROLE "{{role_name}}" FORCE;')
                print(f"Dropped role: {{role_name}}")
            except Exception as e:
                print(f"Error dropping role {{role_name}}: {{e}}")
    
    # Drop schemas (CASCADE will drop all tables)
    print("\\nDropping schemas...")
    for schema_num in range(1, {number_of_schemas} + 1):
        schema_name = f"test_schema_{{schema_num}}"
        try:
            cursor.execute(f"DROP SCHEMA {{schema_name}} CASCADE;")
            print(f"Dropped schema: {{schema_name}}")
        except Exception as e:
            print(f"Error dropping schema {{schema_name}}: {{e}}")
    
    conn.close()
    
    # Cleanup IAM Identity Center users and groups
    print("\\n=== IAM IDENTITY CENTER CLEANUP ===")
    identity_store_id = config['aws']['identity_store_id']
    region = config['aws']['region']
    profile = config['aws'].get('config_profile', None)
    list_types = [item.strip() for item in config['parameters']['list_types'].split(',')]
    number_users_per_type = config.getint('parameters', 'number_users_per_type')
    
    session = boto3.Session(profile_name=profile, region_name=region) if profile else boto3.Session(region_name=region)
    identity_store_client = session.client('identitystore')
    
    for schema_num in range(1, {number_of_schemas} + 1):
        # Delete users and group memberships
        for user_type in list_types:
            group_name = f"test_schema_{{schema_num}}_group_{{user_type}}"
            
            # Find and delete group
            try:
                groups_response = identity_store_client.list_groups(
                    IdentityStoreId=identity_store_id,
                    Filters=[{{
                        'AttributePath': 'DisplayName',
                        'AttributeValue': group_name
                    }}]
                )
                
                for group in groups_response.get('Groups', []):
                    group_id = group['GroupId']
                    
                    # Delete group memberships
                    memberships = identity_store_client.list_group_memberships(
                        IdentityStoreId=identity_store_id,
                        GroupId=group_id
                    )
                    
                    for membership in memberships.get('GroupMemberships', []):
                        identity_store_client.delete_group_membership(
                            IdentityStoreId=identity_store_id,
                            MembershipId=membership['MembershipId']
                        )
                    
                    # Delete group
                    identity_store_client.delete_group(
                        IdentityStoreId=identity_store_id,
                        GroupId=group_id
                    )
                    print(f"Deleted group: {{group_name}}")
            except Exception as e:
                print(f"Error deleting group {{group_name}}: {{e}}")
            
            # Delete users
            for user_num in range(1, number_users_per_type + 1):
                username = f"test_schema_{{schema_num}}_user_{{user_type}}_{{user_num}}"
                try:
                    users_response = identity_store_client.list_users(
                        IdentityStoreId=identity_store_id,
                        Filters=[{{
                            'AttributePath': 'UserName',
                            'AttributeValue': username
                        }}]
                    )
                    
                    for user in users_response.get('Users', []):
                        identity_store_client.delete_user(
                            IdentityStoreId=identity_store_id,
                            UserId=user['UserId']
                        )
                        print(f"Deleted user: {{username}}")
                except Exception as e:
                    print(f"Error deleting user {{username}}: {{e}}")
    
    print("\\nPerformance test cleanup completed!")

if __name__ == "__main__":
    cleanup_all()
"""
    
    with open(cleanup_file, 'w') as f:
        f.write(cleanup_content)
    
    print(f"Cleanup script generated at: {cleanup_file}")

if __name__ == "__main__":
    overall_start_time = datetime.now()
    print(f"Performance test started at: {overall_start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # create_schemas_and_tables
    start_time = datetime.now()
    print(f"Starting create_schemas_and_tables at: {start_time.strftime('%H:%M:%S')}")
    create_schemas_and_tables()
    end_time = datetime.now()
    print(f"Completed create_schemas_and_tables at: {end_time.strftime('%H:%M:%S')} (Duration: {end_time - start_time})\n")
    
    # create_idc_users_and_groups
    start_time = datetime.now()
    print(f"Starting create_idc_users_and_groups at: {start_time.strftime('%H:%M:%S')}")
    create_idc_users_and_groups()
    end_time = datetime.now()
    print(f"Completed create_idc_users_and_groups at: {end_time.strftime('%H:%M:%S')} (Duration: {end_time - start_time})\n")
    
    # create_redshift_roles
    start_time = datetime.now()
    print(f"Starting create_redshift_roles at: {start_time.strftime('%H:%M:%S')}")
    create_redshift_roles()
    end_time = datetime.now()
    print(f"Completed create_redshift_roles at: {end_time.strftime('%H:%M:%S')} (Duration: {end_time - start_time})\n")
    
    # execute_rs_privs_to_lf
    start_time = datetime.now()
    print(f"Starting execute_rs_privs_to_lf at: {start_time.strftime('%H:%M:%S')}")
    execute_rs_privs_to_lf()
    end_time = datetime.now()
    print(f"Completed execute_rs_privs_to_lf at: {end_time.strftime('%H:%M:%S')} (Duration: {end_time - start_time})\n")
    
    # generate_cleanup_script
    start_time = datetime.now()
    print(f"Starting generate_cleanup_script at: {start_time.strftime('%H:%M:%S')}")
    generate_cleanup_script()
    end_time = datetime.now()
    print(f"Completed generate_cleanup_script at: {end_time.strftime('%H:%M:%S')} (Duration: {end_time - start_time})\n")
    
    final_end_time = datetime.now()
    total_elapsed = final_end_time - overall_start_time
    print(f"Performance test completed at: {final_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total elapsed time: {total_elapsed}")
    print(f"Total elapsed time (seconds): {total_elapsed.total_seconds():.2f}s")