import boto3
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
    
    return {
        'host': config['redshift']['host'],
        'port': config['redshift']['port'],
        'dbname': config['redshift']['dbname'],
        'user': secret['username'],
        'password': secret['password']
    }

def cleanup_all():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'test_perf_config.ini')
    config.read(config_path)
    
    print("Starting performance test cleanup...\n")
    
    conn_params = get_redshift_credentials(config)
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Drop Redshift roles
    print("Dropping Redshift roles...")
    list_types = [item.strip() for item in config['parameters']['list_types'].split(',')]
    iam_idc_namespace = config['aws']['iam_idc_namespace']
    
    for schema_num in range(1, 5 + 1):
        for user_type in list_types:
            role_name = f"{iam_idc_namespace}:test_schema_{schema_num}_group_{user_type}"
            try:
                cursor.execute(f'DROP ROLE "{role_name}" FORCE;')
                print(f"Dropped role: {role_name}")
            except Exception as e:
                print(f"Error dropping role {role_name}: {e}")
    
    # Drop schemas (CASCADE will drop all tables)
    print("\nDropping schemas...")
    for schema_num in range(1, 5 + 1):
        schema_name = f"test_schema_{schema_num}"
        try:
            cursor.execute(f"DROP SCHEMA {schema_name} CASCADE;")
            print(f"Dropped schema: {schema_name}")
        except Exception as e:
            print(f"Error dropping schema {schema_name}: {e}")
    
    conn.close()
    
    # Cleanup IAM Identity Center users and groups
    print("\n=== IAM IDENTITY CENTER CLEANUP ===")
    identity_store_id = config['aws']['identity_store_id']
    region = config['aws']['region']
    profile = config['aws'].get('config_profile', None)
    list_types = [item.strip() for item in config['parameters']['list_types'].split(',')]
    number_users_per_type = config.getint('parameters', 'number_users_per_type')
    
    session = boto3.Session(profile_name=profile, region_name=region) if profile else boto3.Session(region_name=region)
    identity_store_client = session.client('identitystore')
    
    for schema_num in range(1, 5 + 1):
        # Delete users and group memberships
        for user_type in list_types:
            group_name = f"test_schema_{schema_num}_group_{user_type}"
            
            # Find and delete group
            try:
                groups_response = identity_store_client.list_groups(
                    IdentityStoreId=identity_store_id,
                    Filters=[{
                        'AttributePath': 'DisplayName',
                        'AttributeValue': group_name
                    }]
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
                    print(f"Deleted group: {group_name}")
            except Exception as e:
                print(f"Error deleting group {group_name}: {e}")
            
            # Delete users
            for user_num in range(1, number_users_per_type + 1):
                username = f"test_schema_{schema_num}_user_{user_type}_{user_num}"
                try:
                    users_response = identity_store_client.list_users(
                        IdentityStoreId=identity_store_id,
                        Filters=[{
                            'AttributePath': 'UserName',
                            'AttributeValue': username
                        }]
                    )
                    
                    for user in users_response.get('Users', []):
                        identity_store_client.delete_user(
                            IdentityStoreId=identity_store_id,
                            UserId=user['UserId']
                        )
                        print(f"Deleted user: {username}")
                except Exception as e:
                    print(f"Error deleting user {username}: {e}")
    
    print("\nPerformance test cleanup completed!")

if __name__ == "__main__":
    cleanup_all()
