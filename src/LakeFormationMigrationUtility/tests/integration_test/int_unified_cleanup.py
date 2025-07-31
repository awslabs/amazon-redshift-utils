import boto3
import psycopg2
import configparser
import os
import json


def cleanup_all():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'test_int_config.ini')
    config.read(config_path)

    print("Starting unified cleanup process...\n")
    
    # ===== REDSHIFT CLEANUP =====
    print("=== REDSHIFT CLEANUP ===")
    try:

        # Get credentials from Secrets Manager
        secret_name = config['redshift']['secret_name']
        region = config['aws']['region']
        profile = config['aws'].get('config_profile', None)
        
        # Create boto3 session and clients
        session = boto3.Session(profile_name=profile, region_name=region) if profile else boto3.Session(region_name=region)
        secrets_client = session.client('secretsmanager')
        
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

        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Get datashare database name
        datashare_db = config['parameters']['datashare_database_name']
        object_level_permissions = config.getboolean('parameters', 'object_level_permissions')
        
        # Revoke permissions from test_user_analyst3 and AWSIDC:test_user_analyst3@example.com
        print("Revoking permissions from test_user_analyst3 and AWSIDC:test_user_analyst3@example.com...")
        try:
            # Revoke permissions from test_user_analyst3
            cursor.execute(f"REVOKE USAGE ON DATABASE {datashare_db} FROM test_user_analyst3;")
            if object_level_permissions:
                cursor.execute(f"REVOKE USAGE ON SCHEMA {datashare_db}.tickit FROM test_user_analyst3;")
                cursor.execute(f"REVOKE ALL ON ALL TABLES IN SCHEMA {datashare_db}.tickit FROM test_user_analyst3;")
            print("Revoked permissions from test_user_analyst3")
            
            # Revoke permissions from AWSIDC:test_user_analyst3@example.com
            cursor.execute(f'REVOKE USAGE ON DATABASE {datashare_db} FROM "AWSIDC:test_user_analyst3@example.com";')
            if object_level_permissions:
                cursor.execute(f'REVOKE USAGE ON SCHEMA {datashare_db}.tickit FROM "AWSIDC:test_user_analyst3@example.com";')
                cursor.execute(f'REVOKE ALL ON ALL TABLES IN SCHEMA {datashare_db}.tickit FROM "AWSIDC:test_user_analyst3@example.com";')
            print("Revoked permissions from AWSIDC:test_user_analyst3@example.com")
        except Exception as e:
            print(f"Error revoking permissions: {e}")
        
        # Revoke role relationships
        print("\nRevoking role relationships...")
        try:
            cursor.execute("REVOKE ROLE test_role_tn_tickit_restricted FROM ROLE test_role_bn_etl;")
            print("Revoked test_role_tn_tickit_restricted from test_role_bn_etl")
            cursor.execute('REVOKE ROLE test_role_tn_tickit_restricted FROM ROLE "AWSIDC:test_role_bn_etl";')
            print("Revoked test_role_tn_tickit_restricted from AWSIDC:test_role_bn_etl")
        except Exception as e:
            print(f"Error revoking roles: {e}")
        
        # Drop users
        print("\nDropping users...")
        users = ['test_user_admin', 'test_user_analyst1', 'test_user_analyst2', 'test_user_analyst3', 'test_user_etl1', 'test_user_etl2', 'test_user_adhoc']
        for user in users:
            try:
                cursor.execute(f"DROP USER IF EXISTS {user};")
                print(f"Dropped user: {user}")
            except Exception as e:
                print(f"Error dropping user {user}: {e}")
        
        # Drop AWSIDC users first
        print("\nDropping AWSIDC users...")
        awsidc_users = ['AWSIDC:test_user_analyst3@example.com']
        for user in awsidc_users:
            try:
                cursor.execute(f'DROP USER "{user}";')
                print(f"Dropped AWSIDC user: {user}")
            except Exception as e:
                print(f"Error dropping AWSIDC user {user}: {e}")
                
        # Drop AWSIDC roles
        print("\nDropping AWSIDC roles...")
        awsidc_roles = ['AWSIDC:test_role_bn_admin', 'AWSIDC:test_role_bn_analyst', 'AWSIDC:test_role_bn_etl', 'AWSIDC:test_role_bn_adhoc']
        for role in awsidc_roles:
            try:
                cursor.execute(f'DROP ROLE "{role}" FORCE;')
                print(f"Dropped AWSIDC role: {role}")
            except Exception as e:
                print(f"Error dropping AWSIDC role {role}: {e}")
        
        # Drop local roles
        print("\nDropping local roles...")
        roles = ['test_role_bn_admin', 'test_role_bn_analyst', 'test_role_bn_etl', 'test_role_tn_tickit_restricted', 'test_role_bn_adhoc']
        for role in roles:
            try:
                cursor.execute(f"DROP ROLE {role} FORCE;")
                print(f"Dropped role: {role}")
            except Exception as e:
                print(f"Error dropping role {role}: {e}")
                
        print("Redshift cleanup completed successfully!")
    except Exception as e:
        print(f"Error in Redshift cleanup: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
    
    # ===== IAM IDENTITY CENTER CLEANUP =====
    print("\n=== IAM IDENTITY CENTER CLEANUP ===")
    try:
        # Get IAM Identity Center configuration
        identity_store_id = config['aws']['identity_store_id']
        region = config['aws']['region']
        profile = config['aws'].get('config_profile', None)
        
        # Create boto3 session and clients
        session = boto3.Session(profile_name=profile, region_name=region) if profile else boto3.Session(region_name=region)
        identity_store_client = session.client('identitystore')
        
        # List of users and groups to delete
        users = ['test_user_admin', 'test_user_analyst1', 'test_user_analyst2', 'test_user_analyst3', 'test_user_etl1', 'test_user_etl2', 'test_user_adhoc']
        groups = ['test_role_bn_admin', 'test_role_bn_analyst', 'test_role_bn_etl', 'test_role_bn_adhoc']
        
        # Find and delete group memberships and groups
        print("Processing groups...")
        for group_name in groups:
            try:
                # List groups to find the ID
                response = identity_store_client.list_groups(
                    IdentityStoreId=identity_store_id,
                    Filters=[{
                        'AttributePath': 'DisplayName',
                        'AttributeValue': group_name
                    }]
                )
                
                for group in response.get('Groups', []):
                    group_id = group['GroupId']
                    print(f"Found group {group_name} with ID: {group_id}")
                    
                    # List group memberships
                    memberships = identity_store_client.list_group_memberships(
                        IdentityStoreId=identity_store_id,
                        GroupId=group_id
                    )
                    
                    # Delete each membership
                    for membership in memberships.get('GroupMemberships', []):
                        identity_store_client.delete_group_membership(
                            IdentityStoreId=identity_store_id,
                            MembershipId=membership['MembershipId']
                        )
                        print(f"Deleted membership {membership['MembershipId']} from group {group_name}")
                    
                    # Delete the group
                    identity_store_client.delete_group(
                        IdentityStoreId=identity_store_id,
                        GroupId=group_id
                    )
                    print(f"Deleted group {group_name}")
            except Exception as e:
                print(f"Error processing group {group_name}: {e}")
        
        # Find and delete users
        print("\nProcessing users...")
        for username in users:
            try:
                # List users to find the ID
                response = identity_store_client.list_users(
                    IdentityStoreId=identity_store_id,
                    Filters=[{
                        'AttributePath': 'UserName',
                        'AttributeValue': username
                    }]
                )
                
                for user in response.get('Users', []):
                    user_id = user['UserId']
                    print(f"Found user {username} with ID: {user_id}")
                    
                    # Delete the user
                    identity_store_client.delete_user(
                        IdentityStoreId=identity_store_id,
                        UserId=user_id
                    )
                    print(f"Deleted user {username}")
            except Exception as e:
                print(f"Error processing user {username}: {e}")
                
        print("IAM Identity Center cleanup completed successfully!")
    except Exception as e:
        print(f"Error in IAM Identity Center cleanup: {e}")
    
    print("\nUnified cleanup process completed!")

if __name__ == "__main__":
    cleanup_all()
