
import boto3
import csv
import io
import logging
import configparser

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_configuration(config_path='idc_config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)

    required_aws_params = ['region', 'account_id', 'identity_store_id', 'instance_arn', 'permission_set_arn']
    required_s3_params = ['s3_bucket', 'users_file', 'roles_file', 'role_memberships_file']

    # Check AWS configuration
    if 'AWS' not in config:
        raise ValueError("Missing AWS section in the configuration file.")
    for param in required_aws_params:
        if not config.get('AWS', param):
            raise ValueError(f"Missing or empty AWS configuration parameter: {param}")

    # Check S3 configuration
    if 'S3' not in config:
        raise ValueError("Missing S3 section in the configuration file.")
    for param in required_s3_params:
        if not config.get('S3', param):
            raise ValueError(f"Missing or empty S3 configuration parameter: {param}")
            
    # Check GROUP_MANAGEMENT section and assign_permission_set parameter
    if 'GROUP_MANAGEMENT' not in config:
        logger.warning("Missing GROUP_MANAGEMENT section in the configuration file. Defaulting 'assign_permission_set' to True.")
        config.add_section('GROUP_MANAGEMENT')
        config.set('GROUP_MANAGEMENT', 'assign_permission_set', 'True')
    elif not config.get('GROUP_MANAGEMENT', 'assign_permission_set'):
        logger.warning("Missing or empty 'assign_permission_set' parameter in GROUP_MANAGEMENT section. Defaulting to True.")
        config.set('GROUP_MANAGEMENT', 'assign_permission_set', 'True')


    return config

def read_csv_from_s3(s3_client, bucket, file_key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=file_key)
        csv_content = response["Body"].read().decode("utf-8")
        csv_reader = csv.reader(io.StringIO(csv_content))
        data = list(csv_reader)
        return data
    except Exception as e:
        logger.error(f"Error reading {file_key} from S3: {e}")
        return None

def get_user_id(identity_store_client, identity_store_id, user_name):
    """Get user ID from username if exists"""
    try:
        # List users with filter to find by username
        response = identity_store_client.list_users(
            IdentityStoreId=identity_store_id,
            Filters=[
                {
                    'AttributePath': 'UserName',
                    'AttributeValue': user_name
                }
            ]
        )
        
        for user in response.get('Users', []):
            if user.get('UserName') == user_name:
                logger.info(f"Found existing user: {user_name} with ID: {user['UserId']}")
                return user['UserId']
                
        return None
    except Exception as e:
        logger.error(f"Error searching for user {user_name}: {e}")
        return None

def create_user(identity_store_client, identity_store_id, user_name):
    # Check if the user already exists
    user_id = get_user_id(identity_store_client, identity_store_id, user_name)
    
    if user_id:
        logger.info(f"User already exists: {user_name}")
        return user_id
    
    # User doesn't exist, create it
    try:
        response = identity_store_client.create_user(
            IdentityStoreId=identity_store_id,
            UserName=user_name,
            DisplayName=user_name,
            Name={'GivenName': user_name, 'FamilyName': 'from Redshift'},
            Emails=[{"Value": f"{user_name}@example.com", "Type": "work"}]
        )
        logger.info(f"User created: {user_name}")
        return response["UserId"]
    except Exception as e:
        logger.error(f"Error creating user {user_name}: {e}")
        raise

def get_group_id(identity_store_client, identity_store_id, group_name):
    """Get group ID from group name if exists"""
    try:
        # List groups with filter to find by display name
        response = identity_store_client.list_groups(
            IdentityStoreId=identity_store_id,
            Filters=[
                {
                    'AttributePath': 'DisplayName',
                    'AttributeValue': group_name
                }
            ]
        )
        
        for group in response.get('Groups', []):
            if group.get('DisplayName') == group_name:
                logger.info(f"Found existing group: {group_name} with ID: {group['GroupId']}")
                return group['GroupId']
                
        return None
    except Exception as e:
        logger.error(f"Error searching for group {group_name}: {e}")
        return None

def create_group(identity_store_client, sso_admin_client, identity_store_id, role_name, assign_permission_set=True, **additional_config):
    # Check if the group already exists
    group_id = get_group_id(identity_store_client, identity_store_id, role_name)
    
    if group_id:
        logger.info(f"Group already exists: {role_name}")
    else:
        # Group doesn't exist, create it
        try:
            response = identity_store_client.create_group(
                IdentityStoreId=identity_store_id,
                DisplayName=role_name
            )
            logger.info(f"Group created: {role_name}")
            group_id = response["GroupId"]
        except Exception as e:
            logger.error(f"Error creating group {role_name}: {e}")
            raise
    
    # Assign permission set if requested (for both new and existing groups)
    if assign_permission_set:
        try:
            sso_admin_client.create_account_assignment(
                InstanceArn=additional_config['instance_arn'],
                TargetId=additional_config['account_id'],
                TargetType='AWS_ACCOUNT',
                PermissionSetArn=additional_config['permission_set_arn'],
                PrincipalType='GROUP',
                PrincipalId=group_id
            )
            logger.info(f"Permission set assigned to group: {role_name}")
        except Exception as e:
            if "ConflictException" in str(e):
                logger.info(f"Permission set already assigned to group {role_name}")
            else:
                logger.error(f"Error assigning permission set to group {group_id}: {e}")
    
    return group_id

def add_user_to_group(identity_store_client, identity_store_id, user_id, group_id):
    try:
        identity_store_client.create_group_membership(
            IdentityStoreId=identity_store_id,
            GroupId=group_id,
            MemberId={'UserId': user_id}
        )
        logger.info(f"Added user {user_id} to group {group_id}")
    except Exception as e:
        if "ConflictException" in str(e):
            logger.info(f"User {user_id} is already a member of group {group_id}")
        else:
            logger.error(f"Error adding user {user_id} to group {group_id}: {e}")
            raise

def process_roles(config, aws_clients, assign_permission_set=True):
    s3_bucket = config.get('S3', 's3_bucket')
    roles_file = config.get('S3', 'roles_file')
    identity_store_id = config.get('AWS', 'identity_store_id')

    roles_data = read_csv_from_s3(aws_clients['s3'], s3_bucket, roles_file)
    if roles_data is None:
        logger.error("Could not read roles data. Exiting process_roles.")
        return {}

    role_ids = {}

    for row in roles_data[1:]:  # Skip header
        role_name = row[0]
        role_id = create_group(aws_clients['identity_store'], aws_clients['sso_admin'], identity_store_id, role_name, assign_permission_set=assign_permission_set, **{
            'instance_arn': config.get('AWS', 'instance_arn'),
            'account_id': config.get('AWS', 'account_id'),
            'permission_set_arn': config.get('AWS', 'permission_set_arn')
        })
        role_ids[role_name] = role_id

    return role_ids

def process_users(config, aws_clients):
    s3_bucket = config.get('S3', 's3_bucket')
    users_file = config.get('S3', 'users_file')
    identity_store_id = config.get('AWS', 'identity_store_id')

    users_data = read_csv_from_s3(aws_clients['s3'], s3_bucket, users_file)
    if users_data is None:
        logger.error("Could not read users data. Exiting process_users.")
        return {}
        
    user_ids = {}

    for row in users_data[1:]:  # Skip header
        user_name = row[0]
        user_id = create_user(aws_clients['identity_store'], identity_store_id, user_name)
        user_ids[user_name] = user_id

    return user_ids

def process_role_memberships(config, aws_clients, user_ids, role_ids):
    s3_bucket = config.get('S3', 's3_bucket')
    role_memberships_file = config.get('S3', 'role_memberships_file')
    identity_store_id = config.get('AWS', 'identity_store_id')

    role_memberships_data = read_csv_from_s3(aws_clients['s3'], s3_bucket, role_memberships_file)
    if role_memberships_data is None:
        logger.error("Could not read role memberships data. Exiting process_role_memberships.")
        return

    for row in role_memberships_data[1:]:  # Skip header
        user_name, role_name = row
        user_id = user_ids.get(user_name)
        role_id = role_ids.get(role_name)
        
        if not user_id:
            logger.error(f"User ID not found for user {user_name}")
            continue
        
        if not role_id:
            logger.error(f"Role ID not found for role {role_name}")
            continue
            
        add_user_to_group(aws_clients['identity_store'], identity_store_id, user_id, role_id)

def main():
    try:
        config = load_configuration()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return

    aws_clients = {
        's3': boto3.client('s3', region_name=config.get('AWS', 'region')),
        'identity_store': boto3.client('identitystore', region_name=config.get('AWS', 'region')),
        'sso_admin': boto3.client('sso-admin', region_name=config.get('AWS', 'region'))
    }

    assign_permission_set = config.getboolean('GROUP_MANAGEMENT', 'assign_permission_set', fallback=True)

    user_ids = process_users(config, aws_clients)
    role_ids = process_roles(config, aws_clients, assign_permission_set=assign_permission_set)
    process_role_memberships(config, aws_clients, user_ids, role_ids)

if __name__ == "__main__":
    main()

