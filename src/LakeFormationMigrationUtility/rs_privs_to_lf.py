import os
import sys
import psycopg2
import argparse
import configparser
import subprocess
import json
import boto3
import logging
from datetime import datetime

__version__ = "1.0"

def setup_logging(debug=False, output_dir=None):
    """
    Setup logging configuration
    """
    # Create logs directory
    if output_dir:
        log_dir = os.path.join(output_dir, 'logs')
    else:
        log_dir = 'logs'
    
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'lakeformation_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    # Set logging level
    level = logging.DEBUG if debug else logging.INFO
    
    # Configure logging
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Lake Formation Migration Utility v{__version__} started")
    logger.info(f"Log file: {log_file}")
    return logger

def load_config(config_file='config.ini'):
    """
    Load configuration from config file (default: config.ini)
    """
    logger = logging.getLogger(__name__)
    if not os.path.exists(config_file):
        logger.error(f"Config file '{config_file}' not found")
        sys.exit(1)

    config = configparser.ConfigParser()
    try:
        config.read(config_file)
        logger.info(f"Configuration loaded from {config_file}")
        return config
    except Exception as e:
        logger.error(f"Error reading config file: {e}")
        sys.exit(1)

def get_secret(secret_name, region_name=None, config_profile=None):
    """
    Retrieve a secret from AWS Secrets Manager
    """
    logger = logging.getLogger(__name__)
    try:
        logger.debug(f"Retrieving secret: {secret_name}")
        # Create a Boto3 session with the specified profile if provided
        session_kwargs = {}
        if config_profile:
            session_kwargs['profile_name'] = config_profile
        session = boto3.session.Session(**session_kwargs)
        
        # Create a Secrets Manager client
        client_kwargs = {}
        if region_name:
            client_kwargs['region_name'] = region_name
        client = session.client('secretsmanager', **client_kwargs)
        
        # Get the secret value
        response = client.get_secret_value(SecretId=secret_name)
        
        # Parse the secret JSON string
        if 'SecretString' in response:
            secret = json.loads(response['SecretString'])
            logger.info(f"Successfully retrieved secret: {secret_name}")
            return secret
        else:
            logger.error("Secret value is not a string")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error retrieving secret from AWS Secrets Manager: {e}")
        sys.exit(1)

def connect_to_redshift(config):
    """
    Establish connection to Redshift database using config and AWS Secrets Manager
    """
    logger = logging.getLogger(__name__)
    try:
        redshift_config = config['redshift']
        
        # Get region and profile from AWS section if available
        region_name = None
        config_profile = None
        if 'aws' in config:
            region_name = config['aws'].get('region')
            config_profile = config['aws'].get('config_profile')
        
        # Get credentials from AWS Secrets Manager
        secret_name = redshift_config.get('secret_name')
        if not secret_name:
            logger.error("'secret_name' not found in redshift configuration")
            sys.exit(1)
            
        secret = get_secret(secret_name, region_name, config_profile)
        
        # Connect to Redshift using the secret credentials
        conn = psycopg2.connect(
            host=redshift_config['host'],
            port=int(redshift_config.get('port', 5439)),
            dbname=redshift_config['dbname'],
            user=secret['username'],
            password=secret['password'],
            application_name=f'LakeFormationMigrationUtility-v{__version__}'
        )
        logger.info(f"Connected to Redshift database: {redshift_config['dbname']}")
        return conn
    except KeyError as e:
        logger.error(f"Missing required configuration parameter: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error connecting to Redshift: {e}")
        sys.exit(1)

def get_users_with_usage_permission(conn, p_datashare_database_name=None, identity_provider_namespace='AWSIDC'):
    """
    Query svv_database_privileges to get users with USAGE permission on the datashare database(s)
    Only include identity_provider_namespace: prefixed users for IDC auth
    """
    try:
        cursor = conn.cursor()

        query = """
        SELECT DISTINCT
            database_name,
            identity_name
        FROM
            svv_database_privileges
        WHERE
            privilege_type IN ('USAGE', 'ALL')
            AND 
        """

        if p_datashare_database_name:
            query += f"database_name = '{p_datashare_database_name}'"
        else:
            query += "database_name LIKE '%datashare%'"

        query += """
        ORDER BY
            identity_name
        """

        print(f"Executing query for users with USAGE permission: {query}")
        cursor.execute(query)
        db_users = cursor.fetchall()

        # Create a dictionary mapping database names to lists of users
        user_map = {}
        prefix = f"{identity_provider_namespace}:"
        
        for db_name, identity_name in db_users:
            # Only include identity_provider_namespace: prefixed users
            if identity_name.lower().startswith(prefix.lower()):
                user_map.setdefault(db_name, []).append(identity_name)

        return user_map
    except Exception as e:
        print(f"Error retrieving database users with USAGE permission: {e}")
        conn.close()
        sys.exit(1)

def get_local_database_permissions(conn, database_name, identity_provider_namespace='AWSIDC'):
    """
    Get IDC users with permissions on the local database and its tables using system tables
    Returns user_map and table_grants for local database
    """
    try:
        cursor = conn.cursor()
        
        # Debug: Check current database
        cursor.execute("SELECT current_database()")
        current_db = cursor.fetchone()[0]
        print(f"Current database: {current_db}")
        
        # Debug: Check available schemas
        cursor.execute("SELECT schema_name FROM svv_redshift_schemas WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_internal')")
        schemas = cursor.fetchall()
        print(f"Available schemas: {[s[0] for s in schemas]}")
        
        # Get all tables in the local database (exclude system schemas)
        tables_query = f"""
        SELECT schema_name, table_name
        FROM svv_redshift_tables
        WHERE database_name = '{database_name}'
        AND schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_internal')
        ORDER BY schema_name, table_name
        """
        
        print(f"Executing local tables query: {tables_query}")
        cursor.execute(tables_query)
        table_results = cursor.fetchall()
        print(f"Found {len(table_results)} tables in local database")
        for schema, table in table_results[:5]:  # Show first 5 tables
            print(f"  Table: {schema}.{table}")
        
        tables = [(database_name, schema, table) for schema, table in table_results]
        
        # Get table-level grants using system tables (includes IDC role handling)
        table_grants = get_table_grants(conn, tables, identity_provider_namespace)
        
        # Create empty user_map since we only care about table permissions
        user_map = {}
        
        cursor.close()
        return user_map, tables, table_grants
        
    except Exception as e:
        print(f"Error retrieving local database permissions: {e}")
        conn.close()
        sys.exit(1)



def get_datashare_tables(conn, p_datashare_database_name=None):
    """
    Get all tables in the datashare database(s) using svv_redshift_tables
    """
    try:
        cursor = conn.cursor()

        # Use svv_redshift_tables to get tables in datashare databases
        query = """
        SELECT
            database_name,
            schema_name,
            table_name
        FROM
            svv_redshift_tables
        WHERE
        """

        if p_datashare_database_name:
            query += f"database_name = '{p_datashare_database_name}'"
        else:
            query += "database_name LIKE '%datashare%'"

        query += """
        ORDER BY
            database_name, schema_name, table_name
        """

        print(f"Executing query for tables: {query}")
        cursor.execute(query)
        tables = cursor.fetchall()
        cursor.close()

        return tables
    except Exception as e:
        print(f"Error retrieving datashare tables: {e}")
        conn.close()
        sys.exit(1)

def get_idc_group_id(role_name, identity_store_id, config_profile=None, identity_provider_namespace='AWSIDC', region_name=None):
    """
    Get IDC group ID using AWS CLI get-group-id command with caching
    """
    # Check cache first
    if role_name in group_id_cache:
        return group_id_cache[role_name]
    
    try:
        # Remove identity provider namespace prefix if present
        prefix = f"{identity_provider_namespace}:"
        group_name = remove_prefix(role_name, prefix)
        
        # Create the JSON string for alternate-identifier
        alternate_identifier = json.dumps({"UniqueAttribute": {"AttributePath": "DisplayName", "AttributeValue": group_name}})
        
        cmd = ['aws']
        if config_profile:
            cmd.extend(['--profile', config_profile])
        if region_name:
            cmd.extend(['--region', region_name])
        cmd.extend([
            'identitystore', 'get-group-id',
            '--identity-store-id', identity_store_id,
            '--alternate-identifier', alternate_identifier
        ])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Warning: Could not get group ID for {role_name}: {result.stderr}")
            group_id_cache[role_name] = None
            return None
        
        response = json.loads(result.stdout)
        group_id = response.get('GroupId')
        if group_id:
            print(f"Found IDC group ID for {role_name}: {group_id}")
            group_id_cache[role_name] = group_id
            return group_id
        else:
            print(f"Warning: No group ID found for {role_name}")
            group_id_cache[role_name] = None
            return None
            
    except Exception as e:
        print(f"Warning: Error getting IDC group ID for {role_name}: {e}")
        group_id_cache[role_name] = None
        return None

def get_idc_role_grants(conn, identity_provider_namespace='AWSIDC'):
    """
    Get all roles granted to IDC roles using SVV_ROLE_GRANTS
    Returns a dictionary mapping IDC role names to lists of granted roles
    """
    try:
        cursor = conn.cursor()
        role_grants = {}
        
        # Create prefix for filtering
        prefix = f"{identity_provider_namespace}:%"
        
        query = f"""
        SELECT 
            role_name,
            granted_role_name
        FROM 
            svv_role_grants
        WHERE 
            role_name LIKE '{prefix}'
            AND NOT granted_role_name LIKE '{prefix}'
        """
        
        print(f"Executing query for IDC role grants: {query}")
        cursor.execute(query)
        results = cursor.fetchall()
        
        for role_name, granted_role in results:
            role_grants.setdefault(role_name, []).append(granted_role)
            print(f"  Found role {granted_role} granted to IDC role {role_name}")
        
        cursor.close()
        return role_grants
    
    except Exception as e:
        print(f"Error retrieving IDC role grants: {e}")
        return {}

def get_table_grants_for_roles(conn, tables, target_roles):
    """
    Get grants for specific roles on tables using SHOW GRANTS command
    Returns a dictionary mapping (db_name, schema_name, table_name) to a list of (grantee, privilege_type, identity_type) tuples
    Only includes the specified target roles
    """
    try:
        cursor = conn.cursor()
        table_grants = {}
        
        for db_name, schema_name, table_name in tables:
            full_table_name = f"{db_name}.{schema_name}.{table_name}"
            print(f"Getting grants for table: {full_table_name}")
            
            try:
                # Execute SHOW GRANTS command
                query = f"SHOW GRANTS ON TABLE {full_table_name}"
                cursor.execute(query)
                
                # Get column names from cursor description
                columns = [desc[0] for desc in cursor.description]
                
                # Find the indices for identity_name, privilege_type, and identity_type
                identity_name_idx = columns.index('identity_name') if 'identity_name' in columns else -1
                privilege_type_idx = columns.index('privilege_type') if 'privilege_type' in columns else -1
                identity_type_idx = columns.index('identity_type') if 'identity_type' in columns else -1
                
                if identity_name_idx == -1 or privilege_type_idx == -1 or identity_type_idx == -1:
                    print(f"Warning: Could not find required columns in SHOW GRANTS result for {full_table_name}")
                    print(f"Available columns: {columns}")
                    continue
                
                grants = cursor.fetchall()
                
                # Store results in dictionary
                table_key = (db_name, schema_name, table_name)
                table_grants[table_key] = []
                
                # Process each grant
                for grant in grants:
                    grantee = grant[identity_name_idx]
                    privilege_type = grant[privilege_type_idx]
                    identity_type = grant[identity_type_idx]
                    
                    # Only include target roles and exclude filtered permissions
                    if grantee in target_roles and privilege_type not in ['RULE', 'TRIGGER', 'REFERENCES', 'TRUNCATE', 'UPDATE']:
                        table_grants[table_key].append((grantee, privilege_type, identity_type))
                        print(f"  Found grant: {grantee} ({identity_type}) has {privilege_type} on {full_table_name}")
            
            except Exception as e:
                print(f"Warning: Could not get grants for table {full_table_name}: {e}")
                # Continue with other tables
        
        cursor.close()
        return table_grants
    
    except Exception as e:
        print(f"Error retrieving table grants for roles: {e}")
        conn.close()
        sys.exit(1)

def get_table_grants(conn, tables, identity_provider_namespace='AWSIDC'):
    """
    Get grants for each table using SHOW GRANTS command
    Returns a dictionary mapping (db_name, schema_name, table_name) to a list of (grantee, privilege_type, identity_type) tuples
    Only include identity_provider_namespace: prefixed users/roles for IDC auth
    """
    try:
        cursor = conn.cursor()
        table_grants = {}
        
        for db_name, schema_name, table_name in tables:
            full_table_name = f"{db_name}.{schema_name}.{table_name}"
            print(f"Getting grants for table: {full_table_name}")
            
            try:
                # Execute SHOW GRANTS command
                query = f"SHOW GRANTS ON TABLE {full_table_name}"
                cursor.execute(query)
                
                # Get column names from cursor description
                columns = [desc[0] for desc in cursor.description]
                
                # Find the indices for identity_name, privilege_type, and identity_type
                identity_name_idx = columns.index('identity_name') if 'identity_name' in columns else -1
                privilege_type_idx = columns.index('privilege_type') if 'privilege_type' in columns else -1
                identity_type_idx = columns.index('identity_type') if 'identity_type' in columns else -1
                
                if identity_name_idx == -1 or privilege_type_idx == -1 or identity_type_idx == -1:
                    print(f"Warning: Could not find required columns in SHOW GRANTS result for {full_table_name}")
                    print(f"Available columns: {columns}")
                    continue
                
                grants = cursor.fetchall()
                
                # Store results in dictionary
                table_key = (db_name, schema_name, table_name)
                table_grants.setdefault(table_key, [])
                
                # Define excluded permissions
                excluded_permissions = ['RULE', 'TRIGGER', 'REFERENCES', 'TRUNCATE', 'UPDATE']
                prefix = f"{identity_provider_namespace}:"
                
                # Process each grant
                for grant in grants:
                    grantee = grant[identity_name_idx]
                    privilege_type = grant[privilege_type_idx]
                    identity_type = grant[identity_type_idx]
                    
                    # Filter - exclude certain permissions and only include prefixed users/roles
                    if (grantee != 'rdsdb' and 
                        privilege_type not in excluded_permissions and 
                        grantee.lower().startswith(prefix.lower())):
                        
                        table_grants[table_key].append((grantee, privilege_type, identity_type))
                        print(f"  Found grant: {grantee} ({identity_type}) has {privilege_type} on {full_table_name}")
            
            except Exception as e:
                print(f"Warning: Could not get grants for table {full_table_name}: {e}")
                # Continue with other tables
        
        cursor.close()
        return table_grants
    
    except Exception as e:
        print(f"Error retrieving table grants: {e}")
        conn.close()
        sys.exit(1)

def get_idc_user_id(username, identity_store_id, config_profile=None, identity_provider_namespace='AWSIDC', region_name=None):
    """
    Get IDC user ID using AWS CLI get-user-id command with the correct syntax
    Uses email address as the identifier
    """
    try:
        # Create the JSON string for alternate-identifier using primaryEmail
        alternate_identifier = json.dumps({"UniqueAttribute": {"AttributePath": "emails.value", "AttributeValue": username}})
        
        cmd = ['aws']
        if config_profile:
            cmd.extend(['--profile', config_profile])
        if region_name:
            cmd.extend(['--region', region_name])
        cmd.extend([
            'identitystore', 'get-user-id',
            '--identity-store-id', identity_store_id,
            '--alternate-identifier', alternate_identifier
        ])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Warning: Could not get user ID for {username}: {result.stderr}")
            return username
        
        response = json.loads(result.stdout)
        user_id = response.get('UserId')
        if user_id:
            print(f"Found IDC user ID for {username}: {user_id}")
            return user_id
        else:
            print(f"Warning: No user ID found for {username}")
            return username
            
    except Exception as e:
        print(f"Warning: Error getting IDC user ID for {username}: {e}")
        return username

# Cache for user IDs and group IDs to avoid duplicate lookups
user_id_cache = {}
group_id_cache = {}

def remove_prefix(text, prefix):
    """
    Helper function to remove prefix from a string if it exists
    """
    return text[len(prefix):] if text.lower().startswith(prefix.lower()) else text

def get_principal_identifier(identity_name, aws_account_id, config_profile=None, identity_store_id=None, identity_provider_namespace='AWSIDC', region_name=None):
    """
    Generate the principal identifier for Lake Formation permissions based on identity name for IDC users
    """
    # Extract user name from the identity name if it has a prefix
    prefix = f"{identity_provider_namespace}:"
    username = remove_prefix(identity_name, prefix)
    
    if not identity_store_id:
        print(f"Warning: No identity store ID provided for IDC user {username}")
        return f"arn:aws:identitystore:::user/{username}"
    
    # Use cached user ID if available, otherwise look it up
    user_id = user_id_cache.get(username) or get_idc_user_id(username, identity_store_id, config_profile, identity_provider_namespace, region_name)
    user_id_cache[username] = user_id
    
    # Use the format with the actual user ID
    return f"arn:aws:identitystore:::user/{user_id}"

def create_lf_command(aws_account_id, profile_param, principal_identifier, resource_json, permissions_str, comment="", action="grant"):
    """
    Helper function to create a Lake Formation command with consistent formatting
    action can be 'grant' or 'revoke'
    """
    return (
        comment +
        f"aws{profile_param} lakeformation {action}-permissions \\\n" +
        f"  --catalog-id \"{aws_account_id}\" \\\n" +
        f"  --principal DataLakePrincipalIdentifier={principal_identifier} \\\n" +
        f"  --resource '{resource_json}' \\\n" +
        f"  --permissions {permissions_str}\n\n"
    )

def create_table_resource_json(full_catalog_id, schema_name, table_name=None):
    """
    Helper function to create the resource JSON for a table or schema
    """
    if table_name:
        # Table-level resource
        return '{"Table": {"CatalogId": "' + full_catalog_id + '", "DatabaseName": "' + schema_name + '", "Name": "' + table_name + '"}}'
    else:
        # Schema-level resource with TableWildcard
        return '{"Table": {"CatalogId": "' + full_catalog_id + '", "DatabaseName": "' + schema_name + '", "TableWildcard": {}}}'


def generate_lakeformation_commands(tables, user_map, aws_account_id, producer_catalog, 
                             object_level_permissions=False, table_grants=None, config_profile=None, identity_store_id=None, conn=None, output_dir=None,
                             debug=False, identity_provider_namespace='AWSIDC', region_name=None):
    """
    Generate AWS CLI commands for LakeFormation permissions
    If object_level_permissions is True, generate table-level permissions based on SHOW GRANTS
    Otherwise, generate schema-level permissions using TableWildcard syntax
    """
    logger = logging.getLogger(__name__)
    commands = []
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create output directory in the same location as logs
    if output_dir:
        output_base = output_dir
    else:
        output_base = '.'
    
    run_dir = os.path.join(output_base, f"run_{timestamp}")
    os.makedirs(run_dir, exist_ok=True)
    script_filepath = os.path.join(run_dir, f"lakeformation_permissions_{timestamp}.sh")

    # Create debug file for get-user-id commands if debug mode is enabled
    if debug:
        debug_filepath = os.path.join(run_dir, f"get_user_id_commands_{timestamp}.sh")
        with open(debug_filepath, 'w') as debug_file:
            debug_file.write("#!/bin/bash\n\n")
            debug_file.write("# AWS CLI get-user-id commands for debugging\n")
            debug_file.write(f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Collect all unique usernames
            usernames = set()
            if object_level_permissions and table_grants:
                for grants in table_grants.values():
                    for grantee, _, identity_type in grants:
                        # Only include users, not roles
                        if identity_type.lower() == 'user':
                            # Use the configured identity provider namespace
                            prefix = f"{identity_provider_namespace}:"
                            if grantee.lower().startswith(prefix.lower()):
                                usernames.add(grantee[len(prefix):])
                            else:
                                usernames.add(grantee)
            else:
                for users in user_map.values():
                    for user in users:
                        # Use the configured identity provider namespace
                        prefix = f"{identity_provider_namespace}:"
                        if user.lower().startswith(prefix.lower()):
                            usernames.add(user[len(prefix):])
                        else:
                            usernames.add(user)
            
            # Write get-user-id commands
            for username in sorted(usernames):
                cmd = ['aws']
                if config_profile:
                    cmd.extend(['--profile', config_profile])
                # Create the JSON string for alternate-identifier
                alternate_identifier = json.dumps({"UniqueAttribute": {"AttributePath": "emails.value", "AttributeValue": username}})
                
                # Format the command with proper quoting for the shell script, with each parameter on a separate line
                debug_file.write(f"# Getting user ID for {username}\n")
                debug_file.write(f"aws{' --profile ' + config_profile if config_profile else ''}{' --region ' + region_name if region_name else ''} identitystore get-user-id \\\n")
                debug_file.write(f"  --identity-store-id {identity_store_id} \\\n")
                debug_file.write(f"  --alternate-identifier '{alternate_identifier}'\n\n")


    # Format the complete CatalogId for the resource section
    full_catalog_id = f"{aws_account_id}:{producer_catalog}"

    with open(script_filepath, 'w') as script_file:
        script_file.write("#!/bin/bash\n\n")
        script_file.write("# AWS LakeFormation permission commands generated from Redshift permissions\n")
        script_file.write(f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        script_file.write("# Authentication type: IDC\n")
        script_file.write(f"# Object-level permissions: {object_level_permissions}\n\n")

        # Track how many commands we generate
        commands_count = 0
        
        # Track role tables
        role_tables = {}
        
        # Check if IDC roles have other roles granted to them
        idc_role_grants = {}
        if conn:
            idc_role_grants = get_idc_role_grants(conn, identity_provider_namespace)
            
            # Get table grants for the granted roles
            if idc_role_grants:
                all_granted_roles = set()
                for granted_roles in idc_role_grants.values():
                    all_granted_roles.update(granted_roles)
                
                if all_granted_roles:
                    print(f"Getting table grants for roles granted to IDC roles: {all_granted_roles}")
                    # Get ALL permissions for granted roles regardless of auth_type
                    granted_role_tables = get_table_grants_for_roles(conn, tables, all_granted_roles)
                    
                    # Debug: Show what permissions were found for granted roles
                    for (db, schema, table), grants in granted_role_tables.items():
                        for grantee, priv, identity in grants:
                            if grantee in all_granted_roles:
                                print(f"  DEBUG: Found {grantee} has {priv} on {schema}.{table}")
                    
                    # Add granted role permissions to IDC roles
                    for idc_role, granted_roles in idc_role_grants.items():
                        for granted_role in granted_roles:
                            # Find tables where the granted role has permissions
                            for (db_name, schema_name, table_name), grants in granted_role_tables.items():
                                for grantee, privilege_type, identity_type in grants:
                                    if grantee == granted_role and privilege_type not in ['RULE', 'TRIGGER', 'REFERENCES', 'TRUNCATE', 'UPDATE']:
                                        # Add this table to the IDC role's permissions
                                        if idc_role not in role_tables:
                                            role_tables[idc_role] = []
                                        if (schema_name, table_name) not in role_tables[idc_role]:
                                            role_tables[idc_role].append((schema_name, table_name))
                                            print(f"  Added table {schema_name}.{table_name} to IDC role {idc_role} via granted role {granted_role}")
                                        
                                        # Also add to table_grants so it gets processed in the main loop
                                        table_key = (None, schema_name, table_name)
                                        if table_key not in table_grants:
                                            table_grants[table_key] = []
                                        # Add the IDC role as having the same permission on this table
                                        grant_tuple = (idc_role, privilege_type, 'role')
                                        if grant_tuple not in table_grants[table_key]:
                                            table_grants[table_key].append(grant_tuple)
                                            print(f"  Added {idc_role} {privilege_type} grant for table {schema_name}.{table_name} to table_grants")
                                        else:
                                            print(f"  Skipped duplicate: {idc_role} {privilege_type} grant for table {schema_name}.{table_name}")
                        
                        print(f"  Processing IDC role {idc_role} with granted roles")
        
        # Set profile parameter for all commands
        profile_param = f" --profile {config_profile}" if config_profile else ""
        
        if object_level_permissions and table_grants:
            script_file.write("# Table-level permissions based on SHOW GRANTS results\n\n")
            
            # Group grants by (grantee, table) to combine permissions
            grouped_grants = {}
            for (db_name, schema_name, table_name), grants in table_grants.items():
                for grantee, privilege_type, identity_type in grants:
                    key = (grantee, db_name, schema_name, table_name, identity_type)
                    if key not in grouped_grants:
                        grouped_grants[key] = []
                    grouped_grants[key].append(privilege_type)
            
            # Process grouped grants
            for (grantee, db_name, schema_name, table_name, identity_type), privileges in grouped_grants.items():
                # Create permissions string and add DESCRIBE if any permissions exist
                permissions_set = set(privileges)
                if permissions_set:
                    permissions_set.add('DESCRIBE')
                perms_str = ' '.join(f'"{perm}"' for perm in sorted(permissions_set))
                    
                # Handle roles
                if identity_type.lower() == 'role':
                    # Use group ID as principal
                    group_id = get_idc_group_id(grantee, identity_store_id, config_profile, identity_provider_namespace, region_name)
                    if group_id:
                        principal_identifier = f"arn:aws:identitystore:::group/{group_id}"
                    else:
                        continue  # Skip if group ID not found
                else:
                    # Get principal identifier for users
                    principal_identifier = get_principal_identifier(grantee, aws_account_id, config_profile, identity_store_id, identity_provider_namespace, region_name)
                
                # Generate command with all permissions for this user/role on this table
                # Add comment showing the IDC user/group name
                # Use the configured identity provider namespace for comments
                prefix = f"{identity_provider_namespace}:"
                if identity_type.lower() == 'role':
                    comment = f"# IDC Group: {remove_prefix(grantee, prefix)}\n"
                else:
                    comment = f"# IDC User Email: {remove_prefix(grantee, prefix)}\n"
                
                # Create resource JSON for table
                resource_json = create_table_resource_json(full_catalog_id, schema_name, table_name)
                
                # Generate command
                command = create_lf_command(
                    aws_account_id, 
                    profile_param, 
                    principal_identifier, 
                    resource_json, 
                    perms_str, 
                    comment
                )
                
                script_file.write(command)
                commands.append(command)
                commands_count += 1
            
            logger.info(f"Generated {commands_count} table-level permission commands")
        else:
            # Get unique database-schema combinations
            db_schema_map = {}
            for db_name, schema_name, _ in tables:
                db_schema_map.setdefault(db_name, set()).add(schema_name)
                
            # Generate schema-level permissions with TableWildcard
            script_file.write("# Schema-level SELECT and DESCRIBE permissions for all tables in datashare database(s)\n")
            script_file.write("# For users with USAGE permission on the database\n\n")
            
            for db_name, schemas in db_schema_map.items():
                # If we have users with USAGE permission for this database
                if db_name in user_map:
                    for schema_name in schemas:
                        for identity_name in user_map[db_name]:
                            principal_identifier = get_principal_identifier(identity_name, aws_account_id, config_profile, identity_store_id, identity_provider_namespace, region_name)

                            # Use schema name as DatabaseName parameter in the LakeFormation resource
                            profile_param = f" --profile {config_profile}" if config_profile else ""
                            # Create resource JSON for schema (TableWildcard)
                            resource_json = create_table_resource_json(full_catalog_id, schema_name)
                            
                            # Generate command
                            command = create_lf_command(
                                aws_account_id, 
                                profile_param, 
                                principal_identifier, 
                                resource_json, 
                                '"SELECT" "DESCRIBE"'
                            )

                            script_file.write(command)
                            commands.append(command)
                            commands_count += 1
            
            logger.info(f"Generated {commands_count} schema-level SELECT and DESCRIBE permission commands")

    # Create rollback script
    rollback_filepath = os.path.join(run_dir, f"rollback_lakeformation_{timestamp}.sh")
    with open(rollback_filepath, 'w') as rollback_file:
        rollback_file.write("#!/bin/bash\n\n")
        rollback_file.write("# AWS LakeFormation rollback commands\n")
        rollback_file.write(f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        rollback_file.write("# Use this script to revoke permissions created by the lakeformation_permissions script\n\n")
        
        # Rollback for table permissions
        if object_level_permissions and table_grants:
            rollback_file.write("# Revoke table-level permissions\n\n")
            
            # Group grants by (grantee, table) to combine permissions, just like in the main script
            grouped_grants = {}
            for (db_name, schema_name, table_name), grants in table_grants.items():
                for grantee, privilege_type, identity_type in grants:
                    key = (grantee, db_name, schema_name, table_name, identity_type)
                    if key not in grouped_grants:
                        grouped_grants[key] = []
                    grouped_grants[key].append(privilege_type)
            
            # Process grouped grants
            for (grantee, db_name, schema_name, table_name, identity_type), privileges in grouped_grants.items():
                # Create permissions string and add DESCRIBE if any permissions exist
                permissions_set = set(privileges)
                if permissions_set:
                    permissions_set.add('DESCRIBE')  # Add DESCRIBE to match the main script
                perms_str = ' '.join(f'"{perm}"' for perm in sorted(permissions_set))
                
                # Handle roles
                if identity_type.lower() == 'role':
                    # For IDC roles, get group ID
                    group_id = get_idc_group_id(grantee, identity_store_id, config_profile, identity_provider_namespace, region_name)
                    if group_id:
                        principal_identifier = f"arn:aws:identitystore:::group/{group_id}"
                    else:
                        continue  # Skip if group ID not found
                else:
                    # Get principal identifier for users
                    principal_identifier = get_principal_identifier(grantee, aws_account_id, config_profile, identity_store_id, identity_provider_namespace, region_name)
                
                # Create resource JSON for table
                resource_json = create_table_resource_json(full_catalog_id, schema_name, table_name)
                
                # Generate revoke command with all permissions for this user/role on this table
                prefix = f"{identity_provider_namespace}:"
                if identity_type.lower() == 'role':
                    comment = f"# IDC Group: {remove_prefix(grantee, prefix)}\n"
                else:
                    comment = f"# IDC User Email: {remove_prefix(grantee, prefix)}\n"
                
                revoke_cmd = create_lf_command(
                    aws_account_id, 
                    profile_param, 
                    principal_identifier, 
                    resource_json, 
                    perms_str, 
                    comment, 
                    "revoke"
                )
                
                rollback_file.write(revoke_cmd)
        else:
            # Rollback for schema-level permissions
            rollback_file.write("# Revoke schema-level permissions\n\n")
            for db_name, schemas in db_schema_map.items():
                if db_name in user_map:
                    for schema_name in schemas:
                        for identity_name in user_map[db_name]:
                            principal_identifier = get_principal_identifier(identity_name, aws_account_id, config_profile, identity_store_id, identity_provider_namespace, region_name)
                            # Create resource JSON for schema (TableWildcard)
                            resource_json = create_table_resource_json(full_catalog_id, schema_name)
                            
                            # Generate revoke command
                            revoke_cmd = create_lf_command(
                                aws_account_id, 
                                profile_param, 
                                principal_identifier, 
                                resource_json, 
                                '"SELECT" "DESCRIBE"', 
                                "", 
                                "revoke"
                            )
                            
                            rollback_file.write(revoke_cmd)
    
    logger.info(f"Generated {len(commands)} total LakeFormation commands in file: {script_filepath}")
    logger.info(f"Generated rollback script: {rollback_filepath}")
    return commands

def get_identity_provider_namespace(conn):
    """
    Query SVV_IDENTITY_PROVIDERS to get the identity provider namespace for AWS IDC
    """
    try:
        cursor = conn.cursor()
        query = "select namespc from SVV_IDENTITY_PROVIDERS where type='awsidc'"
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        if result and result[0]:
            return result[0]
        else:
            print("Warning: No AWS IDC identity provider found in SVV_IDENTITY_PROVIDERS, using default 'AWSIDC'")
            return 'AWSIDC'
    except Exception as e:
        print(f"Error querying SVV_IDENTITY_PROVIDERS: {e}")
        print("Using default identity provider namespace: 'AWSIDC'")
        return 'AWSIDC'

def main():
    parser = argparse.ArgumentParser(description='Extract Redshift permissions and generate AWS LakeFormation commands')

    parser.add_argument('--config', '-c', default='config.ini', help='Path to config file (default: config.ini)')
    parser.add_argument('--output-dir', '-o', help='Base directory to save generated scripts (default: "output")')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.debug, args.output_dir)

    # Load configuration
    config = load_config(args.config)

    # Connect to Redshift
    conn = connect_to_redshift(config)

    # Get parameters from config
    p_datashare_database_name = None
    permissions_type = 'datashare'  # default
    
    if 'parameters' in config:
        if 'datashare_database_name' in config['parameters']:
            p_datashare_database_name = config['parameters']['datashare_database_name']
        if 'permissions_type' in config['parameters']:
            permissions_type = config['parameters']['permissions_type'].lower()
    
    logger.info(f"Permissions type: {permissions_type}")
    
    # Check if datashare object-level permissions are enabled
    object_level_permissions = False
    if 'parameters' in config and 'datashare_object_level_permissions' in config['parameters']:
        object_level_permissions = config['parameters']['datashare_object_level_permissions'].lower() == 'true'
    logger.info(f"Datashare object-level permissions: {object_level_permissions}")
    
    # Get identity provider namespace from SVV_IDENTITY_PROVIDERS
    identity_provider_namespace = get_identity_provider_namespace(conn)
    logger.info(f"Using identity provider namespace: {identity_provider_namespace}")
    
    # Process based on permissions_type
    if permissions_type == 'local':
        # Get local database name from config
        local_db_name = config['redshift']['dbname']
        logger.info(f"Processing local database permissions for: {local_db_name}")
        
        user_map, tables, table_grants = get_local_database_permissions(conn, local_db_name, identity_provider_namespace)
        user_count = sum(len(users) for users in user_map.values())
        logger.info(f"Found {user_count} IDC users with permissions on local database {local_db_name}")
        logger.info(f"Retrieved {len(tables)} tables from local database")
        
        if table_grants:
            grant_count = sum(len(grants) for grants in table_grants.values())
            logger.info(f"Retrieved {grant_count} table-level grants for IDC users/roles")
    else:
        # Original datashare logic
        if p_datashare_database_name:
            logger.info(f"Filtering permissions for datashare database: {p_datashare_database_name}")
        
        # Get users with USAGE permission on the database (required to access tables)
        user_map = get_users_with_usage_permission(conn, p_datashare_database_name, identity_provider_namespace)
        db_count = len(user_map)
        user_count = sum(len(users) for users in user_map.values())
        
        logger.info(f"Found {user_count} IDC users (with {identity_provider_namespace}: prefix) with USAGE permission across {db_count} databases")

        # Get all tables in the datashare database(s)
        tables = get_datashare_tables(conn, p_datashare_database_name)
        logger.info(f"Retrieved {len(tables)} tables from datashare database(s)")

        # Get table-level grants if datashare object_level_permissions is enabled
        table_grants = None
        if object_level_permissions and tables:
            logger.info("Getting table-level grants using SHOW GRANTS...")
            table_grants = get_table_grants(conn, tables, identity_provider_namespace)
            grant_count = sum(len(grants) for grants in table_grants.values())
            
            logger.info(f"Retrieved {grant_count} table-level grants for IDC users/roles (with {identity_provider_namespace}: prefix)")

    if args.debug:
        if user_map:
            print("\nUsers with USAGE permission by database:")
            for db_name, users in user_map.items():
                print(f"  {db_name}: {users}")

        print("\nTables data (sample):")
        for table in tables[:5]:  # Print first 5 tables for debugging
            print(f"  {table}")

        # Print all databases found in tables
        db_names = set([t[0] for t in tables])
        print(f"\nDatabases found in tables: {db_names}")
        
        # Print table grants if available
        if table_grants:
            print("\nTable grants (sample):")
            sample_count = 0
            for table_key, grants in table_grants.items():
                if sample_count >= 5:
                    break
                if grants:
                    db, schema, table = table_key
                    print(f"  {db}.{schema}.{table}: {grants}")
                    sample_count += 1

    # Generate LakeFormation commands
    if tables and (user_map or (object_level_permissions and table_grants)):
        if not user_map and (not table_grants or not any(table_grants.values())):
            logger.warning(f"No IDC users or roles (with {identity_provider_namespace}: prefix) found with permissions. No Lake Formation commands will be generated.")
            return
        aws_account_id = config.get('aws', 'account_id', fallback=None)
        if not aws_account_id:
            logger.error("AWS account ID not found in config")
            sys.exit(1)

        # Get producer catalog from config
        producer_catalog = config.get('aws', 'producer_catalog', fallback=None)
        if not producer_catalog:
            logger.error("producer_catalog not found in config")
            sys.exit(1)
        
        # Get AWS CLI profile from config (optional)
        config_profile = config.get('aws', 'config_profile', fallback=None)
        if config_profile:
            logger.info(f"Using AWS CLI profile: {config_profile}")
        
        # Get identity store ID from config (required for IDC)
        identity_store_id = config.get('aws', 'identity_store_id', fallback=None)
        if not identity_store_id:
            logger.warning("identity_store_id not found in config")
        
        # Get region from config
        region_name = config.get('aws', 'region', fallback=None)
        if region_name:
            logger.info(f"Using AWS region: {region_name}")
        
        # Generate LakeFormation commands, passing the open connection for role user lookup
        generate_lakeformation_commands(
            tables, user_map, aws_account_id, producer_catalog, 
            object_level_permissions, table_grants, config_profile, identity_store_id, conn, args.output_dir,
            args.debug, identity_provider_namespace, region_name
        )
        
        # Now close the connection
        conn.close()
    else:
        logger.warning("No datashare tables or users found to convert.")

if __name__ == "__main__":
    main()