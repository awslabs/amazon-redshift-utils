#!/usr/bin/env python3
"""
Create Redshift IDC users script

This script:
1. Connects to Redshift
2. Gets all local users (without IAM:/IAMR:/AWSIDC: prefixes)
3. Maps them to IAM IDC to get their email addresses
4. Creates Redshift CLI commands to create IDC users via get-cluster-credentials
"""

import os
import sys
import psycopg2
import argparse
import configparser
import subprocess
import json
from datetime import datetime

def load_config(config_file='config.ini'):
    """Load configuration from config file"""
    if not os.path.exists(config_file):
        print(f"Error: Config file '{config_file}' not found")
        sys.exit(1)

    config = configparser.ConfigParser()
    try:
        config.read(config_file)
        return config
    except Exception as e:
        print(f"Error reading config file: {e}")
        sys.exit(1)

def connect_to_redshift(config):
    """Establish connection to Redshift database using config"""
    try:
        redshift_config = config['redshift']
        conn = psycopg2.connect(
            host=redshift_config['host'],
            port=int(redshift_config.get('port', 5439)),
            dbname=redshift_config['dbname'],
            user=redshift_config['user'],
            password=redshift_config['password']
        )
        print(f"Connected to Redshift database: {redshift_config['dbname']}")
        return conn
    except Exception as e:
        print(f"Error connecting to Redshift: {e}")
        sys.exit(1)

def get_local_redshift_users(conn):
    """Get local Redshift users (excluding IAM/IAMR/AWSIDC prefixed users)"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT usename 
            FROM pg_user 
            WHERE usesuper = 'f' 
              AND usename != 'rdsdb'
              AND NOT (usename LIKE 'IAM:%' OR usename LIKE 'IAMR:%' OR usename LIKE 'AWSIDC:%')
            ORDER BY usename
        """)
        
        users = [row[0] for row in cursor.fetchall()]
        cursor.close()
        print(f"Found {len(users)} local Redshift users")
        return users
    except Exception as e:
        print(f"Error retrieving users from Redshift: {e}")
        sys.exit(1)

def get_idc_user_id(username, identity_store_id, config_profile=None):
    """Get IDC user ID using AWS CLI get-user-id command"""
    try:
        # Create the JSON string for alternate-identifier
        alternate_identifier = json.dumps({"UniqueAttribute": {"AttributePath": "UserName", "AttributeValue": username}})
        
        cmd = ['aws']
        if config_profile:
            cmd.extend(['--profile', config_profile])
        cmd.extend([
            'identitystore', 'get-user-id',
            '--identity-store-id', identity_store_id,
            '--alternate-identifier', alternate_identifier
        ])
        
        print(f"  Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Warning: Could not get user ID for {username}: {result.stderr}")
            return None
        
        response = json.loads(result.stdout)
        user_id = response.get('UserId')
        if user_id:
            print(f"Found IDC user ID for {username}: {user_id}")
            return user_id
        else:
            print(f"Warning: No user ID found for {username}")
            return None
            
    except Exception as e:
        print(f"Warning: Error getting IDC user ID for {username}: {e}")
        return None

def get_idc_user_email(user_id, identity_store_id, config_profile=None):
    """Get IDC user email using user ID"""
    try:
        cmd = ['aws']
        if config_profile:
            cmd.extend(['--profile', config_profile])
        cmd.extend([
            'identitystore', 'describe-user',
            '--identity-store-id', identity_store_id,
            '--user-id', user_id
        ])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Warning: Could not get user details for user ID {user_id}: {result.stderr}")
            return None
        
        user_details = json.loads(result.stdout)
        emails = user_details.get('Emails', [])
        
        if emails:
            primary_email = next((email['Value'] for email in emails if email.get('Primary', False)), emails[0]['Value'])
            print(f"Found email for user ID {user_id}: {primary_email}")
            return primary_email
        else:
            print(f"Warning: No email found for user ID {user_id}")
            return None
            
    except Exception as e:
        print(f"Warning: Error getting IDC user email for user ID {user_id}: {e}")
        return None

def generate_get_cluster_credentials_command(username, email, cluster_identifier, config_profile=None):
    """Generate get-cluster-credentials command"""
    profile_param = f" --profile {config_profile}" if config_profile else ""
    
    # The IDC user will be created as AWSIDC:email
    idc_username = f"AWSIDC:{email}"
    
    command = f"aws{profile_param} redshift get-cluster-credentials \\\n"
    command += f"  --cluster-identifier {cluster_identifier} \\\n"
    command += f"  --db-user {email} \\\n"
    command += f"  --duration-seconds 3600 \\\n"
    command += f"  --auto-create"
    
    return command, idc_username

def generate_commands(local_users, user_emails, cluster_identifier, config_profile=None, output_dir=None):
    """Generate all the Redshift CLI commands"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if output_dir:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        script_filepath = os.path.join(output_dir, f"create_redshift_idc_users_{timestamp}.sh")
    else:
        script_filepath = f"create_redshift_idc_users_{timestamp}.sh"
    
    with open(script_filepath, 'w') as script_file:
        script_file.write("#!/bin/bash\n\n")
        script_file.write("# Redshift get-cluster-credentials commands to create IDC users\n")
        script_file.write(f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        script_file.write("# These commands will create AWSIDC: prefixed users in Redshift\n\n")
        
        commands_generated = 0
        
        for username in local_users:
            email = user_emails.get(username)
            if email:
                command, idc_username = generate_get_cluster_credentials_command(
                    username, email, cluster_identifier, config_profile
                )
                
                script_file.write(f"# Local user: {username} -> IDC user: {idc_username}\n")
                script_file.write(f"{command}\n\n")
                commands_generated += 1
            else:
                script_file.write(f"# Warning: No email found for user {username}, skipping\n\n")
        
        if commands_generated == 0:
            script_file.write("echo \"No valid user mappings found to create IDC users.\"\n")
    
    # Make the file executable
    os.chmod(script_filepath, 0o755)
    
    print(f"Generated {commands_generated} get-cluster-credentials commands in file: {script_filepath}")
    return script_filepath

def main():
    parser = argparse.ArgumentParser(description='Create Redshift IDC users from local Redshift users')
    
    parser.add_argument('--config', '-c', default='config.ini', help='Path to config file (default: config.ini)')
    parser.add_argument('--output-dir', '-o', help='Directory to save generated scripts')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Get required config values
    try:
        identity_store_id = config.get('aws', 'identity_store_id')
        workgroup_name = config.get('redshift', 'host').split('.')[0]  # Extract workgroup name from host
        cluster_identifier = f"redshift-serverless-{workgroup_name}"
        config_profile = config.get('aws', 'config_profile', fallback=None)
    except Exception as e:
        print(f"Error reading required configuration: {e}")
        sys.exit(1)
    
    # Connect to Redshift
    conn = connect_to_redshift(config)
    
    # Get local Redshift users
    local_users = get_local_redshift_users(conn)
    
    # Close connection
    conn.close()
    
    if not local_users:
        print("No local Redshift users found to process")
        return
    
    # Map users to their IDC email addresses
    print(f"\nMapping {len(local_users)} users to IDC email addresses...")
    user_emails = {}
    
    for username in local_users:
        print(f"\nProcessing user: {username}")
        print(f"  Looking up in Identity Store: {identity_store_id}")
        print(f"  Using profile: {config_profile or 'default'}")
        
        # First get the user ID
        user_id = get_idc_user_id(username, identity_store_id, config_profile)
        if user_id:
            print(f"  Found user ID: {user_id}")
            # Then get the email using the user ID
            email = get_idc_user_email(user_id, identity_store_id, config_profile)
            if email:
                user_emails[username] = email
                print(f"  Successfully mapped: {username} -> {email}")
            else:
                print(f"  Failed to get email for user ID: {user_id}")
        else:
            print(f"  User '{username}' not found in Identity Center")
            print(f"  This means '{username}' does not exist as an IDC user")
            print(f"  You may need to create this user in Identity Center first")
    
    print(f"\nSummary: Successfully mapped {len(user_emails)} out of {len(local_users)} users to email addresses")
    
    if len(user_emails) == 0:
        print("\nNo users were successfully mapped. This could mean:")
        print("  1. The local Redshift users don't exist in Identity Center")
        print("  2. The Identity Store ID is incorrect")
        print("  3. The AWS profile doesn't have permission to access Identity Center")
        print("  4. The usernames in Redshift don't match the usernames in Identity Center")
    
    if args.debug or len(user_emails) > 0:
        print("\nUser mappings:")
        for username, email in user_emails.items():
            print(f"  {username} -> {email}")
        
        if len(user_emails) < len(local_users):
            unmapped = set(local_users) - set(user_emails.keys())
            print("\nUnmapped users:")
            for username in unmapped:
                print(f"  {username} (not found in Identity Center)")
    
    # Generate commands
    generate_commands(local_users, user_emails, cluster_identifier, config_profile, args.output_dir)

if __name__ == "__main__":
    main()