import os
import sys
import psycopg2
import argparse
import configparser
import re
import shlex
import secrets
import string
from datetime import datetime

def load_config(config_file='config.ini'):
    """
    Load configuration from config file (default: config.ini)
    """
    if not os.path.exists(config_file):
        print(f"Error: Config file '{config_file}' not found")
        sys.exit(1)

    config = configparser.ConfigParser()
    try:
        config.read(config_file)
        print(f"Loaded configuration from {config_file}")
        return config
    except Exception as e:
        print(f"Error reading config file: {e}")
        sys.exit(1)

def connect_to_redshift(config):
    """
    Establish connection to Redshift database using config
    """
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
    except KeyError as e:
        print(f"Missing required configuration parameter: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error connecting to Redshift: {e}")
        sys.exit(1)

def get_redshift_users(conn):
    """
    Get users from pg_user catalog table in Redshift, filtering out IAM/IAMR users in SQL
    """
    users = []
    try:
        cursor = conn.cursor()
        # Query to get usernames from pg_user with filtering in SQL
        cursor.execute("""
            SELECT usename 
            FROM pg_user 
            WHERE usesuper = 'f' 
              AND usename != 'rdsdb'
              AND NOT (usename LIKE 'IAM:%' OR usename LIKE 'IAMR:%' OR usename LIKE 'AWSIDC:%')
            ORDER BY 1;
        """)

        for row in cursor:
            users.append(row[0])

        cursor.close()
        print(f"Retrieved {len(users)} non-IAM users from Redshift")
        return users
    except Exception as e:
        print(f"Error retrieving users from Redshift: {e}")
        conn.close()
        sys.exit(1)

def generate_secure_password(length=16):
    """
    Generate a secure random password
    """
    # Include at least one of each required character type for AWS IAM password policy
    letters = string.ascii_letters
    digits = string.digits
    special_chars = "!@#" + "$" + "%^&*()_+-=[]{}|"  # Avoid escape sequence issues
    password_chars = letters + digits + special_chars

    # Ensure we have at least one uppercase, one lowercase, one digit, and one special character
    password = [
        secrets.choice(string.ascii_uppercase),
        secrets.choice(string.ascii_lowercase),
        secrets.choice(string.digits),
        secrets.choice(special_chars)
    ]

    # Fill the rest of the password length with random characters
    password.extend(secrets.choice(password_chars) for _ in range(length - 4))

    # Shuffle the password characters
    secrets.SystemRandom().shuffle(password)

    return ''.join(password)

def generate_iam_cli_command(username):
    """
    Generate AWS CLI command to create an IAM user with proper escaping
    """
    # Ensure IAM username follows IAM naming rules (alphanumeric, plus '+', '=', ',', '.', '@', '-', '_')
    valid_username = re.sub(r'[^a-zA-Z0-9+=,.@\-_]', '-', username)

    # Truncate if username exceeds IAM's 64-character limit
    if len(valid_username) > 64:
        valid_username = valid_username[:64]
        print(f"Warning: Username truncated to {valid_username} due to IAM 64-character limit")

    # Escape the original username for shell safety
    escaped_original = shlex.quote(username)

    # Generate AWS CLI command with properly formatted tags
    cli_command = f'aws iam create-user --user-name {valid_username} ' \
                  f'--tags Key=Source,Value=Redshift Key=OriginalRedshiftUsername,Value={escaped_original}'

    return cli_command, valid_username

def generate_policy_attachment_commands(username, policies):
    """
    Generate AWS CLI commands to attach policies to an IAM user
    """
    commands = []
    for policy in policies:
        commands.append(f'aws iam attach-user-policy --user-name {username} --policy-arn {policy}')
    return commands

def generate_console_access_command(username):
    """
    Generate AWS CLI command to create console access with password change required
    """
    password = generate_secure_password()

    # Create login profile command with password reset required
    command = f'aws iam create-login-profile --user-name {username} ' \
              f'--password "{password}" --password-reset-required'

    return command

def generate_commands(redshift_users, create_console_password=True):
    """
    Generate all the AWS CLI commands for IAM users
    """
    commands = []

    # List of IAM policies to attach to each user
    iam_policies = [
        'arn:aws:iam::aws:policy/AmazonRedshiftReadOnlyAccess',
        'arn:aws:iam::aws:policy/AmazonRedshiftQueryEditorV2ReadWriteSharing',
        # Add more policy ARNs as needed
    ]

    # Generate header comments
    commands.append("#!/bin/bash\n")
    commands.append("# AWS CLI Commands to create IAM users from Redshift users")
    commands.append("# Generated script for creating IAM users from Redshift users")
    commands.append("# Consider checking for existing IAM users first with: aws iam list-users\n")

    if create_console_password:
        commands.append("# NOTE: Console access passwords are randomly generated and users will be required to change them on first login\n")

    if not redshift_users:
        commands.append("echo \"No eligible Redshift users found to create IAM users.\"")

    for username in redshift_users:
        # Create user command
        cli_command, valid_username = generate_iam_cli_command(username)
        commands.append(cli_command)

        # Create console access command only if requested
        if create_console_password:
            console_command = generate_console_access_command(valid_username)
            commands.append(console_command)

        # Generate policy attachment commands if policies are specified
        if iam_policies:
            for policy_command in generate_policy_attachment_commands(valid_username, iam_policies):
                commands.append(policy_command)
            commands.append("")  # Add a blank line for readability

    # Add some follow-up commands information
    if redshift_users:
        commands.append("\n# After creating users, you might want to add additional policies")
        commands.append("# Example: aws iam attach-user-policy --user-name USERNAME --policy-arn POLICY_ARN")
        commands.append("# To check if users were created: aws iam list-users")

        if create_console_password:
            commands.append("# To check users with console access: aws iam list-users | jq '.Users[] | select(has(\"PasswordLastUsed\"))'")

    return commands

def write_to_file(commands, output_dir=None):
    """
    Write commands to a file in the specified output directory
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if output_dir:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        script_filepath = os.path.join(output_dir, f"create_iam_users_{timestamp}.sh")
    else:
        script_filepath = f"create_iam_users_{timestamp}.sh"

    with open(script_filepath, 'w') as script_file:
        for command in commands:
            script_file.write(f"{command}\n")

    # Make the file executable
    os.chmod(script_filepath, 0o755)

    print(f"Generated AWS CLI commands in file: {script_filepath}")
    return script_filepath

def main():
    """
    Main function to process Redshift users and generate AWS IAM commands
    """
    parser = argparse.ArgumentParser(description='Generate AWS CLI commands to create IAM users from Redshift users.')

    parser.add_argument('--config', '-c', default='config.ini', help='Path to config file (default: config.ini)')
    parser.add_argument('--output-dir', '-o', help='Directory to save generated scripts')
    parser.add_argument('--no-console', action='store_true', help='Do not create console access for users')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')

    args = parser.parse_args()

    try:
        # Load configuration
        config = load_config(args.config)

        # Connect to Redshift
        conn = connect_to_redshift(config)

        # Get Redshift users (with SQL filtering)
        redshift_users = get_redshift_users(conn)

        # Close the connection
        conn.close()

        if args.debug:
            print("\nRedshift users found:")
            for user in redshift_users:
                print(f"  {user}")
            print()

        # Generate commands
        commands = generate_commands(redshift_users, not args.no_console)

        # Write commands to file
        write_to_file(commands, args.output_dir)

    except Exception as e:
        print(f"Error in main process: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()