# Redshift Permissions to Lake Formation Migration Utility

Utility Version: 1.0 Release Date: 31-Jul-2025

A Python script that extracts permissions from Amazon Redshift (datashares or local databases) and generates AWS Lake Formation permission commands for seamless migration to a SageMaker Lakehouse architecture.

## Overview

This tool automates the migration of data access permissions from Amazon Redshift to AWS Lake Formation by:
- Extracting user permissions from Redshift datashare databases or local databases
- Analyzing table-level grants using `SHOW GRANTS`
- Generating AWS CLI commands for Lake Formation permissions
- Supporting AWS Identity Center (IDC) authentication
- Creating rollback scripts for permission cleanup

## Features

- **Identity Center Authentication**: Supports AWS Identity Center (IDC) users/groups
- **Permission Granularity**: Schema-level or table-level permissions based on configuration
- **Role Inheritance**: Handles IDC roles with granted Redshift roles. This includes Redshift roles that have been granted permissions via other Redshift roles.
- **Automated Scripts**: Generates executable bash scripts with AWS CLI commands
- **Rollback Support**: Creates corresponding rollback scripts for permission cleanup
- **Comprehensive Logging**: Structured logging to both console and timestamped log files
- **Debug Mode**: Provides detailed logging and user ID lookup commands

## Prerequisites

- Python 3.6+
- Required Python packages: `psycopg2`, `configparser`, `boto3`
- The Redshift cluster must use AWS Identity Center (IDC) authentication. Use the [RedshiftIDCMigrationUtility](https://github.com/awslabs/amazon-redshift-utils/tree/master/src/RedshiftIDCMigrationUtility) for migrating from Redshift local users and groups/roles.
- An AWS IAM user with an access key to allow programmatic calls to AWS from the AWS CLI
- AWS CLI configured with appropriate permissions
- Admin access to Amazon Redshift cluster with Redshift authentification details stored in AWS Secret Manager
- Lake Formation permissions to grant/revoke access
- A Glue Data Catalog created from the Redshift producer cluster. See [Registering a cluster to the AWS Glue Data Catalog](https://docs.aws.amazon.com/redshift/latest/mgmt/register-cluster.html)

## Installation

1. Install dependencies:
```bash
pip install psycopg2-binary configparser boto3
```

2. Configure AWS CLI (if not already done):
```bash
aws configure
```

## Configuration

Create a `config.ini` file with the following structure:

```ini
[redshift]
host = your-redshift-cluster.region.redshift.amazonaws.com
port = 5439
dbname = your-database-name
# AWS Secrets Manager secret containing Redshift credentials
secret_name = redshift/credentials

[aws]
account_id = 123456789012
region = us-east-1
producer_catalog = your-producer-catalog-name
# AWS CLI profile to use (optional)
config_profile = your-aws-profile
# Identity Store ID for IDC user lookups
identity_store_id = d-1234567890

[parameters]
# Permission source: 'datashare' for datashare permissions, 'local' for local database permissions
permissions_type = datashare
datashare_database_name = your_datashare_db
# Set to true if the datashare database was created with the 'WITH PERMISSIONS' clause
datashare_object_level_permissions = true
```

### Configuration Parameters

#### Redshift Section
- `host`: Redshift cluster endpoint
- `port`: Port number (default: 5439)
- `dbname`: Database name to connect to
- `secret_name`: AWS Secrets Manager secret name containing Redshift credentials (must contain 'username' and 'password' keys)

#### AWS Section
- `account_id`: AWS account ID for Lake Formation
- `region`: AWS region for API calls (used for Secrets Manager)
- `producer_catalog`: Producer catalog name in Lake Formation

- `config_profile`: AWS CLI profile name (optional, used for both AWS CLI and boto3)
- `identity_store_id`: Identity Store ID (required)

#### Parameters Section
- `permissions_type`: Permission source - `datashare` for datashare permissions, `local` for local database permissions
- `datashare_database_name`: Specific datashare database to process (used when permissions_type = datashare)
- `datashare_object_level_permissions`: Enable table-level permissions for datashare mode (`true`/`false`). Use true if the datashare was created 'WITH PERMISSIONS' (used when permissions_type = datashare)


## Usage

**Note**: If `python` command doesn't work, try using `python3` instead.

### Basic Usage

```bash
python rs_privs_to_lf.py
```

### With Custom Config File

```bash
python rs_privs_to_lf.py --config /path/to/config.ini
```

### With Custom Output Directory

```bash
python rs_privs_to_lf.py --output-dir /path/to/output
```

### Debug Mode

```bash
python rs_privs_to_lf.py --debug
```

### Command Line Options

- `--config, -c`: Path to configuration file (default: `config.ini`)
- `--output-dir, -o`: Base directory for generated scripts (default: `output`)
- `--debug`: Enable debug output and generate user ID lookup commands

## Authentication

### Identity Center Authentication
- Processes only users/roles prefixed with the identity provider namespace (automatically detected from SVV_IDENTITY_PROVIDERS)
- Looks up actual IDC user/group IDs using AWS CLI
- Generates ARNs in format: `arn:aws:identitystore:::user/USER_ID` or `arn:aws:identitystore:::group/GROUP_ID`
- Handles role inheritance from granted Redshift roles in both datashare and local modes

## Permission Sources

### Datashare Permissions (`permissions_type = datashare`)
- Extracts permissions from Redshift datashare databases
- Uses `datashare_database_name` parameter to specify target datashare
- Processes users with USAGE permissions on datashare databases
- Uses `SHOW GRANTS` for table-level permission analysis

### Local Database Permissions (`permissions_type = local`)
- Extracts permissions from the local Redshift database (specified in `dbname`)
- Finds IDC users and roles with table-level permissions on local database tables
- Excludes system schemas (`information_schema`, `pg_catalog`, `pg_internal`)
- Supports IDC role inheritance (roles granted to IDC roles)
- Uses `SHOW GRANTS` for table-level permission analysis
- Ignores `datashare_database_name` and `datashare_object_level_permissions` parameters

## Permission Modes

### Schema-Level Permissions (`datashare_object_level_permissions = false`)
- Grants `SELECT` and `DESCRIBE` permissions on all tables in each schema
- Uses `TableWildcard` syntax for efficient bulk permissions
- Based on database `USAGE` permissions

### Table-Level Permissions (`datashare_object_level_permissions = true`)
- **Datashare mode**: Analyzes individual table grants using `SHOW GRANTS`
- **Local mode**: Uses `SHOW GRANTS` for table-level permission analysis
- Preserves specific permissions (`SELECT`, `INSERT`, `UPDATE`, `DELETE`)
- Automatically adds `DESCRIBE` permission
- Filters out unsupported Lake Formation permissions (`RULE`, `TRIGGER`, etc.)
- Handles IDC role inheritance in both modes

## Output Files

The script generates timestamped files in the output directory:

### Generated Scripts
- `lakeformation_permissions_YYYYMMDD_HHMMSS.sh`: Main permission grant script
- `rollback_lakeformation_YYYYMMDD_HHMMSS.sh`: Rollback script to revoke permissions
- `get_user_id_commands_YYYYMMDD_HHMMSS.sh`: Debug script for IDC user ID lookups (debug mode only)

### Log Files
- `logs/lakeformation_migration_YYYYMMDD_HHMMSS.log`: Detailed execution logs with timestamps
- Logs are automatically created in a `logs/` directory (created if it doesn't exist)
- Both console and file logging are enabled simultaneously

### Script Structure
```bash
#!/bin/bash

# AWS LakeFormation permission commands generated from Redshift datashare permissions
# Generated on: 2024-01-15 10:30:45
# Authentication type: IDC
# Object-level permissions: true

aws lakeformation grant-permissions \
  --catalog-id "123456789012" \
  --principal DataLakePrincipalIdentifier=arn:aws:identitystore:::user/12345678-1234-1234-1234-123456789012 \
  --resource '{"Table": {"CatalogId": "123456789012:producer-catalog", "DatabaseName": "schema_name", "Name": "table_name"}}' \
  --permissions "SELECT" "DESCRIBE"
```

## Error Handling

The script includes comprehensive error handling for:
- Missing configuration parameters
- Redshift connection failures
- AWS CLI command failures
- Missing user/group IDs in Identity Center
- Invalid table grants

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify Redshift credentials and network connectivity
2. **Missing User IDs**: Ensure Identity Center users exist and are properly named
3. **Permission Denied**: Verify AWS CLI permissions for Lake Formation operations
4. **Empty Results**: 
   - **Datashare mode**: Check datashare database names and user permissions
   - **Local mode**: Verify you're connected to the correct cluster and database
   - Ensure identity provider namespace prefixed Redshift users/roles exist
5. **No Tables Found**: 
   - **Local mode**: Confirm tables exist in non-system schemas
   - Check if you're connected to the intended Redshift cluster

### Debug Mode
Enable debug mode to see:
- Current database connection details
- Available schemas in the database
- Detailed user and table information
- Sample table grants
- Enhanced logging with DEBUG level messages
- User ID lookup commands for Identity Center authentication
- Query execution details for troubleshooting

## Logging

The utility provides comprehensive logging functionality:

### Log Features
- **Dual Output**: Logs to both console and timestamped log files
- **Automatic Directory Creation**: Creates `logs/` directory if it doesn't exist
- **Structured Format**: Timestamp, log level, and message for each entry
- **Multiple Log Levels**: INFO, ERROR, WARNING, and DEBUG (with --debug flag)
- **Operation Tracking**: Logs all major operations, errors, and progress updates

### Log File Location
- **Default**: `logs/lakeformation_migration_YYYYMMDD_HHMMSS.log` in current directory
- **With Output Directory**: `{output-dir}/logs/lakeformation_migration_YYYYMMDD_HHMMSS.log`

### What Gets Logged
- Utility startup and version information
- Configuration loading and validation
- AWS Secrets Manager operations
- Redshift database connections
- Permission extraction progress
- Script generation results
- Error messages and troubleshooting information
- Debug details (when --debug flag is used)

## Security Considerations

- Use least-privilege IAM policies
- Review generated scripts before execution
- Test in non-production environments first
- Ensure your AWS Secrets Manager secret contains the keys 'username' and 'password'
- The IAM role/user running the script needs secretsmanager:GetSecretValue permission

Authors: adamgatt@amazon.co.uk, ziadwali@amazon.fr

NOTE: This utility is continuously enhanced to close any gaps and add additional functionality. Please send your issues, feedback, and enhancement requests to the authors with subject line: “[issue/feedback/enhancement] Amazon Redshift Permissions to Lake Formation Migration Utility”



