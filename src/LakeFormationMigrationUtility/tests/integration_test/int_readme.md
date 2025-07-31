# Integration Test for Redshift to Lake Formation Migration

## Overview

The `test_int_redshift_to_lakeformation.py` script is an integration test that validates the complete workflow of migrating permissions from Amazon Redshift to AWS Lake Formation. It creates test users, roles, and permissions in both Redshift and IAM Identity Center, then executes the migration utility to generate Lake Formation permission commands.

## Prerequisites

- Python 3.6+
- Required Python packages: `psycopg2`, `configparser`, `boto3`
- AWS CLI configured with appropriate permissions
- Admin access to Amazon Redshift cluster
- IAM Identity Center configured
- Lake Formation permissions to grant/revoke access
- AWS Secrets Manager secret containing Redshift credentials

## Configuration

The script uses `test_int_config.ini` for configuration. Ensure the following sections are properly configured:

### Required Configuration Sections

- **[redshift]**: Redshift connection details and secrets manager configuration
- **[aws]**: AWS account details, region, and IAM Identity Center configuration
- **[parameters]**: Migration parameters including database names and permission settings

## Test Workflow

The integration test performs the following steps in sequence:

### 1. Create IAM Users (`create_iam_users`)
- Creates IAM users and roles for testing IAM authentication
- Only runs when `auth_type = IAM` in configuration
- **Output**: Count of IAM users and roles created

### 2. Setup Redshift Users and Roles (`setup_redshift_users_and_roles`)
- Creates test users in Redshift (when not using IAM auth)
- Creates business roles: admin, analyst, etl, adhoc
- Creates technical role: tickit_restricted
- Grants appropriate permissions based on `object_level_permissions` setting
- **Output**: Count of Redshift users and roles created

### 3. Setup IAM Identity Center (`setup_idc_users_and_groups`)
- Creates users and groups in IAM Identity Center
- Assigns users to appropriate groups
- Only runs when `auth_type = IDC` in configuration
- **Output**: Count of IDC users and groups created

### 4. Create AWSIDC Roles (`create_awsidc_roles`)
- Creates AWSIDC-prefixed roles in Redshift
- Creates AWSIDC user for direct permissions testing
- Grants permissions to AWSIDC roles based on their local counterparts
- **Output**: Count of IDC roles created in Redshift

### 5. Execute Migration Script (`execute_ds_privs_to_lf`)
- Runs the `rs_privs_to_lf.py` script with test configuration
- Generates Lake Formation permission commands
- **Output**: Script execution results and generated files

### 6. Generate Cleanup Script (`generate_unified_cleanup_script`)
- Creates `int_unified_cleanup.py` script for resource cleanup
- Includes cleanup for Redshift, IAM, and IAM Identity Center resources

## Test Users and Roles

### Standard Test Users
- `test_user_admin` - Administrative user
- `test_user_analyst1`, `test_user_analyst2` - Analyst users
- `test_user_analyst3` - Standalone analyst (direct permissions)
- `test_user_etl1`, `test_user_etl2` - ETL users
- `test_user_adhoc` - Ad-hoc query user

### Business Roles
- `test_role_bn_admin` - Full administrative access
- `test_role_bn_analyst` - Read-only access to all tables
- `test_role_bn_etl` - ETL operations access
- `test_role_bn_adhoc` - Limited access to specific tables

### Technical Roles
- `test_role_tn_tickit_restricted` - Restricted access to specific tables

## Usage

### Basic Execution
```bash
python test_int_redshift_to_lakeformation.py
```

### Expected Output
The script provides detailed timing information and resource counts:
- Start and end times for each function
- Duration of each operation
- Count of resources created in each step
- Total elapsed time for the entire test

### Generated Files
- Lake Formation permission scripts in timestamped output directory
- Rollback scripts for permission cleanup
- `int_unified_cleanup.py` for complete resource cleanup

## Cleanup

After testing, run the generated cleanup script to remove all test resources:

```bash
python int_unified_cleanup.py
```

This will clean up:
- Redshift users, roles, and permissions
- IAM users and roles (when applicable)
- IAM Identity Center users and groups (when applicable)

## Authentication Types

### IAM Authentication (`auth_type = IAM`)
- Creates IAM users and roles
- Creates IAM-prefixed users in Redshift
- Tests IAM-based authentication flow

### IDC Authentication (`auth_type = IDC`)
- Creates users and groups in IAM Identity Center
- Creates AWSIDC-prefixed roles in Redshift
- Tests Identity Center-based authentication flow

## Performance Monitoring

The script includes comprehensive timing and counting features:
- Individual function execution times
- Resource creation counts for each step
- Total test execution time
- Detailed progress reporting

## Troubleshooting

### Common Issues
1. **Configuration Errors**: Verify `test_int_config.ini` settings
2. **Permission Issues**: Ensure AWS credentials have required permissions
3. **Connection Failures**: Check Redshift connectivity and secrets manager access
4. **Resource Conflicts**: Run cleanup script before retesting

### Debug Information
The script provides detailed output including:
- Resource creation confirmations
- Error messages with context
- Timing information for performance analysis
- Resource counts for validation

## Security Considerations

- Test credentials are stored in AWS Secrets Manager
- IAM policies should follow least-privilege principles
- Test resources should be cleaned up after use
- Review generated Lake Formation commands before production use