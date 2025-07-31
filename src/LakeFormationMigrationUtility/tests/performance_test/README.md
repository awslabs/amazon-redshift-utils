# Performance Test for Redshift to Lake Formation Migration

## Overview

The `test_perf_redshift_to_lakeformation.py` script is a performance testing tool that creates large-scale test environments to validate the scalability and performance of the Redshift to Lake Formation migration utility. It generates configurable numbers of schemas, tables, users, groups, and roles to test the migration process at scale.

## Prerequisites

- Python 3.6+
- Required Python packages: `psycopg2`, `configparser`, `boto3`
- AWS CLI configured with appropriate permissions
- Admin access to Amazon Redshift cluster
- IAM Identity Center configured
- Lake Formation permissions to grant/revoke access
- AWS Secrets Manager secret containing Redshift credentials

## Configuration

The script uses `test_perf_config.ini` for configuration. Key performance parameters:

### Performance Parameters
- `number_of_schemas`: Number of test schemas to create (default: 20)
- `number_of_tables_per_schema`: Number of tables per schema (default: 30)
- `list_types`: User/role types (etl, adhoc, analyst, admin)
- `number_users_per_type`: Users per type per schema (default: 2)
- `iam_idc_namespace`: Namespace for IDC roles (default: AWSIDC)

### Required Configuration Sections
- **[redshift]**: Redshift connection details and secrets manager configuration
- **[aws]**: AWS account details, region, and IAM Identity Center configuration
- **[parameters]**: Migration parameters and performance test settings

## Test Workflow

The performance test executes the following operations with detailed timing and counting:

### 1. Create Schemas and Tables (`create_schemas_and_tables`)
- Creates configurable number of test schemas in Redshift
- Creates configurable number of tables per schema
- Each table has simple structure: `id_col INTEGER, desc_col VARCHAR(100)`
- **Output**: Total schemas and tables created with timing

### 2. Create IDC Users and Groups (`create_idc_users_and_groups`)
- Creates users and groups in IAM Identity Center for each schema and type
- Assigns users to appropriate groups
- Scales based on: `schemas × types × users_per_type`
- **Output**: Total IDC users and groups created with timing

### 3. Create Redshift Roles (`create_redshift_roles`)
- Creates IDC-namespaced roles in Redshift for each schema and type
- Grants permissions based on role type:
  - **ETL**: USAGE on schema + INSERT, UPDATE, DELETE, SELECT on tables
  - **ADMIN**: ALL permissions on schema and tables
  - **Others**: USAGE on schema + SELECT on tables
- **Output**: Total Redshift roles created with timing

### 4. Execute Migration Script (`execute_rs_privs_to_lf`)
- Runs the `rs_privs_to_lf.py` script with performance test configuration
- Processes all created schemas, roles, and permissions
- **Output**: Migration script execution results and timing

### 5. Generate Cleanup Script (`generate_cleanup_script`)
- Creates `perf_unified_cleanup.py` in the same directory
- Includes cleanup for all created resources
- **Output**: Cleanup script generation timing

## Scale Calculations

For default configuration (20 schemas, 30 tables/schema, 4 types, 2 users/type):
- **Schemas**: 20
- **Tables**: 600 (20 × 30)
- **IDC Users**: 160 (20 × 4 × 2)
- **IDC Groups**: 80 (20 × 4)
- **Redshift Roles**: 80 (20 × 4)

## Usage

### Basic Execution
```bash
python test_perf_redshift_to_lakeformation.py
```

### Expected Output
The script provides comprehensive performance metrics:
- Overall test start and completion times
- Individual function execution times and durations
- Resource creation counts for each operation
- Total elapsed time in seconds

### Sample Output Format
```
Performance test started at: 2024-01-15 10:00:00

Starting create_schemas_and_tables at: 10:00:00
Schema and table creation completed! Created 20 schemas and 600 tables.
Completed create_schemas_and_tables at: 10:05:30 (Duration: 0:05:30)

Starting create_idc_users_and_groups at: 10:05:30
IAM Identity Center setup completed! Created 160 users and 80 groups.
Completed create_idc_users_and_groups at: 10:12:15 (Duration: 0:06:45)

...

Performance test completed at: 2024-01-15 10:45:30
Total elapsed time: 0:45:30
Total elapsed time (seconds): 2730.00s
```

## Generated Files

### Migration Output
- Lake Formation permission scripts in timestamped output directory
- Rollback scripts for permission cleanup
- Debug information and user ID lookup commands

### Cleanup Script
- `perf_unified_cleanup.py`: Complete resource cleanup script in same directory
- Removes all Redshift schemas, tables, roles, and permissions
- Removes all IAM Identity Center users and groups

## Performance Monitoring

The script includes comprehensive performance tracking:
- **Function-level timing**: Start, end, and duration for each major operation
- **Resource counting**: Exact counts of all created resources
- **Overall metrics**: Total test execution time
- **Progress reporting**: Real-time status updates during execution

## Cleanup

After performance testing, run the generated cleanup script:

```bash
python perf_unified_cleanup.py
```

This removes all test resources:
- Redshift schemas and tables (CASCADE)
- Redshift roles (FORCE)
- IAM Identity Center users and groups
- Group memberships

## Performance Considerations

### Scaling Factors
- **Linear scaling**: Most operations scale linearly with resource count
- **IAM Identity Center**: User/group operations may have API rate limits
- **Redshift**: Schema/table creation is typically fast
- **Migration script**: Processing time depends on permission complexity

### Optimization Tips
- Start with smaller numbers for initial testing
- Monitor AWS service limits and quotas
- Consider running during off-peak hours for large tests
- Use appropriate instance sizes for Redshift cluster

## Troubleshooting

### Common Issues
1. **Timeout errors**: Reduce scale parameters for initial testing
2. **API rate limits**: IAM Identity Center operations may be throttled
3. **Resource limits**: Check AWS service quotas
4. **Memory issues**: Large-scale tests may require more memory

### Performance Analysis
- Compare execution times across different scales
- Identify bottlenecks in specific operations
- Monitor AWS CloudWatch metrics during execution
- Use timing data to optimize configuration

## Security Considerations

- Test credentials stored in AWS Secrets Manager
- Use least-privilege IAM policies
- Clean up test resources promptly
- Avoid running performance tests in production environments
- Review generated Lake Formation commands before applying

## Configuration Examples

### Small Scale Test
```ini
number_of_schemas = 5
number_of_tables_per_schema = 10
number_users_per_type = 1
```

### Large Scale Test
```ini
number_of_schemas = 100
number_of_tables_per_schema = 50
number_users_per_type = 5
```

### Custom Types Test
```ini
list_types = admin, analyst, readonly, poweruser
number_users_per_type = 3
```