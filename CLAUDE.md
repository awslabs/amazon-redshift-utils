# CLAUDE.md

This file provides guidance for Claude Code when working with the amazon-redshift-utils repository.

## Project Overview

Amazon Redshift Utils is a collection of scripts and utilities for Amazon Redshift, a petabyte-scale data warehouse. The repository contains Python scripts, SQL scripts, stored procedures, and admin utilities to optimize Redshift cluster performance.

## Repository Structure

```
/
├── src/                          # Main source directory
│   ├── AdminScripts/             # Diagnostic scripts for clusters
│   ├── AdminViews/               # Views for cluster management and DDL generation
│   ├── AnalyzeVacuumUtility/     # Automates VACUUM and ANALYZE operations
│   ├── ColumnEncodingUtility/    # Applies optimal column encoding to tables
│   ├── CloudDataWarehouseBenchmark/  # Performance benchmarking workloads
│   ├── ManifestGenerator/        # Generates manifest files for COPY commands
│   ├── MetadataTransfer/         # Metadata transfer utilities
│   ├── QMRNotificationUtility/   # Query Monitoring Rule notifications via Lambda/SNS
│   ├── RedshiftAutomation/       # Lambda-based automation module
│   ├── RedshiftIDCMigrationUtility/  # IDC migration utility
│   ├── SimpleReplay/             # Workload capture and replay utility
│   ├── StoredProcedures/         # Example stored procedures
│   ├── SystemTablePersistence/   # System table persistence utilities
│   ├── UnloadCopyUtility/        # Data migration between clusters via S3
│   ├── UserLastLogin/            # User login tracking
│   ├── WorkloadManagementScheduler/  # WLM scheduling
│   ├── bin/                      # Docker entrypoint scripts
│   ├── config_constants.py       # Shared configuration constants
│   ├── redshift_utils_helper.py  # Common helper functions
│   └── requirements.txt          # Main Python dependencies
├── Dockerfile                    # Docker build for running utilities
└── README.md                     # Project documentation
```

## Key Dependencies

The main dependencies (from `src/requirements.txt`):
- `pg8000` - PostgreSQL/Redshift database driver
- `boto3` - AWS SDK for Python
- `redshift-connector` - Native Redshift connector
- `pgpasslib` - pgpass file support
- `shortuuid` - UUID generation

## Running Utilities

### From Command Line
```bash
cd src
python3 ./<folder>/<utility> <args>
```

### Via Docker
Build the image:
```bash
docker build -t amazon-redshift-utils .
```

Run utilities:
```bash
# Analyze & Vacuum
docker run --net host --rm -it -e DB=my-database amazon-redshift-utils analyze-vacuum

# Column Encoding
docker run --net host --rm -it -e DB=my-database amazon-redshift-utils column-encoding

# Unload/Copy
docker run --net host --rm -it -e CONFIG_FILE=s3://... amazon-redshift-utils unload-copy
```

Use `--env-file` for environment variables:
```bash
docker run --net host --rm -it --env-file redshift_utils.env amazon-redshift-utils analyze-vacuum
```

## Testing

Tests exist primarily for the UnloadCopyUtility:
- Unit tests: `src/UnloadCopyUtility/tests/redshift_unload_copy_unittests.py`
- Regression tests: `src/UnloadCopyUtility/tests/redshift_unload_copy_regressiontests.py`
- DDL helpers tests: `src/UnloadCopyUtility/tests/ddl_helpers_tests.py`

Run tests with:
```bash
cd src/UnloadCopyUtility
python -m pytest tests/
```

## Authentication Options

1. **KMS-encrypted password**: Base64-encoded KMS-encrypted string in config
2. **pgpass file**: Use `.pgpass` file (requires rebuilding modules with `build.sh`)
3. **PGPASS environment variable**: Set `$PGPASS`

## Key Utilities Overview

### AnalyzeVacuumUtility
Automates VACUUM and ANALYZE based on table statistics, unsorted rows, and system alerts. Configure with `--analyze-flag` and `--vacuum-flag`.

### ColumnEncodingUtility
Analyzes compression and generates scripts to apply optimal column encoding using `ANALYZE COMPRESSION`.

### UnloadCopyUtility
Migrates data between clusters using UNLOAD to S3 (KMS-encrypted) followed by COPY. Configurable via JSON config files.

### SimpleReplay
Captures and replays workloads from audit logs. Includes extract and replay phases with configurable YAML settings.

### RedshiftAutomation
Lambda-based automation for running utilities on schedule via CloudWatch Events.

## Build Scripts

- `src/RedshiftAutomation/build.sh` - Builds Lambda deployment package
- `src/QMRNotificationUtility/lambda/build.sh` - Builds QMR Lambda package
- `src/UnloadCopyUtility/encryptValue.sh` - Encrypts values with KMS

## Code Style Notes

- Python 3.8+ compatible
- Uses standard Python conventions
- SQL scripts use Redshift-specific syntax
- Configuration typically via environment variables or JSON/YAML files
