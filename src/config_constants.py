COLUMN_ENCODING = "ColumnEncodingUtility"
ANALYZE_VACUUM = "AnalyzeVacuumUtility"
ANALYZE = "Analyze"
VACUUM = "Vacuum"
MONITORING = "Monitoring"
TABLE_PERSISTENCE = "SystemTablePersistence"
WLM_SCHEDULER = "WlmScheduler"
LOCAL_CONFIG = "config.json"

DB_NAME = "db"
DB_HOST = "db_host"
DB_PORT = "db_port"
DB_USER = "db_user"
ENCRYPTED_PASSWORD = "encrypted_pwd"
DB_PASSWORD = "db_pwd"
CMK_ALIAS = "cmk_alias"
TABLE_NAME = "table_name"
SCHEMA_NAME = "schema_name"
TARGET_SCHEMA = "target_schema"
CLUSTER_NAME = "cluster_name"
DO_ANALYZE = "do_analyze"
DO_VACUUM = "do_vacuum"
DO_EXECUTE = "do_execute"
DEBUG = "debug"
FORCE = "force"
QUERY_GROUP = "query_group"
QUERY_SLOT_COUNT = "query_slot_count"
COMPROWS = "comprows"
DROP_OLD_DATA = "drop_old_data"
IGNORE_ERRORS = "ignore_errors"
ANALYZE_COL_WIDTH = "analyze_col_width"
THREADS = "threads"
OUTPUT_FILE = "output_file"
SSL = "ssl"
BLACKLISTED_TABLES = "blacklisted_tables"
AGG_INTERVAL = "agg_interval"
VACUUM_PARAMETER = "vacuum_parameter"
MIN_UNSORTED_PCT = "min_unsorted_pct"
MAX_UNSORTED_PCT = "max_unsorted_pct"
STATS_OFF_PCT = "stats_off_pct"
PREDICATE_COLS = "predicate_cols"
SUPPRESS_CLOUDWATCH = "suppress_cw"
MAX_TBL_SIZE_MB = "max_table_size_mb"
MIN_INTERLEAVED_SKEW = "min_interleaved_skew"
MIN_INTERLEAVED_COUNT = "min_interleaved_count"
KMS_AUTH_CONTEXT = "kms_auth_context"
SYSTABLE_CLEANUP_AFTER_DAYS = "systable_cleanup_after_days"
STATEMENT_TIMEOUT = "statement_timeout"
S3_UNLOAD_LOCATION = "s3_unload_location"
S3_UNLOAD_ROLE_ARN = "s3_unload_role_arn"

config_aliases = {
    "db": ["db", "DatabaseName"],
    "db_host": ["dbHost", "clusterEndpoint", "HostName", "cluster_endpoint"],
    "db_port": ["dbPort", "HostPort"],
    "db_user": ["dbUser", "DbUser"],
    "encrypted_pwd": ["dbPassword", 'EncryptedPassword'],
    "cmk_alias": ["cmkAlias"],
    "table_name": ["analyzeTable"],
    "schema_name": ["analyzeSchema"],
    "target_schema": ["targetSchema"],
    "cluster_name": ["ClusterName", "clusterName"],
    "do_analyze": ["doAnalyze", "analyze_flag", "analyze-flag"],
    "do_vacuum": ["doVacuum", "vacuum_flag", 'vacuum-flag'],
    "do_execute": ["do-execute"],
    "query_group ": ["queryGroup"],
    "query_slot_count": ["querySlotCount"],
    "drop_old_data": ["dropOldData"],
    "ignore_errors": ["ignoreErrors"],
    "output_file": ["outputFile"],
    "ssl": ["ssl-option", 'require-ssl'],
    "blacklisted_tables": ["blacklistedTables"],
    "agg_interval": ["aggregationInterval"],
}


def normalise_config(config):
    config_out = {}

    def add_to_config(constant):
        v = extract_value(constant, config)
        if v is not None:
            config_out[constant] = v

    add_to_config(DB_NAME)
    add_to_config(DB_HOST)
    add_to_config(DB_PORT)
    add_to_config(DB_USER)
    add_to_config(ENCRYPTED_PASSWORD)
    add_to_config(CMK_ALIAS)
    add_to_config(TABLE_NAME)
    add_to_config(SCHEMA_NAME)
    add_to_config(TARGET_SCHEMA)
    add_to_config(CLUSTER_NAME)
    add_to_config(DO_ANALYZE)
    add_to_config(DO_VACUUM)
    add_to_config(DO_EXECUTE)
    add_to_config(DEBUG)
    add_to_config(FORCE)
    add_to_config(QUERY_GROUP)
    add_to_config(QUERY_SLOT_COUNT)
    add_to_config(COMPROWS)
    add_to_config(DROP_OLD_DATA)
    add_to_config(IGNORE_ERRORS)
    add_to_config(ANALYZE_COL_WIDTH)
    add_to_config(THREADS)
    add_to_config(OUTPUT_FILE)
    add_to_config(SSL)
    add_to_config(BLACKLISTED_TABLES)
    add_to_config(AGG_INTERVAL)
    add_to_config(VACUUM_PARAMETER)
    add_to_config(MIN_UNSORTED_PCT)
    add_to_config(MAX_UNSORTED_PCT)
    add_to_config(STATS_OFF_PCT)
    add_to_config(PREDICATE_COLS)
    add_to_config(SUPPRESS_CLOUDWATCH)
    add_to_config(MAX_TBL_SIZE_MB)
    add_to_config(MIN_INTERLEAVED_SKEW)
    add_to_config(MIN_INTERLEAVED_COUNT)
    add_to_config(KMS_AUTH_CONTEXT)
    add_to_config(SYSTABLE_CLEANUP_AFTER_DAYS)
    add_to_config(S3_UNLOAD_LOCATION)
    add_to_config(S3_UNLOAD_ROLE_ARN)

    return config_out


def extract_value(constant, config):
    if constant in config:
        return config[constant]
    else:
        if constant not in config_aliases:
            return None
        else:
            value = config_aliases[constant]
            for v in value:
                if v in config and config[v] != "":
                    return config[v]

            return None
