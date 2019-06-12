#!/usr/bin/env bash

set -e

echo "Running analyze-vacuum utility"

# Required
DB=${DB:-}
DB_USER=${DB_USER:-}
DB_PWD=${DB_PWD:-}
DB_HOST=${DB_HOST:-}
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-}

# Optional with Defaults
AWS_REGION=${AWS_REGION:-us-east-1}
DB_PORT=${DB_PORT:-5439}
REQUIRE_SSL=${REQUIRE_SSL:-false}
SCHEMA_NAME=${SCHEMA_NAME:-public}
DEBUG=${DEBUG:-false}
SLOT_COUNT=${SLOT_COUNT:-1}
IGNORE_ERRORS=${IGNORE_ERRORS:-false}
ANALYZE_FLAG=${ANALYZE_FLAG:-true}
VACUUM_FLAG=${VACUUM_FLAG:-true}
VACUUM_PARAMETER=${VACUUM_PARAMETER:-full}
MIN_UNSORTED_PCT=${MIN_UNSORTED_PCT:-5}
MAX_UNSORTED_PCT=${MAX_UNSORTED_PCT:-50}
STATS_OFF_PCT=${STATS_OFF_PCT:-10}
PREDICATE_COLS=${PREDICATE_COLS:-false}
MAX_TABLE_SIZE_MB=${MAX_TABLE_SIZE_MB:-(700*1024)}
MIN_INTERLEAVED_SKEW=${MIN_INTERLEAVED_SKEW:-1.4}
MIN_INTERLEAVED_CNT=${MIN_INTERLEAVED_CNT:-0}
SUPPRESS_CLOUDWATCH=${SUPPRESS_CLOUDWATCH:-false}

# Optional no defaults
DB_CONN_OPTS=${DB_CONN_OPTS:-}
TABLE_NAME=${TABLE_NAME:-}
BLACKLISTED_TABLES=${BLACKLISTED_TABLES:-}
OUTPUT_FILE=${OUTPUT_FILE:-}
QUERY_GROUP=${QUERY_GROUP:-}

if [ "${DB}" == "" ]; then echo "Environment Var 'DB' must be defined"; exit 1
elif [ "${DB_USER}" == "" ]; then echo "Environment Var 'DB_USER' must be defined"; exit 1
elif [ "${DB_PWD}" == "" ]; then echo "Environment Var 'DB_PWD' must be defined"; exit 1
elif [ "${DB_HOST}" == "" ]; then echo "Environment Var 'DB_HOST' must be defined"; exit 1
else
    if [ "${DB_CONN_OPTS}" != "" ]; then DB_CONN_OPTS_CMD="--db-conn-opts ${DB_CONN_OPTS}"; fi
    if [ "${TABLE_NAME}" != "" ]; then TABLE_NAME_CMD="--table-name ${TABLE_NAME}"; fi
    if [ "${BLACKLISTED_TABLES}" != "" ]; then BLACKLISTED_TABLES_CMD="--blacklisted-tables ${BLACKLISTED_TABLES}"; fi
    if [ "${OUTPUT_FILE}" != "" ]; then OUTPUT_FILE_CMD="--output-file ${OUTPUT_FILE}"; fi
    if [ "${QUERY_GROUP}" != "" ]; then QUERY_GROUP_CMD="--query_group ${QUERY_GROUP}"; fi

    python AnalyzeVacuumUtility/analyze-vacuum-schema.py \
        --db ${DB} \
        --db-user ${DB_USER} \
        --db-pwd ${DB_PWD} \
        --db-host ${DB_HOST} \
        --db-port ${DB_PORT} \
        --require-ssl ${REQUIRE_SSL} \
        --schema-name ${SCHEMA_NAME} \
        --debug ${DEBUG} \
        --slot-count ${SLOT_COUNT} \
        --ignore-errors ${IGNORE_ERRORS} \
        --analyze-flag ${ANALYZE_FLAG} \
        --vacuum-flag ${VACUUM_FLAG} \
        --vacuum-parameter ${VACUUM_PARAMETER} \
        --min-unsorted-pct ${MIN_UNSORTED_PCT} \
        --max-unsorted-pct ${MAX_UNSORTED_PCT} \
        --stats-off-pct ${STATS_OFF_PCT} \
        --predicate-cols ${PREDICATE_COLS} \
        --max-table-size-mb ${MAX_TABLE_SIZE_MB} \
        --min-interleaved-skew ${MIN_INTERLEAVED_SKEW} \
        --min-interleaved-cnt ${MIN_INTERLEAVED_CNT} \
        --suppress-cloudwatch ${SUPPRESS_CLOUDWATCH} \
        ${DB_CONN_OPTS_CMD} ${TABLE_NAME_CMD} ${BLACKLISTED_TABLES_CMD} ${OUTPUT_FILE_CMD} ${QUERY_GROUP_CMD}
    echo "done"
fi
