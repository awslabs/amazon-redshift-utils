#!/usr/bin/env bash

source ${HOME}/variables.sh

SCENARIO=scenario006
SOURCE_SCHEMA="ssb"
SOURCE_TABLE="customer"
TARGET_SCHEMA="${SOURCE_SCHEMA}"
TARGET_TABLE="${SOURCE_TABLE}"
PYTHON="python3"

DESCRIPTION="Perform Unload Copy with password encrypted using KMS, create table in target location. "
DESCRIPTION="${DESCRIPTION}Target schema does not exist, should be auto-created. "
DESCRIPTION="${DESCRIPTION}Use ${PYTHON}."

start_scenario "${DESCRIPTION}"

start_step "Create configuration JSON to copy ${SOURCE_SCHEMA}.${SOURCE_TABLE} of source cluster to ${TARGET_SCHEMA}.${TARGET_TABLE} on target cluster"

cat >${HOME}/${SCENARIO}.json <<EOF
{
  "unloadSource": {
    "clusterEndpoint": "${SourceClusterEndpointAddress}",
    "clusterPort": ${SourceClusterEndpointPort},
    "connectPwd": "${KMSEncryptedPassword}",
    "connectUser": "${SourceClusterMasterUsername}",
    "db": "${SourceClusterDBName}",
    "schemaName": "${SOURCE_SCHEMA}",
    "tableName": "${SOURCE_TABLE}"
  },
  "s3Staging": {
    "aws_iam_role": "${S3CopyRole}",
    "path": "s3://${CopyUnloadBucket}/${SCENARIO}/",
    "deleteOnSuccess": "True",
    "region": "eu-west-1",
    "kmsGeneratedKey": "True"
  },
  "copyTarget": {
    "clusterEndpoint": "${TargetClusterEndpointAddress}",
    "clusterPort": ${TargetClusterEndpointPort},
    "connectPwd": "${KMSEncryptedPassword}",
    "connectUser": "${SourceClusterMasterUsername}",
    "db": "${SourceClusterDBName}",
    "schemaName": "${TARGET_SCHEMA}",
    "tableName": "${TARGET_TABLE}"
  }
}
EOF

cat ${HOME}/${SCENARIO}.json >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Drop schema ${TARGET_SCHEMA} in target cluster if exists"
DDL="DROP SCHEMA IF EXISTS ${TARGET_SCHEMA} CASCADE"
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "${DDL}" | grep "DROP SCHEMA"  >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Run Unload Copy Utility"
source ${VIRTUAL_ENV_PY36_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
cd ${HOME}/amazon-redshift-utils/src/UnloadCopyUtility && ${PYTHON} redshift_unload_copy.py --log-level debug --destination-table-auto-create --destination-schema-auto-create ${HOME}/${SCENARIO}.json eu-west-1 >>${STDOUTPUT} 2>>${STDERROR}
EXPECTED_COUNT=`psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "select 'count='||count(*) from ${SOURCE_SCHEMA}.${SOURCE_TABLE};" | grep "count=[0-9]*"|awk -F= '{ print $2}'`  >>${STDOUTPUT} 2>>${STDERROR}
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "select 'count='||count(*) from ${TARGET_SCHEMA}.${TARGET_TABLE};" | grep "count=${EXPECTED_COUNT}" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r
deactivate

stop_scenario