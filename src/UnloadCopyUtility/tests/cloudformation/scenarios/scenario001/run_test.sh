#!/usr/bin/env bash

source ${HOME}/variables.sh

SCENARIO=scenario001
SOURCE_SCHEMA="ssb"
SOURCE_TABLE="dwdate"
TARGET_SCHEMA="public"
TARGET_TABLE="${SOURCE_TABLE}"
PYTHON="python3"

DESCRIPTION="Perform Unload Copy with password encrypted using KMS, expect target location to be correct. "
DESCRIPTION="${DESCRIPTION}Use invocation as per documentation  --s3-config-file s3://${CopyUnloadBucket}/my-unload-copy-config.json --region eu-west-1"
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

start_step "Push config file to S3 "
aws s3 cp "${HOME}/${SCENARIO}.json" "s3://${CopyUnloadBucket}/my-unload-copy-config.json" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r


start_step "Generate DDL for table ${SOURCE_SCHEMA}.${SOURCE_TABLE} on target cluster"
#Extract DDL
psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "select ddl from admin.v_generate_tbl_ddl where schemaname='${SOURCE_SCHEMA}' and tablename='${SOURCE_TABLE}';" | awk '/CREATE TABLE/{flag=1}/ ;$/{flag=0}flag' | sed "s/${SOURCE_SCHEMA}/${TARGET_SCHEMA}/" >${HOME}/${SCENARIO}.ddl.sql 2>>${STDERROR}
increment_step_result $?
cat ${HOME}/${SCENARIO}.ddl.sql >>${STDOUTPUT} 2>>${STDERROR}
increment_step_result $?
stop_step ${STEP_RESULT}

start_step "Drop table ${TARGET_SCHEMA}.${TARGET_TABLE} in target cluster if it exists"
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "DROP TABLE IF EXISTS ${TARGET_SCHEMA}.${TARGET_TABLE};" 2>>${STDERROR} | grep "DROP TABLE"  >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Create table ${TARGET_SCHEMA}.${TARGET_TABLE} in target cluster"
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -f ${HOME}/${SCENARIO}.ddl.sql | grep "CREATE TABLE"  >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r


start_step "Run Unload Copy Utility"
source ${VIRTUAL_ENV_PY36_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
cd ${HOME}/amazon-redshift-utils/src/UnloadCopyUtility && ${PYTHON} redshift_unload_copy.py ${HOME}/${SCENARIO}.json eu-west-1 >>${STDOUTPUT} 2>>${STDERROR}
EXPECTED_COUNT=`psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "select 'count='||count(*) from ${SOURCE_SCHEMA}.${SOURCE_TABLE};" | grep "count=[0-9]*"|awk -F= '{ print $2}'` >>${STDOUTPUT} 2>>${STDERROR}
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "select 'count='||count(*) from ${TARGET_SCHEMA}.${TARGET_TABLE};" | grep "count=${EXPECTED_COUNT}" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r
deactivate

start_step "Remove config file from S3"
aws s3 rm "s3://${CopyUnloadBucket}/my-unload-copy-config.json" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

stop_scenario