#!/bin/bash
# This script bootstraps the installation of pgbench
# initiates it and sets it on the cron for continuous execution
set -e

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
      -s|--secret-id) SECRET_ID="$2"; shift;;
      --debug) set -x;;
      *)
      POSITIONAL+=("$1"); shift;;
  esac
done
set -- "${POSITIONAL[@]}"

function install_system_packages() {
    /usr/bin/yum -y install postgresql-contrib jq
}

function initiate_pgbench() {
  echo "${PG_HOST}:${PG_PORT}:*:${PG_USER}:${PG_PASSWORD}" | tee ~/.pgpass
  chmod 0600 ~/.pgpass
  pgbench -i -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" "${PG_DBNAME}"
}

function schedule_pgbench() {
  BENCHMARKING_SCRIPT="*/2 * * * * root pgbench -c 10 -T 30 -h ${PG_HOST} -p ${PG_PORT} -U ${PG_USER} ${PG_DBNAME}"
  echo "$BENCHMARKING_SCRIPT" | tee /etc/cron.d/pgbench_cron
}

install_system_packages
REGION=$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq .region -r)
SECRET=$(aws secretsmanager get-secret-value --secret-id "$SECRET_ID" --region "$REGION" | jq .SecretString -r)
PG_PASSWORD=$(echo "$SECRET" | jq .password -r)
PG_HOST=$(echo "$SECRET" | jq .host -r)
PG_PORT=$(echo "$SECRET" | jq .port -r)
PG_USER=$(echo "$SECRET" | jq .username -r)
PG_DBNAME=$(echo "$SECRET" | jq .dbname -r)
initiate_pgbench
schedule_pgbench
echo "Bootstrapping complete."
