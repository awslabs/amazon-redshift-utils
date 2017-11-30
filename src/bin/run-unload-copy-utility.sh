#!/usr/bin/env bash

echo "Running unload-copy utility"

# Required
CONFIG_FILE=${CONFIG_FILE:-}
AWS_REGION=${AWS_REGION:-us-east-1}

if [ "${CONFIG_FILE}" == "" ]; then echo "Environment Var 'CONFIG_FILE' must be defined"
else
    python UnloadCopyUtility/redshift-unload-copy.py ${CONFIG_FILE} ${AWS_REGION}
    echo "Done"
fi