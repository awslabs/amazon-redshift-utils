#!/usr/bin/env python
from __future__ import print_function

import os
import sys

# add the lib directory to the sys path
try:
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib/ColumnEncodingUtility"))
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib/AnalyzeVacuumUtility"))
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib/amazon-redshift-monitoring"))
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
except:
    pass

import json
import base64
import boto3
import analyze_schema_compression
import analyze_vacuum
import redshift_monitoring
import config_constants

region_key = 'AWS_REGION'
COLUMN_ENCODING = "ColumnEncodingUtility"
ANALYZE_VACUUM = "AnalyzeVacuumUtility"
MONITORING = "Monitoring"
LOCAL_CONFIG = "config.json"
debug = False if config_constants.DEBUG not in os.environ else os.environ[config_constants.DEBUG]

def safe_get(value, obj):
    if value in obj:
        return obj[value]
    else:
        return None


def event_handler(event, context):
    current_region = 'us-east-1'
    if region_key not in os.environ:
        print("Warning - using default region %s" % current_region)
    else:
        current_region = os.environ[region_key]

    kms_connection = boto3.client('kms', region_name=current_region)

    # load the configuration file
    config_location = LOCAL_CONFIG

    if event is not None and 'ConfigLocation' in event:
        config_location = event['ConfigLocation']

    if config_location.startswith("s3://"):
        # load the configuration file from S3
        s3_client = boto3.client('s3', region_name=current_region)

        bucket = config_location.replace('s3://', '').split("/")[0]
        key = config_location.replace('s3://' + bucket + "/", '')

        obj = s3_client.get_object(Bucket=bucket, Key=key)
        config_body = obj['Body'].read()
        config = json.loads(config_body)
    elif config_location == LOCAL_CONFIG:
        # load from the local configuration
        if not os.path.isfile("config.json"):
            raise Exception("Unable to resolve local config.json file")
        else:
            config_file = open("config.json", 'r')
            config = json.load(config_file)

            if config is None:
                raise Exception("No Configuration Found")
    else:
        raise Exception("Unsupported configuration location %s" % config_location)

    config_detail = config["configuration"]

    # convert the provided configuration into something that the utilities we're calling will understand
    config_detail = config_constants.normalise_config(config_detail)

    if config_constants.DEBUG in config_detail and config_detail[config_constants.DEBUG]:
        debug = True

    # KMS crypto authorisation context
    auth_context = None
    if config_constants.KMS_AUTH_CONTEXT in config_detail:
        auth_context = config_detail[config_constants.KMS_AUTH_CONTEXT]
        # convert to json
        auth_context = json.loads(auth_context)

        if debug:
            print("Using Authorisation Context for decryption")
            print(auth_context)

    # resolve password
    encrypted_password = base64.b64decode(config_detail[config_constants.ENCRYPTED_PASSWORD])

    if encrypted_password != "" and encrypted_password is not None:
        if auth_context is not None:
            # decrypt the password using KMS
            use_password = kms_connection.decrypt(CiphertextBlob=encrypted_password, EncryptionContext=auth_context)[
                'Plaintext']
        else:
            # decrypt the password using KMS
            use_password = kms_connection.decrypt(CiphertextBlob=encrypted_password)[
                'Plaintext']
    else:
        raise Exception("Unable to run Utilities without a configured Password")

    config_detail[config_constants.DB_PASSWORD] = use_password

    if debug:
        print("Using Configuration:")
        print(config_detail)

    run_utilities = []

    if event is not None and "ExecuteUtility" in event:
        if event["ExecuteUtility"] == COLUMN_ENCODING:
            run_utilities.append(COLUMN_ENCODING)
        elif event["ExecuteUtility"] == ANALYZE_VACUUM:
            run_utilities.append(ANALYZE_VACUUM)
        elif event["ExecuteUtility"] == MONITORING:
            run_utilities.append(MONITORING)
    elif 'utilities' in config:
        # run each utility, if requested
        if COLUMN_ENCODING in config["utilities"]:
            run_utilities.append(COLUMN_ENCODING)

        if ANALYZE_VACUUM in config["utilities"]:
            run_utilities.append(ANALYZE_VACUUM)

        if MONITORING in config["utilities"]:
            run_utilities.append(MONITORING)
    else:
        print("No Utilities configured to run. Exiting!")
        return

    results = []
    for util in run_utilities:
        if util == COLUMN_ENCODING:
            print("Running %s" % util)
            analyze_schema_compression.configure(**config_detail)
            encoding_result = analyze_schema_compression.run()
            results.append(encoding_result)
        elif util == ANALYZE_VACUUM:
            print("Running %s" % util)
            analyze_result = analyze_vacuum.run_analyze_vacuum(**config_detail)
            if analyze_result == 0:
                results.append("OK")
        elif util == MONITORING:
            print("Running %s" % util)
            redshift_monitoring.monitor_cluster([config_detail, os.environ])

    print("Processing Complete")
    return results


if __name__ == "__main__":
    event_handler(None if len(sys.argv) == 1 else sys.argv[1], None)
