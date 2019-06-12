#!/usr/bin/env python
from __future__ import print_function

import os
import sys

# add the lib directory to the sys path
try:
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib/ColumnEncodingUtility"))
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib/AnalyzeVacuumUtility"))
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib/SystemTablePersistence"))
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib/amazon-redshift-monitoring"))
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib/WorkloadManagementScheduler"))
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
except:
    pass

import boto3
import analyze_schema_compression
import analyze_vacuum
import redshift_monitoring
import config_constants
import snapshot_system_stats
import wlm_scheduler
import common

region_key = 'AWS_REGION'
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
    config_location = config_constants.LOCAL_CONFIG

    if event is not None and 'ConfigLocation' in event:
        config_location = event['ConfigLocation']

    global debug
    config = common.get_config(config_location, current_region, debug)

    if config_constants.DEBUG in config and config[config_constants.DEBUG]:
        debug = True

    if debug:
        print("Configuration File Contents:")
        print(config)

    # extract the password
    use_password = common.get_password(kms_connection, config, debug)

    # bind the password back into the configuration so we can pass it forward
    config[config_constants.DB_PASSWORD] = use_password

    run_utilities = []

    if event is not None and "ExecuteUtility" in event:
        if event["ExecuteUtility"] == config_constants.COLUMN_ENCODING:
            run_utilities.append(config_constants.COLUMN_ENCODING)
        elif event["ExecuteUtility"] == config_constants.ANALYZE_VACUUM:
            run_utilities.append(config_constants.ANALYZE_VACUUM)
        elif event["ExecuteUtility"] == config_constants.ANALYZE:
            run_utilities.append(config_constants.ANALYZE)
        elif event["ExecuteUtility"] == config_constants.VACUUM:
            run_utilities.append(config_constants.VACUUM)
        elif event["ExecuteUtility"] == config_constants.MONITORING:
            run_utilities.append(config_constants.MONITORING)
        elif event["ExecuteUtility"] == config_constants.TABLE_PERSISTENCE:
            run_utilities.append(config_constants.TABLE_PERSISTENCE)
        elif event["ExecuteUtility"] == config_constants.WLM_SCHEDULER:
            run_utilities.append(config_constants.WLM_SCHEDULER)
    elif 'utilities' in config:
        # run each utility, if requested
        if config_constants.COLUMN_ENCODING in config["utilities"]:
            run_utilities.append(config_constants.COLUMN_ENCODING)

        if config_constants.ANALYZE_VACUUM in config["utilities"]:
            run_utilities.append(config_constants.ANALYZE_VACUUM)

        if config_constants.ANALYZE in config["utilities"]:
            run_utilities.append(config_constants.ANALYZE)

        if config_constants.VACUUM in config["utilities"]:
            run_utilities.append(config_constants.VACUUM)

        if config_constants.MONITORING in config["utilities"]:
            run_utilities.append(config_constants.MONITORING)

        if config_constants.TABLE_PERSISTENCE in config["utilities"]:
            run_utilities.append(config_constants.TABLE_PERSISTENCE)

        if config_constants.WLM_SCHEDULER in config["utilities"]:
            run_utilities.append(config_constants.WLM_SCHEDULER)
    else:
        print("No Utilities configured to run. Exiting!")
        return

    results = []
    for util in run_utilities:
        if util == config_constants.COLUMN_ENCODING:
            print("Running %s" % util)
            analyze_schema_compression.configure(**config)
            encoding_result = analyze_schema_compression.run()
            results.append(encoding_result)
        elif util == config_constants.ANALYZE_VACUUM:
            print("Running %s" % util)
            analyze_result = analyze_vacuum.run_analyze_vacuum(**config)
            if analyze_result == 0:
                results.append("OK")
        elif util == config_constants.ANALYZE:
            print("Running %s" % util)
            # turn on correct flag
            config[config_constants.DO_ANALYZE] = True
            config[config_constants.DO_VACUUM] = False

            analyze_result = analyze_vacuum.run_analyze_vacuum(**config)
            if analyze_result == 0:
                results.append("OK")
        elif util == config_constants.VACUUM:
            print("Running %s" % util)
            # turn on correct flag
            config[config_constants.DO_ANALYZE] = False
            config[config_constants.DO_VACUUM] = True

            analyze_result = analyze_vacuum.run_analyze_vacuum(**config)
            if analyze_result == 0:
                results.append("OK")
        elif util == config_constants.MONITORING:
            print("Running %s" % util)
            redshift_monitoring.monitor_cluster([config, os.environ])
        elif util == config_constants.TABLE_PERSISTENCE:
            print("Running %s" % util)
            snapshot_system_stats.snapshot([config, os.environ])
        elif util == config_constants.WLM_SCHEDULER:
            print("Running %s" % util)
            wlm_scheduler.run_scheduler(config)

    print("Processing Complete")
    return results


if __name__ == "__main__":
    event_handler(None if len(sys.argv) == 1 else sys.argv[1], None)
