#!/usr/bin/env python
from __future__ import print_function

import os
import sys

# add the lib directory to the sys path
try:
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
except:
    pass

import config_constants
import boto3
from datetime import datetime, timedelta
import json

DATE_FORMAT = "%Y-%m-%d %H:%M"
PARAMETER_GROUP_KEY = 'parameter-group-name'
APPLY_REGION_KEY = 'apply-region'
CONFIG_KEY = 'configuration'
ACTIVE_INTERVAL_KEY = 'active-interval'
RULESETS_KEY = 'rulesets'
RULESET_NAME_KEY = 'name'

debug = False if config_constants.DEBUG not in os.environ else bool(os.environ[config_constants.DEBUG])


# extract the ruleset from the configuration based on start/end interval expressions
def get_active_ruleset(config):
    for r in config[RULESETS_KEY]:
        active_interval = r[ACTIVE_INTERVAL_KEY]
        active_tokens = active_interval.split('-')

        # extract the components of the interval expression
        start_tokens = active_tokens[0].split(":")
        end_tokens = active_tokens[1].split(":")

        # create date/time values for start and end of the intervals
        now = datetime.now()
        start_time = datetime(now.year, now.month, now.day, int(start_tokens[0]), int(start_tokens[1]))
        end_time = datetime(now.year, now.month, now.day, int(end_tokens[0]), int(end_tokens[1]))

        # if the end hour is less than the start, then add 1 day
        if int(end_tokens[0]) < int(start_tokens[0]):
            end_time = end_time + timedelta(days=1)

        # check of now is between the start and end dates
        if start_time < now < end_time:
            r['start_time'] = start_time
            r['end_time'] = end_time

            return r

    return None


# helper to raise config exceptions
def raise_config_exception(item, message=None):
    if not message:
        raise Exception("Mandatory item %s not found in configuration" % (item))
    else:
        raise Exception(message)


# validate the configuration
def check_config(config):
    if PARAMETER_GROUP_KEY not in config:
        raise_config_exception(PARAMETER_GROUP_KEY)

    if APPLY_REGION_KEY not in config:
        raise_config_exception(APPLY_REGION_KEY)

    if RULESETS_KEY not in config:
        raise_config_exception(RULESETS_KEY)
    else:
        if not (isinstance(config[RULESETS_KEY], list)):
            raise_config_exception(None, message="%s configuration must be a List" % (RULESETS_KEY))


# download the contents of an S3 file
def get_file_contents(location, region_name):
    s3_client = boto3.client('s3', region_name=region_name)

    bucket = location.replace('s3://', '').split("/")[0]
    key = location.replace('s3://' + bucket + "/", '')

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    config_body = obj['Body'].read()
    return json.loads(config_body)


# extract the wlm_configuration from the ruleest, or from the file reference that is configured for the ruleset
def get_wlm_config(ruleset, region_name):
    wlm_config = None

    if CONFIG_KEY in ruleset and isinstance(ruleset[CONFIG_KEY], list):
        wlm_config = json.dumps(ruleset[CONFIG_KEY])
    elif CONFIG_KEY in ruleset and isinstance(ruleset[CONFIG_KEY], basestring):
        if ruleset[CONFIG_KEY].startswith('s3://'):
            wlm_config = json.dumps(get_file_contents(ruleset[CONFIG_KEY], region_name))
    else:
        raise Exception(
            "Malformed Configuration for Ruleset %s. '%s' must be a json based WLM configuration or an S3 file location" % (
            ruleset[RULESET_NAME_KEY], CONFIG_KEY))

    return wlm_config


def run_scheduler(config):
    # check the validity of the configuration
    check_config(config)

    # determine which ruleset to apply right now
    ruleset = get_active_ruleset(config)
    ruleset_name = ruleset[RULESET_NAME_KEY]

    if ruleset is None:
        raise Exception(
            "Unable to resolve a currently active Ruleset. Please confirm that the Ruleset 'active-interval' expressions are contiguous")
    else:
        print(
            "Found Active Ruleset '%s', running from %s to %s" % (
                ruleset_name, ruleset['start_time'].strftime(DATE_FORMAT),
                ruleset['end_time'].strftime(DATE_FORMAT)))

    # create a redshift client for the configured region
    config_region = config[APPLY_REGION_KEY]
    redshift_client = boto3.client('redshift', region_name=config_region)

    parameter_group = config[PARAMETER_GROUP_KEY]

    # extract the wlm config from the ruleset
    wlm_config = get_wlm_config(ruleset, config_region)

    # apply the changes to the parameter group
    print("Applying Ruleset %s to Parameter Group '%s' in Region %s" % (ruleset_name, parameter_group, config_region))

    if debug:
        print("Applying WLM Configuration:")
        print(wlm_config)

    redshift_client.modify_cluster_parameter_group(
        ParameterGroupName=parameter_group,
        Parameters=[
            {
                'ParameterName': 'wlm_json_configuration',
                'ParameterValue': wlm_config,
                'Description': ruleset_name,
                'Source': 'user',
                'ApplyType': 'dynamic',
                'IsModifiable': True
            },
        ])

    if __name__ == "__main__":
        run_scheduler(sys.argv)
