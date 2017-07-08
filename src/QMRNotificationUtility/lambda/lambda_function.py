#!/usr/bin/env python

from __future__ import print_function

# Copyright 2016-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
# http://aws.amazon.com/apache2.0/
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
import sys
import os

# add the lib directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))

import base64
import pg8000
import datetime
import boto3
import json
import collections

__version__ = "1.4"

print('Loading function')

#### Configuration

ssl = True
interval = '5 minutes'
debug = False

##################
pg8000.paramstyle = "qmark"

def get_env_var(name):
    return os.environ[name] if name in os.environ else None

# resolve cluster connection settings and SNS from environment if set

user = get_env_var('user')
enc_password = get_env_var('enc_password')
host = get_env_var('host')
port = int(get_env_var('port'))
database = get_env_var('database')
sns_arn = get_env_var('sns_arn')

clusterid = host.split(".")[0]

try:
    kms = boto3.client('kms')
    password = kms.decrypt(CiphertextBlob=base64.b64decode(enc_password))['Plaintext']
except:
    print('KMS access failed: Exception %s' % sys.exc_info()[1])

try:
    sns = boto3.resource('sns')
    platform_endpoint = sns.PlatformEndpoint('{sns_arn}'.format(sns_arn = sns_arn))
except:
    print('SNS access failed: Exception %s' % sys.exc_info()[1])


def run_command(cursor, statement):
    if debug:
        print("Running Statement: %s" % statement)

    return cursor.execute(statement)
def publish_to_sns(message):
    try:
        if debug:
            print('Publishing messages to SNS topic')

        # Publish a message.
        response = platform_endpoint.publish(
                  Subject='Redshift Query Monitoring Rule Notifications',
                  Message=message,
                  MessageStructure='string',
                  MessageAttributes={
                       'RedshiftQueryMonitoringRuleNotifications': {
                              'StringValue': 'Redshift Query Monitoring Rule Notifications',
                              'DataType': 'String'
                       }
                  }

            )
        return  response

    except:
        print(' Failed to publish messages to SNS topic: exception %s' % sys.exc_info()[1])
        return 'Failed'


def query_redshift(conn):

    try:

        sql_query = """select /* Lambda Redshift Query Monitoring SNS Notifications */  userid,query,service_class,trim(rule) as rule,trim(action) as action,recordtime from stl_wlm_rule_action WHERE userid > 1 AND recordtime >= GETDATE() - INTERVAL '{interval}' order by recordtime desc;""".format(interval = interval)
        print(sql_query)

        # connect to Redshift and run the query
        cursor = conn.cursor()
        run_command(cursor,sql_query)
        result = cursor.fetchall()

        objects_list = []
        for row in result:
            userid,query,service_class,rule,action,recordtime  = row
            d = collections.OrderedDict()
            d['clusterid'] = clusterid
            d['database'] = database
            d['userid'] = userid
            d['query'] = query
            d['service_class'] = service_class
            d['rule'] = rule
            d['action'] = action
            d['recordtime'] = recordtime.isoformat()
            objects_list.append(d)

        #Publish to SNS if any rows fetched
        if len(objects_list) == 0:
            print('No rows to publish to SNS')
        else:
            query_result_json = json.dumps(objects_list)
            print(query_result_json)
            response= publish_to_sns(query_result_json)

        cursor.close()
        conn.commit()
        print('Completed Succesfully ')

    except:
        print('Query Failed: exception %s' % sys.exc_info()[1])
        return 'Failed'

def lambda_handler(event, context):

    try:
        if debug:
            print('Connect to Redshift: %s' % host)
        conn = pg8000.connect(database=database, user=user, password=password, host=host, port=port, ssl=ssl)
    except:
        print('Redshift Connection Failed: exception %s' % sys.exc_info()[1])
        return 'Failed'

    if debug:
        print('Succesfully Connected Redshift Cluster')

    # Collect Query Monitoring Rule metrics and Publish to SNS topic
    query_redshift(conn)
    conn.close()

    return 'Finished'

if __name__ == "__main__":
    lambda_handler(sys.argv[0], None)
