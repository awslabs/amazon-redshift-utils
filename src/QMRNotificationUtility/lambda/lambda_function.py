# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import sys
import os
import pprint

# add the lib directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))

import base64
import datetime
import boto3
import json
import time
import collections
from botocore.config import Config
import botocore.session as bc

__version__ = "2.0"

#### Configuration

ssl = True
interval = '5 minutes'
debug = False

##################

def get_env_var(name):
    return os.environ[name] if name in os.environ else None

# resolve cluster connection settings and SNS from environment if set
user = get_env_var('user')
enc_password = get_env_var('enc_password')
host = get_env_var('host')
port = int(get_env_var('port'))
database = get_env_var('database')
sns_arn = get_env_var('sns_arn')
region = get_env_var('region')

clusterid = host.split(".")[0]

client_redshift = None
platform_endpoint = None



# Use Data API to obtain credentials
def execute_api_sql(queries, dbCluster=None, dbWorkgroup=None, dbPort=5439, dbName=None, dbUser=None):
    global client_redshift
    
    # New process - use Data API with temporary credentials
    try:
        if( dbCluster is not None):
            print("Connecting to cluster: " + dbCluster)
            response = client_redshift.execute_statement(
                ClusterIdentifier=dbCluster,
                Database=dbName,
                DbUser=dbUser,
                Sql=queries,
                StatementName="QMRNotificationUtility-v%s" % __version__,
                WithEvent=False
                )
        elif( dbWorkgroup is not None ):
            print("Connecting to workgroup: " + dbWorkgroup)
            response = client_redshift.execute_statement(
                WorkgroupName=dbWorkgroup,  # Required for serverless
                Database=dbName,
                Sql=queries,
                StatementName="QMRNotificationUtility-v%s" % __version__,
                WithEvent=False
                )
                
        response_id = response['Id']
        print("Query response id: " + str(response_id))
        
        statement_finished = False
        response_data = None
        
        while( statement_finished is False):
            # Sleep and poll for 5 seconds
            print("Sleeping...")
            time.sleep(1)
            
            response_data = client_redshift.describe_statement(
                    Id=response_id
                    )
                    
            pprint.pprint(response_data)
            
            if( response_data['Status'] == 'FINISHED'):
                statement_finished = True
            elif( response_data['Status'] == 'FAILED'):
                #logger.info(json.dumps(response_data, indent=4,default=str))
                print(response_data)
                statement_finished = True
            else:
                #logger.info("Statement status: " + response_data['Status'])
                print(response_data['Status'])

        # Now get the results
        result_rows = response_data["ResultRows"]
        has_results = response_data["HasResultSet"]

        if( (result_rows > 0) and (has_results == True) ):
            # Return the data
            query_results = client_redshift.get_statement_result(
                    Id=response_id
                    )
            return query_results["Records"]

        else:
            return None

    except:
        print('Redshift Connection Failed: exception %s' % sys.exc_info()[1])
        
def query_redshift():
        
    # Global event parameters
    global user
    global host
    global port
    global database
    global sns_arn
    global region

    try:
        sql_query = """select /* Lambda Redshift Query Monitoring SNS Notifications */  userid,query,service_class,trim(rule) as rule,trim(action) as action,recordtime from stl_wlm_rule_action WHERE userid > 1 AND recordtime >= GETDATE() - INTERVAL '{interval}' order by recordtime desc;""".format(interval = interval)
        
        # Use Data API function to connect to Redshift
        print("Connecting as Redshift user: " + user)
        
        # Use Data API function to connect to Redshift
        result_rows = execute_api_sql(sql_query, host, None, port, database, user)
        
        if( result_rows is None):
            print("No results from QMR query")
        
        else:
            # Get the results from the returned dictionary
            objects_list = []
            
            for row in result_rows:
                
                print(row)
                userid,query,service_class,rule,action,recordtime  = row
                d = collections.OrderedDict()
                d['clusterid'] = clusterid
                d['database'] = database
                d['userid'] = userid
                d['query'] = query
                d['service_class'] = service_class
                d['rule'] = rule
                d['action'] = action
                d['recordtime'] = recordtime #--.isoformat()
                
                objects_list.append(d)
    
            #Publish to SNS if any rows fetched
            if len(objects_list) == 0:
                print('No rows to publish to SNS')
            else:
                print("Publishing rows to SNS")
                query_result_json = json.dumps(objects_list)
                response = publish_to_sns(query_result_json)
        
        print('Completed Succesfully ')
        return 'Success'
        
    except:
        print('Query Failed: exception %s' % sys.exc_info()[1])
        return 'Failed'
    
def publish_to_sns(message):
    global platform_endpoint
    
    try:
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
            
        print("Published message...")
        return response

    except:
        print(' Failed to publish messages to SNS topic: exception %s' % sys.exc_info()[1])
        return 'Failed'


def lambda_handler(event, context):
    global client_redshift
    global platform_endpoint
    
    # Global event parameters
    global user
    global host
    global port
    global database
    global sns_arn
    global region
    
    # Setup the Redshift Data API session
    try:
        bc_session = bc.get_session()
        
        session = boto3.Session(
                botocore_session=bc_session,
                region_name=region,
            )
        
        # Setup the client
        client_redshift = session.client("redshift-data")
        print("Data API client successfully loaded")
    except:
        print('redshift-data session failed: Exception %s' % sys.exc_info()[1])

    # Setup the SNS client resource
    try:
        sns = boto3.resource('sns')
        platform_endpoint = sns.PlatformEndpoint('{sns_arn}'.format(sns_arn = sns_arn))
        
    except: 
        print('SNS access failed: Exception %s' % sys.exc_info()[1])
        
    # Execute the QMR query
    query_redshift()

