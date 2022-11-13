# !/usr/bin/python

import logging
import logging.config
logging.config.fileConfig('config/logging.conf')
logger = logging.getLogger()

import os
import sys

# add the lib directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
sys.path.append(os.path.join(os.path.dirname(__file__), "utils"))
logger.debug("Appeneded 'lib' and 'utils' to the list sys path")

import boto3
from botocore.config import Config
import traceback
import time
import argparse
import queries
import json
from utils import getiamcredentials

#### Static Configuration
ssl = True
##################

__version__ = "1.2"

client = None

def execute_api_sql(queries, dbCluster=None, dbWorkgroup=None, dbPort=5439, dbName=None, dbUser=None):
    # New process - use Data API with temporary credentials
    try:
        if( dbCluster is not None):
            response = client.execute_statement(
                ClusterIdentifier=dbCluster,
                Database=dbName,
                DbUser=dbUser,
                Sql=queries,
                WithEvent=False
                )
        elif( dbWorkgroup is not None ):
            response = client.execute_statement(
                Database=dbName,
                Sql=queries,
                WithEvent=False,
                WorkgroupName=dbWorkgroup  # Required for serverless
                )
        
        response_id = response['Id']

        statement_finished = False
        response_data = None

        while( statement_finished is False):
            # Sleep and poll for 5 seconds
            time.sleep(5)

            response_data = client.describe_statement(
                    Id=response_id
                    )

            if( response_data['Status'] == 'FINISHED'):
                statement_finished = True
            elif( response_data['Status'] == 'FAILED'):
                logger.info(json.dumps(response_data, indent=4,default=str))
                statement_finished = True
            else:
                logger.info("Statement status: " + response_data['Status'])

        # Now get the results
        result_rows = response_data["ResultRows"]
        has_results = response_data["HasResultSet"]

        if( (result_rows > 0) and (has_results == True) ):
            # Return the data
            query_results = client.get_statement_result(
                    Id=response_id
                    )
            return query_results["Records"]

        else:
            return None

    except:
        logger.error('Redshift Connection Failed: exception %s' % sys.exc_info()[1])



def update_user_last_login(dbCluster=None, dbWorkgroup=None, dbPort=5439, dbName=None, dbUser=None):

    # New process - use Data API with temporary credentials
    set_name = "set application_name to 'RedshiftUserLastLogin-v%s'" % __version__    

    # Set the application name:
    query_data = execute_api_sql(set_name, dbCluster, dbWorkgroup, dbPort, dbName, dbUser)

    # Check if required objects are present or not. 
    logger.debug("Query to check all objects are present or not: %s " % (queries.CHECK_DB_OBJECTS) )

    query_data = execute_api_sql(queries.CHECK_DB_OBJECTS, dbCluster, dbWorkgroup, dbPort, dbName, dbUser)

    # Extract the actual table count from the query results in dictionary form:
    q1 = query_data[0]
    q2 = q1[0]
    tablecount = q2["longValue"]

    if( tablecount < 2):
        # If tables any of the tables are missing then setup schema of objects. 
        logger.info("Missing objects - create schema and tables")    
        try:
            query_data_schema = execute_api_sql(queries.CREATE_SCHEMA, dbCluster, dbWorkgroup, dbPort, dbName, dbUser)

            query_data_stage = execute_api_sql(queries.CREATE_STAGE_TABLE, dbCluster, dbWorkgroup, dbPort, dbName, dbUser)

            query_data_target = execute_api_sql(queries.CREATE_TARGET_TABLE, dbCluster, dbWorkgroup, dbPort, dbName, dbUser)

            logger.info("Successfully created History schema and UserLastLogin stage and target tables ")
        except:
            logger.error("Failed to set up schema or objects: exception %s" % sys.exc_info()[1])
            logger.error("Failed to update user last login information for the cluster %s" % (cluster) )
            raise    
    else:
        #No attempt to create objects will be made if the objects already exist.
        logger.debug("Checked for missing objects and there no missing. Proceeding to update user login information.")

    # Execute DMLs against the. 
    try:
        logger.debug("Truncating the stage table using the statement: %s " % (queries.TRUNCATE_STAGE_TABLE)  )
        #Truncate stage table
        query_data = execute_api_sql(queries.TRUNCATE_STAGE_TABLE, dbCluster, dbWorkgroup, dbPort, dbName, dbUser)

        logger.info("Finished truncating stage table")
        # Use Upsert pattern to update the target table. 
        #Load stage table 
        
        logger.debug("Inserting data into stage table using the statement: %s " % (queries.LOAD_STAGE_TABLE)  )

        # If serverless then execute modified query:
        query_data = execute_api_sql(queries.LOAD_STAGE_TABLE, dbCluster, dbWorkgroup, dbPort, dbName, dbUser)

        logger.info("Finished loading staging table")
        #Update target table
        logger.debug("Updating last login timestamp for existing users in target table from stage table using the query: %s " % (queries.UPDATE_TARGET_TABLE_FROM_STAGE)  )

        query_data = execute_api_sql(queries.UPDATE_TARGET_TABLE_FROM_STAGE, dbCluster, dbWorkgroup, dbPort, dbName, dbUser)
        logger.info("Finished updating last login timestamp for existing users in target table from stage table")
        #Insert new records into target table 
        logger.debug("Inserting new rows for users that don't exist in target table from stage table using the query: %s " % (queries.INSERT_TARGET_TABLE_FROM_STAGE)  )

        query_data = execute_api_sql(queries.INSERT_TARGET_TABLE_FROM_STAGE, dbCluster, dbWorkgroup, dbPort, dbName, dbUser)
        logger.info("Finished inserting last login timestamp for new users in target table from stage table")
    except:
        logger.error("Failed to run DML statements to update user details: exception %s" % sys.exc_info()[1])
        logger.error("Failed to update user last login information for the cluster %s" % (cluster) )
        raise    
        
    logger.info("Successfully updated user last login information for the cluster %s" % (cluster) )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", help="<Full DNS Endpoint of the cluster endpoint>")
    parser.add_argument("--workgroup", help="<Serverless workgroup name>")
    parser.add_argument("--dbPort", help="<cluster port>", required=True)
    parser.add_argument("--dbName", help="<database on cluster having monitoring tables>",required=True)
    parser.add_argument("--dbUser", help="<superuser or monitoring username to connect. (Provisioned cluster only)>")
    parser.add_argument("--region", help="<region of the cluster or endpoint>", required=True)
    args = parser.parse_args()
    
    cluster=args.cluster
    dbPort=args.dbPort
    dbName=args.dbName
    if( args.dbUser is not None):
        dbUser=args.dbUser
    else:
        dbUser = None
    dbCluster=args.cluster
    dbWorkgroup=args.workgroup
    dbRegion=args.region

    my_config = Config(
            region_name = dbRegion
            )

    client = boto3.client('redshift-data', config=my_config)

    # Print starting message
    if ((dbCluster is None) and (dbWorkgroup is not None)):
        logger.info("Starting updating user last login information for the workgroup `%s`" % (dbWorkgroup) )
    elif(( dbWorkgroup is None ) and ( dbCluster is not None )):
        logger.info("Starting updating user last login information for the cluster `%s`" % (dbCluster) )



    update_user_last_login( dbCluster, dbWorkgroup, dbPort, dbName, dbUser )

    
