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

import pg8000
import traceback
import argparse
import queries
from utils import getiamcredentials

#### Static Configuration
ssl = True
##################

__version__ = "1.0"
pg8000.paramstyle = "qmark"


def update_user_last_login(cluster=None, dbPort=5439, dbName=None, dbUser=None):
    logger.info("Starting updating user last login information for the cluster %s" % (cluster) )
    credentials = getiamcredentials(cluster,dbName, dbUser )
    logger.debug( ( "IAM User:%s , Expiration: %s " % (credentials['DbUser'], credentials['Expiration']  )   ) )
    # Extract temp credentials     
    user=credentials['DbUser']
    pwd =credentials['DbPassword']

    # Connect to the cluster using the above credentials. 
    try:
        conn = pg8000.connect(database=dbName, user=user, password=pwd, host=cluster, port=int(dbPort), ssl=ssl)
        logger.debug(("Successfully connected to the cluster %s:%s. DatabaseName: %s and DB User: %s "  %  (cluster, dbPort,dbName, dbUser) ) )
    except:
        logger.error('Redshift Connection Failed: exception %s' % sys.exc_info()[1])
        raise    

    # create a new cursor for methods to run through
    cursor = conn.cursor()
    # set application name
    set_name = "set application_name to 'RedshiftUserLastLogin-v%s'" % __version__    
    cursor.execute(set_name)

    # Check if required objects are present or not. 
    logger.debug("Query to check all objects are present or not: %s " % (queries.CHECK_DB_OBJECTS) )
    cursor.execute(queries.CHECK_DB_OBJECTS)
    tablecount = int(cursor.fetchone()[0])
    # tablecount = int (tablecountresults[0])
    
    if tablecount < 2:
        # If tables any of the tables are missing then setup schema of objects. 
        logger.info("Missing objects")    
        try:
            cursor.execute(queries.CREATE_SCHEMA)
            cursor.execute(queries.CREATE_STAGE_TABLE)
            cursor.execute(queries.CREATE_TARGET_TABLE)
            conn.commit()
            logger.info("Successfully created History schema and UserLastLogin stage and target tables ")
        except:
            logger.error("Failed to set up schema or objects: exception %s" % sys.exc_info()[1])
            # Close the connection
            conn.close()
            logger.error("Failed to update user last login information for the cluster %s" % (cluster) )
            raise    
    else:
        #No attempt to create objects will be made if the objects already exist.
        logger.debug("Checked for missing objects and there no missing. Proceeding to update user login information.")

    # Execute DMLs against the. 
    try:
        logger.debug("Truncating the stage table using the statement: %s " % (queries.TRUNCATE_STAGE_TABLE)  )
        #Truncate stage table
        cursor.execute(queries.TRUNCATE_STAGE_TABLE)
        logger.info("Finished truncating stage table")
        # Use Upsert pattern to update the target table. 
        #Load stage table 
        logger.debug("Inserting data into stage table using the statement: %s " % (queries.LOAD_STAGE_TABLE)  )
        cursor.execute(queries.LOAD_STAGE_TABLE)
        logger.info("Finished loading staging table")
        #Update target table
        logger.debug("Updating last login timestamp for existing users in target table from stage table using the query: %s " % (queries.UPDATE_TARGET_TABLE_FROM_STAGE)  )
        cursor.execute(queries.UPDATE_TARGET_TABLE_FROM_STAGE)
        logger.info("Finished updating last login timestamp for existing users in target table from stage table")
        #Insert new records into target table 
        logger.debug("Inserting new rows for users that don't exist in target table from stage table using the query: %s " % (queries.INSERT_TARGET_TABLE_FROM_STAGE)  )
        cursor.execute(queries.INSERT_TARGET_TABLE_FROM_STAGE)
        logger.info("Finished inserting last login timestamp for new users in target table from stage table")
        conn.commit()
    except:
        logger.error("Failed to run DML statements to update user details: exception %s" % sys.exc_info()[1])
        # Close the connection
        conn.close()
        logger.error("Failed to update user last login information for the cluster %s" % (cluster) )
        raise    
        
    # Close the objects
    cursor.close()
    conn.close()
    logger.info("Successfully updated user last login information for the cluster %s" % (cluster) )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", help="<Full DNS Endpoint of the cluster endpoint>")
    parser.add_argument("--dbPort", help="<cluster port>")
    parser.add_argument("--dbName", help="<database on cluster having monitoring tables>")
    parser.add_argument("--dbUser", help="<superuser or monitoring username to connect>")
    args = parser.parse_args()
    
    cluster=args.cluster
    dbPort=args.dbPort
    dbName=args.dbName
    dbUser=args.dbUser
    
    update_user_last_login(cluster, dbPort, dbName, dbUser )

    
