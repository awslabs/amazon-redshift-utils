#!/usr/bin/python
from boto3 import client

import logging
import logging.config
logging.config.fileConfig('config/logging.conf')
logger = logging.getLogger()

# Generates IAM credentials for the cluster and specified user. 
# The user is expected to be already present on the cluster. This user will not be created automatically. 
def getiamcredentials(dbhost=None,dbname=None, dbuser=None):
    # Redshift <clusterid>.<randomid>.<region>.redshift.amazonaws.com:5439
    clusterid = dbhost.split('.')[0]
    region = dbhost.split('.')[2]
    
    try:
        redshift = client('redshift',region_name=region )
        credentials = redshift.get_cluster_credentials(DbUser=dbuser, DbName=dbname,
                                                              ClusterIdentifier=clusterid, AutoCreate=False)
        return credentials
    except Exception as err:
        logger.error("Failed to generate temporary IAM Credentials")
        logger.error(err)
        exit()
        return 'Failed'

