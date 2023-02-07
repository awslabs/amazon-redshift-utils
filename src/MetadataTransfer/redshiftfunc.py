# Generates IAM credentials for the cluster and specified user.
# The user is expected to be already present on the cluster. This user will not be created automatically.
import logging
import boto3

logger = logging.getLogger('MetadataTransfer')

def getiamcredentials(dbhost=None,dbname=None, dbuser=None):
    # Redshift <clusterid>.<randomid>.<region>.redshift.amazonaws.com:5439
    logger.info("Getting credentials: " + dbhost )
    clusterid = dbhost.split('.')[0]
    region = dbhost.split('.')[2]
    logger.info("Cluster: " + clusterid + " Region: " + region)

    try:
        redshift = boto3.client('redshift',region_name=region )

        logger.info("Redshift client: " + region)
        credentials = redshift.get_cluster_credentials(DbUser=dbuser, DbName=dbname,
                                                              ClusterIdentifier=clusterid, AutoCreate=False)
        return credentials
    except Exception as err:
        logger.error(err)
        return 'Failed'
