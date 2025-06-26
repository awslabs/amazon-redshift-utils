import boto3
import psycopg2
import csv
import logging
import configparser
from io import StringIO
from botocore.exceptions import ClientError

# setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(config_file='redshift_unload.ini'):
    """Load configuration from file"""
    #config = configparser.ConfigParser()

    config = configparser.ConfigParser(inline_comment_prefixes=('#',))
    config.read(config_file)
    return config

def validate_config(config):
    """Validate that all required configuration parameters are present based on cluster_type"""
    if 'REDSHIFT' not in config:
        raise ValueError("Missing configuration section: REDSHIFT")
    
    cluster_type = config['REDSHIFT'].get('cluster_type')
    if not cluster_type:
        raise ValueError("Missing 'cluster_type' parameter in [REDSHIFT] section. Must be 'provisioned' or 'serverless'.")

    required_params = {
        'AWS': ['region', 's3_bucket'],
        'S3_FILES': ['roles', 'users', 'role_memberships']
    }

    if cluster_type == 'provisioned':
        required_params['REDSHIFT'] = ['cluster_id', 'db_user', 'db_name', 'host', 'port']
    elif cluster_type == 'serverless':
        required_params['REDSHIFT'] = ['db_user', 'db_name', 'workgroup_name', 'port']
    else:
        raise ValueError(f"Invalid cluster_type: {cluster_type}. Must be 'provisioned' or 'serverless'.")
    
    for section, params in required_params.items():
        if section not in config:
            raise ValueError(f"Missing configuration section: {section}")
        
        for param in params:
            if param not in config[section]:
                raise ValueError(f"Missing configuration parameter: {section}.{param}")

def check_bucket_encryption(bucket_name):
    try:
        # Create S3 client
        s3_client = boto3.client('s3')
        
        # Get bucket encryption configuration
        response = s3_client.get_bucket_encryption(Bucket=bucket_name)
        
        # If we get here, bucket is encrypted
        encryption_rules = response['ServerSideEncryptionConfiguration']['Rules']
        print(f"Bucket {bucket_name} is encrypted with configuration:")
        print(encryption_rules)
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ServerSideEncryptionConfigurationNotFoundError':
            print(f"Bucket {bucket_name} is not encrypted")
            return False
        else:
            print(f"Error checking bucket encryption: {str(e)}")
            raise

def get_redshift_credentials(config):
    """Get Redshift credentials based on cluster type."""
    try:
        cluster_type = config['REDSHIFT']['cluster_type']

        if cluster_type == 'provisioned':
            client = boto3.client('redshift', region_name=config['AWS']['region'])
            credentials = client.get_cluster_credentials(
                DbUser=config['REDSHIFT']['db_user'],
                DbName=config['REDSHIFT']['db_name'],
                ClusterIdentifier=config['REDSHIFT']['cluster_id']
            )
            return credentials
        elif cluster_type == 'serverless':
            client = boto3.client('redshift', region_name=config['AWS']['region'])
            credentials = client.get_cluster_credentials(
                DbUser=config['REDSHIFT']['db_user'],
                DbName=config['REDSHIFT']['db_name'],
                ClusterIdentifier=f"redshift-serverless-{config['REDSHIFT']['workgroup_name']}"
            )
            client = boto3.client('redshift-serverless', region_name=config['AWS']['region'])
            workgroup = client.get_workgroup(workgroupName=config['REDSHIFT']['workgroup_name'])
            endpoint = workgroup['workgroup']['endpoint']
            host = endpoint['address']
            return {
               "DbUser": credentials['DbUser'],
               "DbPassword": credentials['DbPassword'],
               "Host": host
            }   
        else:
            raise ValueError(f"Unsupported cluster_type: {cluster_type}")

    except Exception as e:
        logger.error(f"Error getting credentials: {str(e)}")
        return None

def connect_to_redshift(config):
    creds = get_redshift_credentials(config)
    if creds is None:
        return None
    
    try:
        cluster_type = config['REDSHIFT']['cluster_type']
        
        if cluster_type == 'provisioned':
            host = config['REDSHIFT']['host']
        elif cluster_type == 'serverless':
            host = creds["Host"] # For serverless, host comes from credentials
        else:
            raise ValueError(f"Unsupported cluster_type: {cluster_type}")

        conn = psycopg2.connect(
            dbname=config['REDSHIFT']['db_name'],
            user=creds["DbUser"],
            password=creds["DbPassword"],
            host=host,
            port=config['REDSHIFT']['port'],
            sslmode='require', 
            timeout=None
        )
        logger.info("Connected to Redshift successfully!")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Redshift: {e}")
        return None

def fetch_data_from_redshift(conn, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        cursor_description = cursor.description
        result = cursor.fetchall()
        cursor.close()
        return result, cursor_description
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return None, None

def write_to_s3(data, cursor_description, s3_key, config):
    try:
        s3_client = boto3.client('s3', region_name=config['AWS']['region'])
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)

        if data:
            csv_writer.writerow([desc[0] for desc in cursor_description])

        for row in data:
            csv_writer.writerow(row)

        s3_client.put_object(
            Bucket=config['AWS']['s3_bucket'],
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )
        logger.info(f"Data successfully written to S3: {s3_key}")
    except Exception as e:
        logger.error(f"Error writing to S3: {e}")

def get_and_write_data(conn, query, s3_key, config):
    data, cursor_description = fetch_data_from_redshift(conn, query)
    if data is not None:
        write_to_s3(data, cursor_description, s3_key, config)

def main():
    try:
         # Load configuration
         config = load_config()
         validate_config(config)

         # Validate if input bucket is encrypted
         bucket_name = config['AWS']['s3_bucket']
         if check_bucket_encryption(bucket_name) is False:
            logger.error(f"{bucket_name} is not encrypted.  Please enable bucket encryption")

         # Connect to Redshift
         conn = connect_to_redshift(config)
         if conn is None:
            logger.error("Failed to connect to Redshift. Exiting program.")
            return

         # SQL Queries (common to both)
         query_roles = """
              select groname from pg_group
              where groname not like '%:%'
              union
              SELECT role_name FROM svv_roles 
              where role_owner <> 'rdsdb' and role_name not ilike 'awsidc%' and role_name not like '%:%' """
    
         query_users = """
         SELECT user_name FROM svv_user_info 
         where superuser = 'false' 
         and user_name not ilike 'awsidc%' 
         and user_name not ilike 'IAM%'
         and user_name not like '%:%'
         """
    
         query_role_memberships = """
         SELECT u.usename, g.groname 
         FROM (SELECT groname, grolist, generate_series(1, array_upper(grolist, 1)) AS i 
         FROM pg_group) AS g
         JOIN pg_user u ON g.grolist[i] = u.usesysid
         where u.usename not like '%:%'
         and g.groname not like '%:%'
         UNION
         SELECT user_name,role_name FROM svv_user_grants
         where user_name not like '%:%'
         and role_name not like '%:%'
         """

         # Execute queries and write to S3
         get_and_write_data(conn, query_roles, config['S3_FILES']['roles'], config)
         get_and_write_data(conn, query_users, config['S3_FILES']['users'], config)
         get_and_write_data(conn,query_role_memberships,config['S3_FILES']['role_memberships'], config)

         conn.close()

    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return

if __name__ == "__main__":
    main()
