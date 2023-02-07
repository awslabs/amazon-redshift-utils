import re

import boto3
import gzip
import io
import json
import logging
import logging.handlers
import os
import redshift_connector
import time
from urllib.parse import urlparse
import yaml
import base64
from botocore.exceptions import ClientError
import datetime

logger = logging.getLogger("SimpleReplayLogger")

LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def init_logging(level=logging.INFO):
    """ Initialize logging to stdio """
    logger = logging.getLogger("SimpleReplayLogger")
    logger.setLevel(logging.DEBUG)
    logging.Formatter.converter = time.gmtime
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(get_log_formatter())
    logger.addHandler(ch)
    return logger


def set_log_level(level):
    """ Change the log level for the default (stdio) logger. Logfile always logs DEBUG. """
    logger = logging.getLogger("SimpleReplayLogger")
    for handler in logger.handlers:
        if type(handler) == logging.StreamHandler:
            handler.setLevel(level)


def add_logfile(filename, dir="simplereplay_logs", level=logging.DEBUG, backup_count=2, preamble=''):
    """ Additionally log to a logfile """
    os.makedirs(dir, exist_ok=True)
    filename = f"{dir}/{filename}"
    file_exists = os.path.isfile(filename)
    fh = logging.handlers.RotatingFileHandler(filename, backupCount=backup_count)

    # if the file exists from a previous run, rotate it
    if file_exists:
        fh.doRollover()

    # dump the preamble to the file first
    if preamble:
        with open(filename, "w") as fp:
            fp.write(preamble.rstrip() + '\n\n' + '-' * 40 + "\n")

    fh.setLevel(level)
    fh.setFormatter(get_log_formatter())
    logger = logging.getLogger("SimpleReplayLogger")
    logger.info(f"Logging to {filename}")
    logger.addHandler(fh)
    logger.debug("== Initializing logfile ==")


def get_log_formatter(process_idx=None, job_id=None):
    """ Define the log format, with the option to prepend process and job/thread to each message """
    format = "[%(levelname)s] %(asctime)s"
    if process_idx is not None:
        format += f" [{process_idx}]"
    if job_id is not None:
        format += " (%(threadName)s)"
    format += " %(message)s"
    formatter = logging.Formatter(format, datefmt=LOG_DATE_FORMAT)
    formatter.converter = time.gmtime
    return formatter


def prepend_ids_to_logs(process_idx=None, job_id=None):
    """ Update logging to prepend process_id and / or job_id to all emitted logs """
    logger = logging.getLogger("SimpleReplayLogger")
    for handler in logger.handlers:
        handler.setFormatter(get_log_formatter(process_idx, job_id))


def log_version():
    """ Read the VERSION file and log it """
    logger = logging.getLogger("SimpleReplayLogger")
    try:
        with open("VERSION", "r") as fp:
            logger.info(f"Version {fp.read().strip()}")
    except:
        logger.warning(f"Version unknown")


def db_connect(interface="psql",
               host=None, port=5439, username=None, password=None, database=None,
               odbc_driver=None,
               drop_return=False):
    """ Connect to the database using the method specified by interface (either psql or odbc)
      :param drop_return: if True, don't store returned value.
    """
    if interface == "psql":
        conn = redshift_connector.connect(user=username, password=password, host=host,
                                          port=port, database=database)

        # if drop_return is set, monkey patch driver to not store result set in memory
        if drop_return:
            def drop_data(self, data) -> None:
                pass

            # conn.handle_DATA_ROW = drop_data
            conn.message_types[redshift_connector.core.DATA_ROW] = drop_data

    elif interface == "odbc":
        import pyodbc
        if drop_return:
            raise Exception("drop_return not supported for odbc")

        odbc_connection_str = "Driver={}; Server={}; Database={}; IAM=1; DbUser={}; DbPassword={}; Port={}".format(
            odbc_driver, host, database, username, password, port
        )
        conn = pyodbc.connect(odbc_connection_str)
    else:
        raise ValueError(f"Unknown Interface {interface}")
    conn.autocommit = False
    return conn


def retrieve_compressed_json(location):
    """ Load a gzipped json file from the specified location, either local or s3 """
    sql_gz = load_file(location)
    json_content = gzip.GzipFile(fileobj=io.BytesIO(sql_gz), mode='rb').read().decode('utf-8')
    return json.loads(json_content)


def load_file(location, decode=False):
    """ load a file from s3 or local. decode if the file should be interpreted as text rather than binary """
    try:
        if location.startswith("s3://"):
            url = urlparse(location, allow_fragments=False)
            s3 = boto3.resource('s3')
            content = s3.Object(url.netloc, url.path.lstrip('/')).get()["Body"].read()
        else:
            with open(location, 'rb') as data:
                content = data.read()
        if decode:
            content = content.decode('utf-8')
    except Exception as e:
        logger.error(
            f"Unable to load file from {location}. Does the file exist and do you have correct permissions? {str(e)}")
        raise (e)

    return content


def load_config(location):
    """ Load a yaml config file from a local file or from s3 """
    logger.info(f"Loading config file from {location}")
    config = load_file(location, decode=True)

    try:
        config_yaml = yaml.safe_load(config)
    except yaml.YAMLError as exception:
        logger.error(exception)
        return None

    return config_yaml


def cluster_dict(endpoint, is_serverless=False, start_time=None, end_time=None):
    '''Create a object-like dictionary from cluster endpoint'''
    parsed = urlparse(endpoint)
    url_split = parsed.scheme.split(".")
    port_database = parsed.path.split("/")

    if is_serverless:
        workgroup_name = url_split[0]

    cluster = {
        "endpoint": endpoint,
        "id": url_split[0],
        "host": parsed.scheme,
        "region": url_split[2],
        "port": port_database[0],
        "database": port_database[1],
        "is_serverless": is_serverless
    }

    if start_time is not None:
        cluster["start_time"] = start_time
    if end_time is not None:
        cluster["end_time"] = end_time

    logger = logging.getLogger("SimpleReplayLogger")

    if not is_serverless:
        rs_client = boto3.client('redshift', region_name=cluster.get("region"))
        try:
            response = rs_client.describe_clusters(ClusterIdentifier=cluster.get('id'))

            cluster["num_nodes"] = (response['Clusters'][0]['NumberOfNodes'])
            cluster["instance"] = (response['Clusters'][0]['NodeType'])
        except Exception as e:
            logger.warning(f"Unable to get cluster information. Please ensure IAM permissions include "
                           f"Redshift:DescribeClusters. {e}")
            cluster["num_nodes"] = "N/A"
            cluster["instance"] = "N/A"
    else:
        rs_client = boto3.client('redshift-serverless', region_name=cluster.get("region"))
        try:
            response = rs_client.get_workgroup(workgroupName=workgroup_name)

            cluster["num_nodes"] = "N/A"
            cluster["instance"] = "Serverless"
            cluster["base_rpu"] = response['workgroup']['baseCapacity']
        except Exception as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.warning(f"Serverless endpoint could not be found "
                               f"RedshiftServerless:GetWorkGroup. {e}")
            else:
                logger.warning(f"Exception during fetching work group details for Serverless endpoint "
                               f"RedshiftServerless:GetWorkGroup. {e}")
                cluster["num_nodes"] = "N/A"
                cluster["instance"] = "Serverless"
                cluster["base_rpu"] = "N/A"

    return cluster


def bucket_dict(bucket_url):
    logger = logging.getLogger("SimpleReplayLogger")
    bucket, path = None, None
    try:
        parsed = urlparse(bucket_url)
        bucket, path = parsed.netloc, parsed.path
    except ValueError as e:
        logger.error(f'{e}\nPlease enter a valid S3 url following one of the following style conventions: '
                     f'S3://bucket-name/key-name')
        exit(-1)
    if path.startswith('/'):
        path = path[1:]
    if not path == '' and not path.endswith('/'):
        path = f"{path}/"
    if path == 'replays/':
        path = ''
    return {'url': bucket_url,
            'bucket_name': bucket,
            'prefix': path}


def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = None

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        # secret_string = json.loads(get_secret_value_response['SecretString'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'AccessDeniedException':
            # Use is not authorized to perform secretsmanager:GetSecretValue on requested resource
            raise e

    # Decrypts secret using the associated KMS key.
    # Depending on whether the secret is a string or binary, one of these fields will be populated.
    if 'SecretString' in get_secret_value_response:
        secret = json.loads(get_secret_value_response['SecretString'])
    else:
        secret = json.loads(base64.b64decode(get_secret_value_response['SecretBinary']))

    return secret


def categorize_error(err_code):
    # https://www.postgresql.org/docs/current/errcodes-appendix.html
    err_class = {'00': 'Successful Completion',
                 '01': 'Warning',
                 '02': 'No Data',
                 '03': 'SQL Statement Not Yet Complete',
                 '08': 'Connection Exception',
                 '09': 'Triggered Action Exception',
                 '0A': 'Feature Not Supported',
                 '0B': 'Invalid Transaction Initiation',
                 '0F': 'Locator Exception',
                 '0L': 'Invalid Grantor',
                 '0P': 'Invalid Role Specification',
                 '0Z': 'Diagnostics Exception',
                 '20': 'Case Not Found',
                 '21': 'Cardinality Violation',
                 '22': 'Data Exception',
                 '23': 'Integrity Constraint Violation',
                 '24': 'Invalid Cursor State',
                 '25': 'Invalid Transaction State',
                 '26': 'Invalid SQL Statement Name',
                 '27': 'Triggered Data Change Violation',
                 '28': 'Invalid Authorization Specification',
                 '2B': 'Dependent Privilege Descriptors Still Exist',
                 '2D': 'Invalid Transaction Termination',
                 '2F': 'SQL Routine Exception',
                 '34': 'Invalid Cursor Name',
                 '38': 'External Routine Exception',
                 '39': 'External Routine Invocation Exception',
                 '3B': 'Savepoint Exception',
                 '3D': 'Invalid Catalog Name',
                 '3F': 'Invalid Schema Name',
                 '40': 'Transaction Rollback',
                 '42': 'Syntax Error or Access Rule Violation',
                 '44': 'WITH CHECK OPTION Violation',
                 '53': 'Insufficient Resources',
                 '54': 'Program Limit Exceeded',
                 '55': 'Object Not In Prerequisite State',
                 '57': 'Operator Intervention',
                 '58': 'System Error',
                 '72': 'Snapshot Failure',
                 'F0': 'Configuration File Error',
                 'HV': 'Foreign Data Wrapper Error (SQL/MED)',
                 'P0': 'PL/pgSQL Error',
                 'XX': 'Internal Error',
                 }
    if err_code[0:2] in err_class.keys():
        return err_class[err_code[0:2]]

    return "Uncategorized Error"


def remove_comments(string):
    pattern = r"(\".*?\"|\'.*?\')|(/\*.*?\*/|//[^\r\n]*$)"
    # first group captures quoted strings (double or single)
    # second group captures comments (//single-line or /* multi-line */)
    regex = re.compile(pattern, re.MULTILINE|re.DOTALL)
    def _replacer(match):
        # if the 2nd group (capturing comments) is not None,
        # it means we have captured a non-quoted (real) comment string.
        if match.group(2) is not None:
            return "" # so we will return empty to remove the comment
        else: # otherwise, we will return the 1st group
            return match.group(1) # captured quoted-string
    return regex.sub(_replacer, string)


def parse_error(error, user, db, qtxt):
    err_entry = {'timestamp': datetime.datetime.now(tz=datetime.timezone.utc).isoformat(timespec='seconds'),
                 'user': user,
                 'db': db,
                 'query_text': remove_comments(qtxt)
                }

    temp = error.__str__().replace("\"", r"\"")
    raw_error_string = json.loads(temp.replace("\'", "\""))
    err_entry['detail'] = ""

    if 'D' in raw_error_string:
        detail_string = raw_error_string['D']
        try:
            detail = detail_string[detail_string.find("context:"):detail_string.find("query")].split(":", maxsplit=1)[-1].strip()
            err_entry['detail'] = detail
        except Exception as e:
            print(e)
            err_entry['detail'] = ""

    err_entry['code'] = raw_error_string['C']
    err_entry['message'] = raw_error_string['M']
    err_entry['severity'] = raw_error_string['S']
    err_entry['category'] = categorize_error(err_entry['code'])

    return err_entry


def create_json(replay, cluster, workload, complete, stats, tag=""):
    """Generates a JSON containing cluster details for the replay
    """

    if cluster['start_time'] and cluster['end_time']:
        duration = cluster['end_time'] - cluster['start_time']
        duration = duration - datetime.timedelta(microseconds=duration.microseconds)

        cluster['start_time'] = str(cluster['start_time'].isoformat(timespec='seconds'))
        cluster['end_time'] = str(cluster['end_time'].isoformat(timespec='seconds'))
        cluster['duration'] = str(duration)

    if complete:
        cluster['status'] = "Complete"
    else:
        cluster['status'] = "Incomplete"

    cluster['replay_id'] = replay
    cluster['replay_tag'] = tag
    cluster['workload'] = workload

    for k, v in enumerate(stats):
        cluster[v] = stats[v]

    cluster["connection_success"] = cluster['connection_count'] - cluster['connection_errors']

    json_object = json.dumps(cluster, indent=4)
    with open(f"info.json", "w") as outfile:
        outfile.write(json_object)
        return outfile.name


