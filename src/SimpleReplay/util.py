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
        conn = redshift_connector.connect(user=username,password=password,host=host,
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
        logger.error(f"Unable to load file from {location}. Does the file exist and do you have correct permissions? {str(e)}")
        raise(e)

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


def cluster_dict(endpoint, start_time=None, end_time=None):
    '''Create a object-like dictionary from cluster endpoint'''
    parsed = urlparse(endpoint)
    url_split = parsed.scheme.split(".")
    port_database = parsed.path.split("/")

    cluster = {
        "endpoint": endpoint,
        "id": url_split[0],
        "host": parsed.scheme,
        "region": url_split[2],
        "port": port_database[0],
        "database": port_database[1]
    }

    if start_time is not None:
        cluster["start_time"] = start_time
    if end_time is not None:
        cluster["end_time"] = end_time

    rs_client = boto3.client('redshift', region_name=cluster.get("region"))
    logger = logging.getLogger("SimpleReplayLogger")
    try:
        response = rs_client.describe_clusters(ClusterIdentifier=cluster.get('id'))
        cluster["num_nodes"] = (response['Clusters'][0]['NumberOfNodes'])
        cluster["instance"] = (response['Clusters'][0]['NodeType'])
    except Exception as e:
        logger.warning(f"Unable to get cluster information. Please ensure IAM permissions include "
                       f"Redshift:DescribeClusters. {e}")
        cluster["num_nodes"] = "N/A"
        cluster["instance"] = "N/A"
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
