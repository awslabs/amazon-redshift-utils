"""
extract.py
====================================
The core module of Simple Replay Project
"""

import argparse
import datetime
import gzip
import json
import logging
import os
import pathlib
import re
from collections import OrderedDict

import boto3
import dateutil.parser
import redshift_connector
import yaml
from boto3 import client
from tqdm import tqdm

from audit_logs_parsing import (
    parse_cloudwatch_logs,
    ConnectionLog,
    parse_log,
    Logger,
)

from log_validation import (
    remove_line_comments,
    connection_time_replacement,
    get_logs_in_range,
)
from util import (
    init_logging,
    set_log_level,
    add_logfile,
    log_version,
)

logger = None
g_disable_progress_bar = None

g_bar_format = (
    "{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}{postfix}]"
)



def get_logs(log_location, start_time, end_time, config):
    """
    getting the log location whether cloudwatch or s3 for cluster and checking
    whether the cluster is serverless or provisioned
    :param log_location:
    :param start_time:
    :param end_time:
    :param config:
    :return:
    """

    if  (config["source_cluster_endpoint"] and "redshift-serverless" in config["source_cluster_endpoint"]) or (config["log_location"] and "/aws/" in config["log_location"]):
        logger.info(f"Extracting and parsing logs for serverless")
        logger.info(f"Time range: {start_time or '*'} to {end_time or '*'}")
        logger.info(f"This may take several minutes...")
        return parse_cloudwatch_logs(
            start_time=start_time, end_time=end_time, config=config
        )
    else:
        logger.info(f"Extracting and parsing logs for provisioned")
        logger.info(f"Time range: {start_time or '*'} to {end_time or '*'}")
        logger.info(f"This may take several minutes...")
        if log_location.startswith("s3://"):
            match = re.search(r"s3://([^/]+)/(.*)", log_location)
            if not (match):
                logger.error(f"Failed to parse log location {log_location}")
                return None
            return get_s3_logs(match.group(1), match.group(2), start_time, end_time)
        elif log_location in "cloudwatch":
            # Function for cloudwatch logs
            return parse_cloudwatch_logs(
                start_time=start_time, end_time=end_time, config=config
            )
        else:
            return get_local_logs(log_location, start_time, end_time)


def get_local_logs(log_directory_path, start_time, end_time):
    """

    :param log_directory_path:
    :param start_time:
    :param end_time:
    :return:
    """
    connections = {}
    last_connections = {}
    logs = {}
    databases = set()

    unsorted_list = os.listdir(log_directory_path)
    log_directory = sorted(unsorted_list)

    for filename in tqdm(
        log_directory,
        disable=g_disable_progress_bar,
        unit="files",
        desc="Files processed",
        bar_format=g_bar_format,
    ):
        if g_disable_progress_bar:
            logger.info(f"Processing {filename}")
        if "start_node" in filename:
            log_file = gzip.open(
                log_directory_path + "/" + filename, "rt", encoding="ISO-8859-1"
            )
        else:
            log_file = gzip.open(log_directory_path + "/" + filename, "r")
        parse_log(
            log_file,
            filename,
            connections,
            last_connections,
            logs,
            databases,
            start_time,
            end_time,
        )
        log_file.close()

    return (connections, logs, databases, last_connections)


def get_s3_logs(log_bucket, log_prefix, start_time, end_time):
    """
    getting logs from s3 and passing it to get_s3_audit_logs()
    :param log_bucket:
    :param log_prefix:
    :param start_time:
    :param end_time:
    :return:
    """
    connections = {}
    logs = {}
    last_connections = {}
    databases = set()

    conn = client("s3")

    # get first set of
    response = conn.list_objects_v2(Bucket=log_bucket, Prefix=log_prefix)
    bucket_objects = response["Contents"]

    if "NextContinuationToken" in response:
        prev_key = response["NextContinuationToken"]
        while True:
            response = conn.list_objects_v2(
                Bucket=log_bucket, Prefix=log_prefix, ContinuationToken=prev_key
            )
            bucket_objects.extend(response["Contents"])
            if "NextContinuationToken" not in response:
                break
            prev_key = response["NextContinuationToken"]

    s3_connection_logs = []
    s3_user_activity_logs = []

    for log in bucket_objects:
        filename = log["Key"].split("/")[-1]
        if "connectionlog" in filename:
            s3_connection_logs.append(log)
        elif "useractivitylog" in filename:
            s3_user_activity_logs.append(log)

    logger.info("Parsing connection logs")
    get_s3_audit_logs(
        log_bucket,
        log_prefix,
        start_time,
        end_time,
        s3_connection_logs,
        connections,
        logs,
        databases,
        last_connections,
    )
    logger.info("Parsing user activity logs")
    get_s3_audit_logs(
        log_bucket,
        log_prefix,
        start_time,
        end_time,
        s3_user_activity_logs,
        connections,
        logs,
        databases,
        last_connections,
    )
    return (connections, logs, databases, last_connections)


def get_s3_audit_logs(
    log_bucket,
    log_prefix,
    start_time,
    end_time,
    audit_objects,
    connections,
    logs,
    databases,
    last_connections,
):
    """
    Getting  audit logs from S3 for the cluster from get_s3_logs  and calling the pasrse_log()

    :param log_bucket:
    :param log_prefix:
    :param start_time:
    :param end_time:
    :param audit_objects:
    :param connections:
    :param logs:
    :param databases:
    :param last_connections:
    :return:
    """
    s3 = boto3.resource("s3")

    index_of_last_valid_log = len(audit_objects) - 1

    log_filenames = get_logs_in_range(audit_objects, start_time, end_time)

    logger.info(f"Processing {len(log_filenames)} files")

    is_continue_parsing = True
    curr_index = index_of_last_valid_log

    last = curr_index
    for filename in tqdm(
        log_filenames,
        disable=g_disable_progress_bar,
        unit="files",
        desc="Files processed",
        bar_format=g_bar_format,
    ):
        file_datetime = dateutil.parser.parse(filename.split("_")[-1][:-3]).replace(
            tzinfo=datetime.timezone.utc
        )

        curr_connection_length = len(connections)
        curr_logs_length = len(logs)

        log_object = s3.Object(log_bucket, filename)
        log_file = gzip.GzipFile(fileobj=log_object.get()["Body"])

        parse_log(
            log_file,
            filename,
            connections,
            last_connections,
            logs,
            databases,
            start_time,
            end_time,
        )

    logger.debug(
        f'First audit log in start_time range: {audit_objects[curr_index]["Key"].split("/")[-1]}'
    )
    return (connections, logs, databases, last_connections)


def save_logs(
    logs, last_connections, output_directory, connections, start_time, end_time
):
    """
    saving the extracted logs in S3 location in the following format:
    connections.json, copy_replacements.csv, SQLs.json.gz
    :param logs:
    :param last_connections:
    :param output_directory:
    :param connections:
    :param start_time:
    :param end_time:
    :return:
    """
    num_queries = 0
    for filename, transaction in logs.items():
        num_queries += len(transaction)
    logger.info(
        f"Exporting {len(logs)} transactions ({num_queries} queries) to {output_directory}"
    )

    is_s3 = True
    if output_directory.startswith("s3://"):
        output_s3_location = output_directory[5:].partition("/")
        bucket_name = output_s3_location[0]
        output_prefix = output_s3_location[2]
        s3_client = boto3.client("s3")
        archive_filename = "/tmp/SQLs.json.gz"
    else:
        is_s3 = False
        archive_filename = output_directory + "/SQLs.json.gz"
        logger.info(
            f"Creating directory {output_directory} if it doesn't already exist"
        )
        pathlib.Path(output_directory).mkdir(parents=True, exist_ok=True)

    # transactions has form { "xid": xxx, "pid": xxx, etc..., queries: [] }
    sql_json = {"transactions": OrderedDict()}

    missing_audit_log_connections = set()

    # Save the main logs and find replacements
    replacements = set()

    for filename, queries in tqdm(
        logs.items(),
        disable=g_disable_progress_bar,
        unit="files",
        desc="Files processed",
        bar_format=g_bar_format,
    ):
        for idx, query in enumerate(queries):
            try:
                if query.xid not in sql_json["transactions"]:
                    sql_json["transactions"][query.xid] = {
                        "xid": query.xid,
                        "pid": query.pid,
                        "db": query.database_name,
                        "user": query.username,
                        "time_interval": True,
                        "queries": [],
                    }
                query_info = {
                    "record_time": query.record_time.isoformat(),
                    "start_time": query.start_time.isoformat()
                    if query.start_time
                    else None,
                    "end_time": query.end_time.isoformat() if query.end_time else None,
                }
            except AttributeError:
                logger.error(
                    f"Query is missing header info, skipping {filename}: {query}"
                )
                continue

            query.text = remove_line_comments(query.text).strip()

            if "copy " in query.text.lower() and "from 's3:" in query.text.lower():
                bucket = re.search(
                    r"from 's3:\/\/[^']*", query.text, re.IGNORECASE
                ).group()[6:]
                replacements.add(bucket)
                query.text = re.sub(
                    r"IAM_ROLE 'arn:aws:iam::\d+:role/\S+'",
                    f" IAM_ROLE ''",
                    query.text,
                    flags=re.IGNORECASE,
                )
            if "unload" in query.text.lower() and "to 's3:" in query.text.lower():
                query.text = re.sub(
                    r"IAM_ROLE 'arn:aws:iam::\d+:role/\S+'",
                    f" IAM_ROLE ''",
                    query.text,
                    flags=re.IGNORECASE,
                )

            query.text = f"{query.text.strip()}"
            if not len(query.text) == 0:
                if not query.text.endswith(";"):
                    query.text += ";"

            query_info["text"] = query.text
            sql_json["transactions"][query.xid]["queries"].append(query_info)

            if (
                not hash((query.database_name, query.username, query.pid))
                in last_connections
            ):
                missing_audit_log_connections.add(
                    (query.database_name, query.username, query.pid)
                )

    with gzip.open(archive_filename, "wb") as f:
        f.write(json.dumps(sql_json, indent=2).encode("utf-8"))

    if is_s3:
        dest = output_prefix + "/SQLs.json.gz"
        logger.info("Transferring SQL archive to {dest}")
        s3_client.upload_file(archive_filename, bucket_name, dest)

    logger.info(f"Generating {len(missing_audit_log_connections)} missing connections.")
    for missing_audit_log_connection_info in missing_audit_log_connections:
        connection = ConnectionLog(
            start_time,
            end_time,  # for missing connections set start_time and end_time to our extraction range
            missing_audit_log_connection_info[0],
            missing_audit_log_connection_info[1],
            missing_audit_log_connection_info[2],
        )
        pk = connection.get_pk()
        connections[pk] = connection
    logger.info(
        f"Exporting a total of {len(connections.values())} connections to {output_directory}"
    )
    # Save the connections logs
    sorted_connections = connections.values()
    connections_dict = connection_time_replacement(
        [connection.__dict__ for connection in sorted_connections]
    )
    connections_string = json.dumps(
        [connection.__dict__ for connection in sorted_connections],
        indent=4,
        default=str,
    )
    if is_s3:
        s3_client.put_object(
            Body=connections_string,
            Bucket=bucket_name,
            Key=output_prefix + "/connections.json",
        )
    else:
        connections_file = open(output_directory + "/connections.json", "x")
        connections_file.write(connections_string)
        connections_file.close()

    # Save the replacements
    logger.info(f"Exporting copy replacements to {output_directory}")
    replacements_string = (
        "Original location,Replacement location,Replacement IAM role\n"
    )
    for bucket in replacements:
        replacements_string += bucket + ",,\n"
    if is_s3:
        s3_client.put_object(
            Body=replacements_string,
            Bucket=bucket_name,
            Key=output_prefix + "/copy_replacements.csv",
        )
    else:
        replacements_file = open(output_directory + "/copy_replacements.csv", "w")
        replacements_file.write(replacements_string)
        replacements_file.close()


def unload_system_table(
    source_cluster_urls,
    odbc_driver,
    unload_system_table_queries_file,
    unload_location,
    unload_iam_role,
):
    """
    Unloading the system table if the unload location and unload iam user
    is mentioned in the extract.yaml file.
    :param source_cluster_urls: cluster dict
    :param odbc_driver:
    :param unload_system_table_queries_file:
    :param unload_location:
    :param unload_iam_role:
    :return:
    """
    conn = None
    if odbc_driver:
        conn = pyodbc.connect(source_cluster_urls["odbc"])
    else:
        conn = redshift_connector.connect(
            user=source_cluster_urls["psql"]["username"],
            password=source_cluster_urls["psql"]["password"],
            host=source_cluster_urls["psql"]["host"],
            port=int(source_cluster_urls["psql"]["port"]),
            database=source_cluster_urls["psql"]["database"],
        )

    conn.autocommit = True

    unload_queries = {}
    table_name = ""
    query_text = ""
    for line in open(unload_system_table_queries_file, "r"):
        if line.startswith("--"):
            unload_queries[table_name] = query_text.strip("\n")
            table_name = line[2:].strip("\n")
            query_text = ""
        else:
            query_text += line

    unload_queries[table_name] = query_text.strip("\n")
    del unload_queries[""]

    cursor = conn.cursor()
    for table_name, unload_query in unload_queries.items():
        if table_name and unload_query:
            unload_query = re.sub(
                r"to ''",
                f"TO '{unload_location}/system_tables/{table_name}/'",
                unload_query,
                flags=re.IGNORECASE,
            )
            unload_query = re.sub(
                r"credentials ''",
                f"CREDENTIALS 'aws_iam_role={unload_iam_role}'",
                unload_query,
                flags=re.IGNORECASE,
            )

            cursor.execute(unload_query)
            logger.debug(f"Executed unload query: {unload_query}")


def validate_config_file(config_file):
    """
    Validating the parameters from extract.yaml file
    :param config_file: extract.yaml file
    :return:
    """
    if config_file["source_cluster_endpoint"]:
        if "redshift-serverless" in config_file["source_cluster_endpoint"]:
            if (
                not len(config_file["source_cluster_endpoint"].split(".")) == 6
                or not len(config_file["source_cluster_endpoint"].split(":")) == 2
                or not len(config_file["source_cluster_endpoint"].split("/")) == 2
            ):
                logger.error(
                    'Config file value for "source_cluster_endpoint" is not a valid endpoint. Endpoints must be in the format of <identifier>.<region>.redshift-serverless.amazonaws.com:<port>/<database-name>.'
                )
                exit(-1)
        elif (
            not len(config_file["source_cluster_endpoint"].split(".")) == 6
            or not len(config_file["source_cluster_endpoint"].split(":")) == 2
            or not len(config_file["source_cluster_endpoint"].split("/")) == 2
            or ".redshift.amazonaws.com:" not in config_file["source_cluster_endpoint"]
        ):
            logger.error(
                'Config file value for "source_cluster_endpoint" is not a valid endpoint. Endpoints must be in the format of <cluster-name>.<identifier>.<region>.redshift.amazonaws.com:<port>/<database-name>.'
            )
            exit(-1)
        if not config_file["master_username"]:
            logger.error(
                'Config file missing value for "master_username". Please provide a value or remove the "source_cluster_endpoint" value.'
            )
            exit(-1)
    else:
        if not config_file["log_location"]:
            logger.error(
                'Config file missing value for "log_location". Please provide a value for "log_location", or provide a value for "source_cluster_endpoint".'
            )
            exit(-1)
    if config_file["start_time"]:
        try:
            dateutil.parser.isoparse(config_file["start_time"])
        except ValueError:
            logger.error(
                'Config file "start_time" value not formatted as ISO 8601. Please format "start_time" as ISO 8601 or remove its value.'
            )
            exit(-1)
    if config_file["end_time"]:
        try:
            dateutil.parser.isoparse(config_file["end_time"])
        except ValueError:
            logger.error(
                'Config file "end_time" value not formatted as ISO 8601. Please format "end_time" as ISO 8601 or remove its value.'
            )
            exit(-1)
    if not config_file["workload_location"]:
        logger.error(
            'Config file missing value for "workload_location". Please provide a value for "workload_location".'
        )
        exit(-1)
    if config_file["source_cluster_system_table_unload_location"] and not config_file[
        "source_cluster_system_table_unload_location"
    ].startswith("s3://"):
        logger.error(
            'Config file value for "source_cluster_system_table_unload_location" must be an S3 location (starts with "s3://"). Please remove this value or put in an S3 location.'
        )
        exit(-1)
    if (
        config_file["source_cluster_system_table_unload_location"]
        and not config_file["source_cluster_system_table_unload_iam_role"]
    ):
        logger.error(
            'Config file missing value for "source_cluster_system_table_unload_iam_role". Please provide a value for "source_cluster_system_table_unload_iam_role", or remove the value for "source_cluster_system_table_unload_location".'
        )
        exit(-1)
    if (
        config_file["source_cluster_system_table_unload_location"]
        and not config_file["unload_system_table_queries"]
    ):
        logger.error(
            'Config file missing value for "unload_system_table_queries". Please provide a value for "unload_system_table_queries", or remove the value for "source_cluster_system_table_unload_location".'
        )
        exit(-1)
    if config_file["unload_system_table_queries"] and not config_file[
        "unload_system_table_queries"
    ].endswith(".sql"):
        logger.error(
            'Config file value for "unload_system_table_queries" does not end with ".sql". Please ensure the value for "unload_system_table_queries" ends in ".sql". See the provided "unload_system_tables.sql" as an example.'
        )
        exit(-1)


def get_cluster_log_location(source_cluster_endpoint):
    """Get the audit log location for the cluster via the API
    """
    if "redshift-serverless" not in source_cluster_endpoint:
        logger.debug(f"Retrieving log location for {source_cluster_endpoint}")
        region = source_cluster_endpoint.split(".")[2]
        result = client("redshift", region).describe_logging_status(
            ClusterIdentifier=source_cluster_endpoint.split(".")[0]
        )

        if not result["LoggingEnabled"]:
            logger.warning(
                f"Cluster {source_cluster_endpoint} does not appear to have audit logging enabled.  Please confirm logging is enabled."
            )
            return None
        if "LogDestinationType" in result and result["LogDestinationType"] == "cloudwatch":
            return "cloudwatch"
        location = "s3://{}/{}".format(result["BucketName"], result.get("S3KeyPrefix", ""))
        logger.debug(f"Log location: {location}")
        return location
    else:
        logger.debug(f"Retrieving log location for {source_cluster_endpoint}")
        region = source_cluster_endpoint.split(".")[2]
        serverless_client = boto3.client('redshift-serverless',region)
        workgroup_response = serverless_client.get_workgroup(workgroupName=source_cluster_endpoint.split(".")[0])
        namespace_response = serverless_client.get_namespace(namespaceName=workgroup_response["workgroup"]["namespaceName"])
        if len(namespace_response["namespace"]["logExports"])==0 :
            logger.warning(
                f"Cluster {source_cluster_endpoint} does not appear to have audit logging enabled.  Please confirm logging is enabled."
            )
            return None

def load_driver():
    interface = None
    if g_config["odbc_driver"]:
        try:
            import pyodbc

            interface = "odbc"
        except Exception as err:
            logger.error(
                'Error importing pyodbc. Please ensure pyodbc is correctly installed or remove the value for "odbc_driver" to use redshift_connector.'
            )
    else:
        try:
            import redshift_connector

            interface = "psql"
        except Exception as err:
            logger.error(
                'Error importing redshift_connector. Please ensure redshift_connector is correctly installed or add an ODBC driver name value for "odbc_driver" to use pyodbc.'
            )

    return interface


def get_connection_string(cluster_endpoint, username, odbc_driver):
    cluster_endpoint_split = cluster_endpoint.split(".")
    cluster_region = cluster_endpoint.split(".")[2]
    cluster_id = cluster_endpoint_split[0]
    cluster_host = cluster_endpoint.split(":")[0]
    if "redshift-serverless" in cluster_endpoint:
        cluster_port = cluster_endpoint_split[5].split("/")[0][4:]
        cluster_database = cluster_endpoint_split[5].split("/")[1]
    else:
        cluster_port = cluster_endpoint_split[5].split("/")[0][4:]
        cluster_database = cluster_endpoint_split[5].split("/")[1]
    try:
        if "redshift-serverless" in cluster_endpoint:
            response = client.get_credentials(
                dbName=cluster_database,
                durationSeconds=123,
                workgroupName=cluster_id
            )
            cluster_psql = {
                "username": response["dbUser"],
                "password": response["dbPassword"],
                "host": cluster_host,
                "port":  cluster_port,
                "database": cluster_database,
            }
        else:
            response = client("redshift", cluster_region).get_cluster_credentials(
                DbUser=username,
                ClusterIdentifier=cluster_id,
                AutoCreate=False,
            )
            cluster_psql = {
                "username": response["DbUser"],
                "password": response["DbPassword"],
                "host": cluster_host,
                "port": cluster_port,
                "database": cluster_database,
            }

        return {"odbc": None, "psql": cluster_psql}
    except Exception as err:
        logger.error("Failed to generate connection string: " + str(err))
        return ""


def init_logger():
    """Initialize logger"""
    global logger
    logger = init_logging(logging.INFO)
    Logger(logger)


def get_arguments():
    """
    getting the CLI arguments
    :return: CLI argumets
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "config_file",
        type=argparse.FileType("r"),
        help="Location of extraction config file.",
    )
    return parser.parse_args()


def init_global_config(args):
    """
    Loading the extract.yaml filr
    :param args: CLI as extract.yaml file
    """
    global g_config
    g_config = {}
    with args.config_file as stream:
        try:
            g_config = yaml.safe_load(stream)
        except yaml.YAMLError as exception:
            logger.error(f"Failed to parse extraction config yaml file: {exception}")
            exit(-1)


def configure_logging():
    """
    logging configuration, settting log_level to (INFO or DEBUG)
    and producing the progress_bar in the output
    """
    level = logging.getLevelName(g_config.get("log_level", "INFO").upper())
    set_log_level(level)

    if g_config.get("logfile_level") != "none":
        level = logging.getLevelName(g_config.get("logfile_level", "DEBUG").upper())
        log_file = "extract.log"
        add_logfile(
            log_file,
            level=level,
            preamble=yaml.dump(g_config),
            backup_count=g_config.get("backup_count", 2),
        )
    log_version()
    # disable_progress_bar should be None if user sets to False, to disable writing to
    # non-tty (i.e. log file)
    if g_config.get("disable_progress_bar") == True:
        global g_disable_progress_bar
        g_disable_progress_bar = True


def attempt_driver_loading():
    """
    calling load_driver for loading either psql or odbc driver
    """
    interface = load_driver()
    if not interface:
        logger.error("Failed to load driver.")
        exit(-1)


def get_parameters_for_log_extraction():
    """
    return parameters extraction_name, start_time, end_time, log_location
    """
    if g_config["source_cluster_endpoint"]:
        extraction_name = f'Extraction_{g_config["source_cluster_endpoint"].split(".")[0]}_{datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat()}'
    else:
        extraction_name = f"Extraction_{datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat()}"

    start_time = ""
    if g_config.get("start_time"):
        start_time = dateutil.parser.parse(g_config["start_time"]).astimezone(
            dateutil.tz.tzutc()
        )

    end_time = ""
    if g_config.get("end_time"):
        end_time = dateutil.parser.parse(g_config["end_time"]).astimezone(
            dateutil.tz.tzutc()
        )

        # read the logs
        log_location = ""
        if (g_config["source_cluster_endpoint"] and "redshift-serverless" not in g_config[
            "source_cluster_endpoint"]) or (g_config["log_location"] and "s3://" in g_config["log_location"]):
            if g_config.get("log_location"):
                log_location = g_config["log_location"]
            elif g_config.get("source_cluster_endpoint"):
                log_location = get_cluster_log_location(g_config["source_cluster_endpoint"])
            else:
                logger.error(
                    "Either log_location or source_cluster_endpoint must be specified."
                )
                exit(-1)
        else:
            if g_config.get("log_location"):
                log_location = g_config["log_location"]
            elif g_config.get("source_cluster_endpoint"):
                log_location = get_cluster_log_location(g_config["source_cluster_endpoint"])
            else:
                logger.error(
                    "Either log_location or source_cluster_endpoint must be specified."
                )
                exit(-1)
        return extraction_name, start_time, end_time, log_location


def retrieve_cluster_endpoint_info(extraction_name):
    """
    retrieving information for cluster endpoint and calling the
    get_connection_string(), unload_system_table
    :param extraction_name: name for the extraction log file
    :return:
    """
    if g_config["source_cluster_endpoint"] and "redshift-serverless" not in g_config["source_cluster_endpoint"]:
        logger.info(f'Retrieving info from {g_config["source_cluster_endpoint"]}')
        source_cluster_urls = get_connection_string(
            g_config["source_cluster_endpoint"],
            g_config["master_username"],
            g_config["odbc_driver"],
        )

        if (
            g_config["source_cluster_system_table_unload_location"]
            and g_config["unload_system_table_queries"]
            and g_config["source_cluster_system_table_unload_iam_role"]
        ):
            logger.info(
                f'Exporting system tables to {g_config["source_cluster_system_table_unload_location"]}'
            )

            unload_system_table(
                source_cluster_urls,
                g_config["odbc_driver"],
                g_config["unload_system_table_queries"],
                g_config["source_cluster_system_table_unload_location"]
                + "/"
                + extraction_name,
                g_config["source_cluster_system_table_unload_iam_role"],
            )

            logger.info(
                f'Exported system tables to {g_config["source_cluster_system_table_unload_location"]}'
            )


def validate_log_result(connections, audit_logs) -> None:
    """Validate log results from get_logs()

    :param connections: number of connection logs
    :param audit_logs: number of audit logs
    :return: None
    """
    logger.debug(
        f"Found {len(connections)} connection logs, {len(audit_logs)} audit logs"
    )

    if len(audit_logs) == 0 or len(connections) == 0:
        logger.warning(
            "No audit logs or connections logs found. "
            "Please verify that the audit log location or cluster endpoint is correct. "
            "Note, audit logs can take several hours to start appearing in S3 after logging is first enabled."
        )
        exit(-1)


def main():
    init_logger()
    args = get_arguments()
    init_global_config(args)
    validate_config_file(g_config)
    configure_logging()
    attempt_driver_loading()

    (
        extraction_name,
        start_time,
        end_time,
        log_location,
    ) = get_parameters_for_log_extraction()
    (connections, audit_logs, databases, last_connections) = get_logs(
        log_location, start_time, end_time, g_config
    )

    validate_log_result(connections=connections, audit_logs=audit_logs)
    retrieve_cluster_endpoint_info(extraction_name)

    save_logs(
        audit_logs,
        last_connections,
        g_config["workload_location"] + "/" + extraction_name,
        connections,
        start_time,
        end_time,
    )


if __name__ == "__main__":
    main()
