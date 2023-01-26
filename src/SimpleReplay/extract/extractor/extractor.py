"""
extractor.py
====================================
The core module of Simple Replay Project
"""

import datetime
import gzip
import json
import logging
import pathlib
import re
from collections import OrderedDict

import dateutil.parser
import redshift_connector
from boto3 import client
from tqdm import tqdm

from audit_logs_parsing import (
    ConnectionLog,
)
from helper import aws_service as aws_service_helper
from log_validation import remove_line_comments
from .cloudwatch_extractor import CloudwatchExtractor
from .s3_extractor import S3Extractor
from .local_extractor import LocalExtractor

logger = logging.getLogger("SimpleReplayLogger")


class Extractor:
    disable_progress_bar = None
    bar_format = (
        "{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}{postfix}]"
    )
    config = None

    def __init__(self, config, cloudwatch_extractor=None, s3_extractor=None, local_extractor=None):
        self.config = config
        self.disable_progress_bar = config.get("disable_progress_bar")
        self.cloudwatch_extractor = cloudwatch_extractor if cloudwatch_extractor else CloudwatchExtractor(self.config)
        self.s3_extractor = s3_extractor if s3_extractor else S3Extractor(self.config)
        self.local_extractor = local_extractor if local_extractor else LocalExtractor(self.config)
    def get_extract(self, log_location, start_time, end_time):
        """
        getting the log location whether cloudwatch or s3 for cluster and checking
        whether the cluster is serverless or provisioned
        :param log_location:
        :param start_time:
        :param end_time:
        :param config:
        :return:
        """

        if (self.config.get("source_cluster_endpoint") and "redshift-serverless" in self.config.get("source_cluster_endpoint")) or (
                self.config.get("log_location") and "/aws/" in self.config.get("log_location")):
            logger.info(f"Extracting and parsing logs for serverless")
            logger.info(f"Time range: {start_time or '*'} to {end_time or '*'}")
            logger.info(f"This may take several minutes...")
            return self.cloudwatch_extractor.get_extract_from_cloudwatch(start_time, end_time)
        else:
            logger.info(f"Extracting and parsing logs for provisioned")
            logger.info(f"Time range: {start_time or '*'} to {end_time or '*'}")
            logger.info(f"This may take several minutes...")
            if log_location.startswith("s3://"):
                match = re.search(r"s3://([^/]+)/(.*)", log_location)
                if not match:
                    logger.error(f"Failed to parse log location {log_location}")
                    return None
                return self.s3_extractor.get_extract_from_s3(match.group(1), match.group(2), start_time, end_time)
            elif log_location in "cloudwatch":
                # Function for cloudwatch logs
                return self.cloudwatch_extractor.get_extract_from_cloudwatch(start_time, end_time)
            else:
                return self.local_extractor.get_extract_locally(log_location, start_time, end_time)

    def save_logs(self, logs, last_connections, output_directory, connections, start_time, end_time):
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
        log_items = logs.items()
        for filename, transaction in log_items:
            num_queries += len(transaction)
        logger.info(
            f"Exporting {len(logs)} transactions ({num_queries} queries) to {output_directory}"
        )

        is_s3 = True
        output_prefix = None
        bucket_name = None
        if output_directory.startswith("s3://"):
            output_s3_location = output_directory[5:].partition("/")
            bucket_name = output_s3_location[0]
            output_prefix = output_s3_location[2]
            archive_filename = "/tmp/SQLs.json.gz"
        else:
            is_s3 = False
            archive_filename = output_directory + "/SQLs.json.gz"
            logger.info(
                f"Creating directory {output_directory} if it doesn't already exist"
            )
            pathlib.Path(output_directory).mkdir(parents=True, exist_ok=True)

        sql_json, missing_audit_log_connections, replacements = self.get_sql_connections_replacements(last_connections,
                                                                                                      log_items)

        with gzip.open(archive_filename, "wb") as f:
            f.write(json.dumps(sql_json, indent=2).encode("utf-8"))

        if is_s3:
            dest = output_prefix + "/SQLs.json.gz"
            logger.info(f"Transferring SQL archive to {dest}")
            aws_service_helper.s3_upload(archive_filename, bucket_name, dest)

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
        connections_string = json.dumps(
            [connection.__dict__ for connection in sorted_connections],
            indent=4,
            default=str,
        )
        if is_s3:
            aws_service_helper.s3_put_object(connections_string, bucket_name, output_prefix + "/connections.json")
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
            aws_service_helper.s3_put_object(replacements_string, bucket_name, output_prefix + "/copy_replacements.csv")
        else:
            replacements_file = open(output_directory + "/copy_replacements.csv", "w")
            replacements_file.write(replacements_string)
            replacements_file.close()

    def get_sql_connections_replacements(self, last_connections, log_items):
        # transactions has form { "xid": xxx, "pid": xxx, etc..., queries: [] }
        sql_json = {"transactions": OrderedDict()}
        missing_audit_log_connections = set()
        replacements = set()
        for filename, queries in tqdm(
                log_items,
                disable=self.disable_progress_bar,
                unit="files",
                desc="Files processed",
                bar_format=self.bar_format,
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

                if not hash((query.database_name, query.username, query.pid)) in last_connections:
                    missing_audit_log_connections.add((query.database_name, query.username, query.pid))
        return sql_json, missing_audit_log_connections, replacements

    def unload_system_table(
            self,
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
                logger.debug(f"Executed unload query: {table_name}")

    def load_driver(self):
        interface = None
        if self.config.get("odbc_driver"):
            try:
                import pyodbc

                interface = "odbc"
            except Exception as err:
                logger.error(
                    "Error importing pyodbc. Please ensure pyodbc is correctly installed or remove the value for "
                    "\"odbc_driver\" to use redshift_connector. "
                )
        else:
            try:
                import redshift_connector

                interface = "psql"
            except Exception as err:
                logger.error(
                    'Error importing redshift_connector. Please ensure redshift_connector is correctly installed or add '
                    'an ODBC driver name value for "odbc_driver" to use pyodbc. '
                )

        return interface

    def get_connection_string(self, cluster_endpoint, username, odbc_driver):
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
                    "port": cluster_port,
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
            return {"odbc": odbc_driver, "psql": cluster_psql}
        except Exception as err:
            logger.error("Failed to generate connection string: " + str(err))
            return ""

    def get_parameters_for_log_extraction(self):
        """"
        :param config: from extract.yaml
        :return: extraction_name, start_time, end_time, log_location
        """
        now_iso_format = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat()
        if self.config.get("source_cluster_endpoint"):
            extraction_name = f'Extraction_{self.config.get("source_cluster_endpoint").split(".")[0]}_{now_iso_format}'
        else:
            extraction_name = f"Extraction_{now_iso_format}"

        start_time = ""
        if self.config.get("start_time"):
            start_time = dateutil.parser.parse(self.config.get("start_time")).astimezone(
                dateutil.tz.tzutc()
            )

        end_time = ""
        if self.config.get("end_time"):
            end_time = dateutil.parser.parse(self.config.get("end_time")).astimezone(
                dateutil.tz.tzutc()
            )

        log_location = ""
        if self.config.get("log_location"):
            log_location = self.config.get("log_location")
        elif self.config.get("source_cluster_endpoint"):
            source_cluster_endpoint = self.config.get("source_cluster_endpoint")
            # if provisioned, the logs can be in S3 or Cloudwatch
            if "redshift-serverless" not in source_cluster_endpoint:
                result = aws_service_helper.redshift_describe_logging_status(source_cluster_endpoint)
                if not result["LoggingEnabled"]:
                    logger.warning(
                        f"Cluster {source_cluster_endpoint} does not appear to have audit logging enabled." +
                        "Please confirm logging is enabled."
                    )
                    log_location = None
                elif "LogDestinationType" in result and result["LogDestinationType"] == "cloudwatch":
                    log_location = "cloudwatch"
                else:
                    log_location = "s3://{}/{}".format(result["BucketName"], result.get("S3KeyPrefix", ""))
                logger.debug(f"Log location: {log_location}")
            # if serverless return None since the logs will be in Cloudwatch
            else:
                logger.info(f"Found a redshift-serverless workload with endpoint {source_cluster_endpoint}")
                log_location = "cloudwatch"
                return (extraction_name, start_time, end_time, log_location)
        else:
            logger.error(
                "Either log_location or source_cluster_endpoint must be specified."
            )
            exit(-1)

        return (extraction_name, start_time, end_time, log_location)

    def retrieve_cluster_endpoint_info(self, extraction_name):
        source_cluster_endpoint = self.config.get("source_cluster_endpoint")
        if source_cluster_endpoint and "redshift-serverless" not in source_cluster_endpoint:
            logger.info(f'Retrieving info from {source_cluster_endpoint}')
            source_cluster_urls = self.get_connection_string(
                source_cluster_endpoint,
                self.config.get("master_username"),
                self.config.get("odbc_driver"),
            )

            if (
                    self.config.get("source_cluster_system_table_unload_location")
                    and self.config.get("unload_system_table_queries")
                    and self.config.get("source_cluster_system_table_unload_iam_role")
            ):
                logger.info(
                    f'Exporting system tables to {self.config.get("source_cluster_system_table_unload_location")}'
                )

                self.unload_system_table(
                    source_cluster_urls,
                    self.config.get("odbc_driver"),
                    self.config.get("unload_system_table_queries"),
                    self.config.get("source_cluster_system_table_unload_location")
                    + "/"
                    + extraction_name,
                    self.config.get("source_cluster_system_table_unload_iam_role"),
                )

                logger.info(
                    f'Exported system tables to {self.config.get("source_cluster_system_table_unload_location")}'
                )

    @staticmethod
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
