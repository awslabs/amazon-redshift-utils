"""
auditlogs_parsing.py

This module parses various auditlogs
"""
import datetime
import time
import gzip
import re
import os

import boto3
import dateutil.parser

from log_validation import is_duplicate, is_valid_log

logger = None


class Log:
    def __init__(self):
        self.record_time = ""
        self.start_time = ""
        self.end_time = ""
        self.username = ""
        self.database_name = ""
        self.pid = ""
        self.xid = ""
        self.text = ""

    def get_filename(self):
        base_name = (
            self.database_name
            + "-"
            + self.username
            + "-"
            + self.pid
            + "-"
            + self.xid
            + " ("
            + self.record_time.isoformat()
            + ")"
        )
        return base_name

    def __str__(self):
        return (
            "Record time: %s, Start time: %s, End time: %s, Username: %s, Database: %s, PID: %s, XID: %s, Query: %s"
            % (
                self.record_time,
                self.start_time,
                self.end_time,
                self.username,
                self.database_name,
                self.pid,
                self.xid,
                self.text,
            )
        )

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.record_time == other.record_time
            and self.start_time == other.start_time
            and self.end_time == other.end_time
            and self.username == other.username
            and self.database_name == other.database_name
            and self.pid == other.pid
            and self.xid == other.xid
            and self.text == other.text
        )

    def __hash__(self):
        return hash((str(self.pid), str(self.xid), self.text.strip("\n")))


class ConnectionLog:
    def __init__(self, session_initiation_time, end_time, database_name, username, pid):
        self.session_initiation_time = session_initiation_time
        self.disconnection_time = end_time
        self.application_name = ""
        self.database_name = database_name
        self.username = username
        self.pid = pid
        self.time_interval_between_transactions = True
        self.time_interval_between_queries = "transaction"

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.session_initiation_time == other.session_initiation_time
            and self.disconnection_time == other.disconnection_time
            and self.application_name == other.application_name
            and self.database_name == other.database_name
            and self.username == other.username
            and self.pid == other.pid
            and self.time_interval_between_transactions
            == other.time_interval_between_transactions
            and self.time_interval_between_queries
            == other.time_interval_between_queries
        )

    def __hash__(self):
        return hash((self.database_name, self.username, self.pid))

    def get_pk(self):
        return hash(
            (self.session_initiation_time, self.database_name, self.username, self.pid)
        )


class Logger:
    def __init__(self, logger_in):
        global logger
        logger = logger_in


def parse_log(
    log_file,
    filename,
    connections,
    last_connections,
    logs,
    databases,
    start_time,
    end_time,
):
    """
    This function parses the different logs and send it to the respective function

    :param log_file: filname(either connectionlog, useractivitylog or startnodelog)
    :param filename: name of the file from s3
    :param connections: the connections dict
    :param last_connections: last_connections dict
    :param logs: logs dict
    :param databases: databases dict
    :param start_time: start_time of extract
    :param end_time: end_time of extract
    """
    if "useractivitylog" in filename:
        logger.debug(f"Parsing user activity log: {filename}")
        parse_user_activity_log(log_file, logs, databases, start_time, end_time)
    elif "connectionlog" in filename:
        logger.debug(f"Parsing connection log: {filename}")
        parse_connection_log(
            log_file, connections, last_connections, start_time, end_time
        )
    elif "start_node" in filename:
        logger.debug(f"Parsing start node log: {filename}")
        parse_start_node_log(log_file, logs, databases, start_time, end_time)


def parse_connection_log(file, connections, last_connections, start_time, end_time):
    for line in file.readlines():
        line = line.decode("utf-8")
        connection_information = line.split("|")
        connection_event = connection_information[0]
        event_time = datetime.datetime.strptime(
            connection_information[1], "%a, %d %b %Y %H:%M:%S:%f"
        ).replace(tzinfo=datetime.timezone.utc)
        pid = connection_information[4]
        database_name = connection_information[5].strip()
        if connection_information[7].strip() == "IAM AssumeUser":
            username = connection_information[6].strip()[4:]
        else:
            username = connection_information[6].strip()
        application_name = connection_information[15]
        if (
            username != "rdsdb"
            and (not start_time or event_time >= start_time)
            and (not end_time or event_time <= end_time)
        ):

            connection_log = ConnectionLog(
                event_time, end_time, database_name, username, pid
            )
            if connection_event == "initiating session ":
                connection_key = connection_log.get_pk()
                # create a new connection
                connections[connection_key] = connection_log
                last_connections[hash(connection_log)] = connection_key
            elif connection_event == "set application_name ":
                if hash(connection_log) in last_connections:
                    connection_key = last_connections[hash(connection_log)]
                    if connection_key in connections:
                        # set the latest connection with application name
                        connections[connection_key].application_name = " ".join(
                            application_name.split()
                        )
                    else:
                        # create new connection if there's no one yet with start
                        # time equals to start of extraction
                        connection_log.session_initiation_time = start_time
                        connection_key = connection_log.get_pk()
                        connections[connection_key] = connection_log
                        last_connections[hash(connection_log)] = connection_key
            elif connection_event == "disconnecting session ":
                if hash(connection_log) in last_connections:
                    connection_key = last_connections[hash(connection_log)]
                    if connection_key in connections:
                        # set the latest connection with disconnection time
                        connections[connection_key].disconnection_time = event_time
                else:
                    # create new connection if there's no one yet with start
                    # time equals to start of extraction
                    connection_log.session_initiation_time = start_time
                    connection_log.disconnection_time = event_time
                    connection_key = connection_log.get_pk()
                    connections[connection_key] = connection_log
                    last_connections[hash(connection_log)] = connection_key


def parse_user_activity_log(file, logs, databases, start_time, end_time):
    user_activity_log = Log()
    datetime_pattern = re.compile(r"'\d+-\d+-\d+T\d+:\d+:\d+Z UTC")
    fetch_pattern = re.compile(
        r"fetch\s+(next|all|forward all|\d+|forward\s+\d+)\s+(from|in)\s+\S+",
        flags=re.IGNORECASE,
    )
    for line in file.readlines():
        line = line.decode("utf-8")
        if datetime_pattern.match(line):
            if user_activity_log.xid and is_valid_log(
                user_activity_log, start_time, end_time
            ):
                filename = user_activity_log.get_filename()
                if filename in logs:
                    # Check if duplicate. This happens with JDBC connections.
                    prev_query = logs[filename][-1]
                    if not is_duplicate(prev_query.text, user_activity_log.text):
                        if fetch_pattern.search(
                            prev_query.text
                        ) and fetch_pattern.search(user_activity_log.text):
                            user_activity_log.text = f"--{user_activity_log.text}"
                            logs[filename].append(user_activity_log)
                        else:
                            logs[filename].append(user_activity_log)
                else:
                    logs[filename] = [user_activity_log]

                databases.add(user_activity_log.database_name)
                user_activity_log = Log()
            line_split = line.split(" LOG: ")
            query_information = line_split[0].split(" ")

            user_activity_log.record_time = dateutil.parser.parse(
                query_information[0][1:]
            )
            user_activity_log.username = query_information[4][5:]
            user_activity_log.database_name = query_information[3][3:]
            user_activity_log.pid = query_information[5][4:]
            user_activity_log.xid = query_information[7][4:]
            user_activity_log.text = line_split[1]
        else:
            user_activity_log.text += line


def parse_start_node_log(file, logs, databases, start_time, end_time):
    start_node_log = Log()

    datetime_pattern = re.compile(r"'\d+-\d+-\d+ \d+:\d+:\d+ UTC")

    for line in file.readlines():

        if datetime_pattern.match(line):
            if start_node_log.xid and is_valid_log(
                start_node_log, start_time, end_time
            ):
                filename = start_node_log.get_filename()
                if filename in logs:
                    # Check if duplicate. This happens with JDBC connections.
                    prev_query = logs[filename][-1]
                    if not is_duplicate(prev_query.text, start_node_log.text):
                        logs[filename].append(start_node_log)
                else:
                    logs[filename] = [start_node_log]

                databases.add(start_node_log.database_name)
                start_node_log = Log()

            line_split = line.split("LOG:  statement: ")

            # We only want to export statements, not errors or contexts
            if len(line_split) == 2:
                query_information = line_split[0].split(" ")

                start_node_log.record_time = dateutil.parser.parse(
                    query_information[0][1:]
                    + " "
                    + query_information[1]
                    + " "
                    + query_information[2]
                )
                start_node_log.database_name = query_information[4].split("@")[1]
                start_node_log.username = query_information[4][3:].split(":")[0]
                start_node_log.pid = query_information[5][4:]
                start_node_log.xid = query_information[7][4:]
                start_node_log.text = line_split[1].strip()
        else:
            start_node_log.text += line



def milliseconds_since_epoch(logs_time):
    return (
        int(
            (
                logs_time.replace(tzinfo=None) - datetime.datetime(1970, 1, 1)
            ).total_seconds()
        )
        * 1000
    )



def parse_cloudwatch_logs(start_time, end_time, config):
    if config["source_cluster_endpoint"]:
        region = config["source_cluster_endpoint"].split(".")[2]
        endpoint = config["source_cluster_endpoint"].split(".")[0]
        print(f"Extracting Cloud watch logs for {endpoint}")
        cloudwatch_client = boto3.client("logs", region)
        response = cloudwatch_client.describe_log_groups()
        cloudwatch_logs = read_cloudwatch_logs(
            response=response,
            config=config,
            cloudwatch_client=cloudwatch_client,
            endpoint=endpoint,
            start_time=start_time,
            end_time=end_time,
        )

    elif config["log_location"]:
        logger.info(f"Extracting logs for {config['log_location']}")
        cloudwatch_client = boto3.client("logs", config["region"])
        response = cloudwatch_client.describe_log_groups(
            logGroupNamePrefix=config["log_location"]
        )
        for loggroupname in response["logGroups"]:
            log_group_name = loggroupname["logGroupName"]
            response_endpoint = cloudwatch_client.describe_log_streams(
                logGroupName=log_group_name
            )
            for endpoint in response_endpoint["logStreams"]:
                end_point = endpoint["logStreamName"]
            cloudwatch_logs = read_cloudwatch_logs(
                response=response,
                config=config,
                cloudwatch_client=cloudwatch_client,
                endpoint=end_point,
                start_time=start_time,
                end_time=end_time,
            )
    return cloudwatch_logs


def read_cloudwatch_logs(
    response, config, cloudwatch_client, endpoint, start_time, end_time
):
    connection_logs = []
    user_logs = []
    connections = {}
    last_connections = {}
    logs = {}
    databases = set()
    workload_location = re.search(
        r"s3://([^/]+)/(.*)", config["workload_location"]
    ).group(1)
    for loggroup in response["logGroups"]:
        log_group_name = loggroup["logGroupName"]
        stream_batch = cloudwatch_client.describe_log_streams(
            logGroupName=log_group_name
        )["logStreams"]
        for stream in stream_batch:
            stream_name = stream["logStreamName"]
            if endpoint == stream_name:
                logger.info(
                    f"Extracting for log group: {log_group_name} between time {start_time} and {end_time}"
                )
                if "useractivitylog" in log_group_name:
                    pagination_parameters = (
                        log_group_name,
                        [stream["logStreamName"]],
                        start_time,
                        end_time,
                        {"MaxItems": 10000},
                    )
                    push_events_to_s3(
                        cloudwatch_client=cloudwatch_client,
                        pagination_parameters=pagination_parameters,
                        log_list=user_logs,
                        log_type="useractivitylog",
                        workload_location=workload_location,
                        connections=connections,
                        last_connections=last_connections,
                        logs=logs,
                        databases=databases,
                    )
                elif "connectionlog" in log_group_name:
                    pagination_parameters = (
                        log_group_name,
                        [stream["logStreamName"]],
                        start_time,
                        end_time,
                        {"MaxItems": 10000},
                    )
                    push_events_to_s3(
                        cloudwatch_client=cloudwatch_client,
                        pagination_parameters=pagination_parameters,
                        log_list=connection_logs,
                        log_type="connectionlog",
                        workload_location=workload_location,
                        connections=connections,
                        last_connections=last_connections,
                        logs=logs,
                        databases=databases,
                    )
    return connections, logs, databases, last_connections


def push_events_to_s3(
    cloudwatch_client,
    pagination_parameters,
    log_list,
    log_type,
    workload_location,
    databases,
    connections,
    logs,
    last_connections,
):

    (
        log_group_name,
        log_stream_names,
        start_time,
        end_time,
        pagination_config,
    ) = pagination_parameters
    paginator = cloudwatch_client.get_paginator("filter_log_events")
    response_iterator = paginator.paginate(
        logGroupName=log_group_name,
        logStreamNames=log_stream_names,
        startTime=milliseconds_since_epoch(start_time),
        endTime=milliseconds_since_epoch(end_time),
        PaginationConfig=pagination_config,
    )
    next_token = None
    while next_token != "":
        for response in response_iterator:
            try:
                next_token = response["nextToken"]
            except KeyError as ke:
                next_token = ""
            for event in response["events"]:
                log_list.append(event["message"])
        pagination_config.update({"StartingToken": next_token})
        response_iterator = paginator.paginate(
            logGroupName=log_group_name,
            logStreamNames=log_stream_names,
            startTime=milliseconds_since_epoch(start_time),
            endTime=milliseconds_since_epoch(end_time),
            PaginationConfig=pagination_config,
        )
    s3_res = boto3.resource("s3")
    logs_gzip = gzip.open(f"{log_type}.gz", "wt")
    logs_gzip.write("\n".join(log_list))
    logs_gzip.close()
    s3_res.meta.client.upload_file(
        f"{log_type}.gz", workload_location, f"{log_type}.gz"
    )
    log_object = s3_res.Object(workload_location, f"{log_type}.gz")
    log_file = gzip.GzipFile(fileobj=log_object.get()["Body"])
    parse_log(
        log_file=log_file,
        filename=f"{log_type}.gz",
        connections=connections,
        last_connections=last_connections,
        logs=logs,
        databases=databases,
        start_time=start_time,
        end_time=end_time,
    )

