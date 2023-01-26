import datetime
import logging
import re

import dateutil.parser

from audit_logs_parsing import (Log, ConnectionLog)
from log_validation import is_valid_log, is_duplicate

logger = logging.getLogger("SimpleReplayLogger")


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
        _parse_user_activity_log(log_file, logs, databases, start_time, end_time)
    elif "connectionlog" in filename:
        logger.debug(f"Parsing connection log: {filename}")
        _parse_connection_log(
            log_file, connections, last_connections, start_time, end_time
        )
    elif "start_node" in filename:
        logger.debug(f"Parsing start node log: {filename}")
        _parse_start_node_log(log_file, logs, databases, start_time, end_time)


def _parse_user_activity_log(file, logs, databases, start_time, end_time):
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


def _parse_start_node_log(file, logs, databases, start_time, end_time):
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


def _parse_connection_log(file, connections, last_connections, start_time, end_time):
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
