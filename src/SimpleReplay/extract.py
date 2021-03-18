import argparse
import gzip
import os
import re
import json
import threading
import time
import yaml
import datetime
from contextlib import contextmanager
import logging

import boto3
from boto3 import client
import dateutil.parser

EXIT_MISSING_VALUE_CONFIG_FILE = 100
EXIT_INVALID_VALUE_CONFIG_FILE = 101

logger = logging.getLogger("ExtractionLogger")
FORMAT = "[%(levelname)s] %(asctime)s %(message)s"
logging.Formatter.converter = time.gmtime
logging.basicConfig(format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
logger.setLevel(logging.INFO)


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

    def get_filename(self, query_index=None):
        base_name = (
            self.database_name + "-" + self.username + "-" + self.pid + "-" + self.xid
        )
        if query_index == 0 or query_index == None:
            return base_name + ".sql"
        else:
            return base_name + "-" + str(query_index) + ".sql"

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
            return hash((self.session_initiation_time, self.database_name, self.username, self.pid))

class SystemLog:
    def __init__(self, start_time, end_time, database_name, user_id, pid, xid, text):
        self.start_time = start_time
        self.end_time = end_time
        self.database_name = database_name
        self.user_id = user_id
        self.pid = pid
        self.xid = xid
        self.text = text

    def __str__(self):
        return (
            "Start time: %s, End time: %s, User id: %s, PID: %s, XID: %s, Query: %s"
            % (
                self.start_time,
                self.end_time,
                self.user_id,
                self.pid,
                self.xid,
                self.text,
            )
        )

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.start_time == other.start_time
            and self.end_time == other.end_time
            and self.user_id == other.user_id
            and self.pid == other.xid
            and self.xid == other.xid
            and self.text == other.text
        )

    def __hash__(self):
        return hash((str(self.pid), str(self.xid), self.text.strip("\n")))


def retrieve_source_cluster_statement_text(
    source_cluster_urls, databases, start_time, end_time, interface
):
    statement_text_logs = {}

    for database_name in databases:
        with initiate_connection(
            source_cluster_urls, interface, database_name
        ) as connection:
            cursor = connection.cursor()

            start_time_where = ""
            end_time_where = ""
            if start_time:
                start_time_where = (
                    f"AND starttime > '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' "
                )
            if end_time:
                end_time_where = (
                    f"AND endtime < '{end_time.strftime('%Y-%m-%d %H:%M:%S')}' "
                )

            cursor.execute(
                "SELECT starttime, endtime, userid, pid, xid, text, sequence "
                "FROM SVL_STATEMENTTEXT "
                f"WHERE userid>1 {start_time_where} {end_time_where}"
                "ORDER BY xid, starttime, sequence;"
            )

            fetch_size = 10000

            rows = cursor.fetchmany(fetch_size)
            complete_log = None
            for row in rows:
                start_time = row[0].replace(tzinfo=datetime.timezone.utc)
                end_time = row[1].replace(tzinfo=datetime.timezone.utc)
                system_log = SystemLog(
                    start_time, end_time, database_name, row[2], row[3], row[4], row[5],
                )

                if row[6] == 0:
                    if complete_log is None:
                        complete_log = system_log
                    else:
                        if hash(complete_log) in statement_text_logs:
                            statement_text_logs[hash(complete_log)].append(complete_log)
                        else:
                            statement_text_logs[hash(complete_log)] = [complete_log]
                        complete_log = system_log
                else:
                    if not complete_log is None:
                        complete_log.text = complete_log.text + system_log.text

                rows = cursor.fetchmany(fetch_size)

            if hash(complete_log) in statement_text_logs:
                statement_text_logs[hash(complete_log)].append(complete_log)
            else:
                statement_text_logs[hash(complete_log)] = [complete_log]

    return statement_text_logs


def combine_logs(audit_logs, statement_text_logs):
    for audit_transaction in audit_logs:
        for audit_query in audit_logs[audit_transaction]:
            matching_statement_text_logs = statement_text_logs.get(hash(audit_query))
            if matching_statement_text_logs:
                statement_text_log = matching_statement_text_logs.pop()
                if statement_text_log:
                    if statement_text_log.start_time:
                        audit_query.start_time = statement_text_log.start_time
                    if statement_text_log.end_time:
                        audit_query.end_time = statement_text_log.end_time


@contextmanager
def initiate_connection(cluster_urls, interface, database_name):
    conn = None
    if interface == "odbc":
        cluster_split = cluster_urls["odbc"].split(";")
        cluster_split[2] = "Database=" + database_name
        cluster = ";".join(cluster_split)
        try:
            conn = pyodbc.connect(cluster, autocommit=True)
            yield conn
        finally:
            if conn is not None:
                conn.close()
    elif interface == "psql":
        try:
            conn = pg8000.connect(
                user=cluster_urls["psql"]["username"],
                password=cluster_urls["psql"]["password"],
                host=cluster_urls["psql"]["host"],
                port=cluster_urls["psql"]["port"],
                database=database_name,
                ssl_context=True,
            )
            conn.autocommit = True
            yield conn
        finally:
            if conn is not None:
                conn.close()


def get_local_logs(log_directory_path, start_time, end_time):
    connections = {}
    last_connections = {}
    logs = {}
    databases = set()


    unsorted_list = os.listdir(log_directory_path)         
    log_directory = sorted(unsorted_list)                  

    for filename in log_directory:
        if "start_node" in filename:
            log_file = gzip.open(
                log_directory_path + "/" + filename, "rt", encoding="ISO-8859-1"
            )
        else:
            log_file = gzip.open(log_directory_path + "/" + filename, "r")
        parse_log(
            log_file, filename, connections, last_connections, logs, databases, start_time, end_time,
        )
        log_file.close()

    return (connections, logs, databases, last_connections)


def parse_log(
    log_file, filename, connections, last_connections, logs, databases, start_time, end_time,         
):
    if "useractivitylog" in filename:
        logger.debug(f"Parsing user activity log: {filename}")
        parse_user_activity_log(log_file, logs, databases, start_time, end_time)
    elif "connectionlog" in filename:
        logger.debug(f"Parsing connection log: {filename}")
        parse_connection_log(log_file, connections, last_connections, start_time, end_time)         
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
        if connection_information[7].strip() == 'IAM AssumeUser':
            username = connection_information[6].strip()[4:]
        else:
            username = connection_information[6].strip()
        application_name = connection_information[15]


#        if username != "rdsdb":
#            if connection_event == "initiating session ":
#                if not start_time or (start_time and event_time > start_time):
#                    connections[connection_key] = connection_log
#            elif connection_event == "set application_name ":
#                if connection_key in connections:
#                    connections[connection_key].application_name = " ".join(
#                        application_name.split()
#                    )
#            elif connection_event == "disconnecting session ":
#                if connection_key in connections:
#                    if end_time and event_time > end_time:
#                        del connections[connection_key]
#                    else:
#                        connection = connections[connection_key]
#                        connection.disconnection_time = event_time

        if username != "rdsdb" and event_time >= start_time and event_time <= end_time:

            connection_log = ConnectionLog(event_time, end_time, database_name, username, pid)

            if connection_event == "initiating session ":
                connection_key = connection_log.get_pk()
                # create a new connection
                connections[connection_key] = connection_log
                last_connections[hash(connection_log)]=connection_key
            elif connection_event == "set application_name ":
                if hash(connection_log) in last_connections:
                    connection_key = last_connections[hash(connection_log)]
                    if connection_key in connections:
                        # set the latest connection with application name
                        connections[connection_key].application_name = " ".join( application_name.split())
                    else: # create new connection if there's no one yet with start time equals to start of extraction
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
                else: # create new connection if there's no one yet with start time equals to start of extraction
                    connection_log.session_initiation_time = start_time
                    connection_log.disconnection_time = event_time
                    connection_key = connection_log.get_pk()
                    connections[connection_key] = connection_log
                    last_connections[hash(connection_log)] = connection_key


def parse_user_activity_log(file, logs, databases, start_time, end_time):
    user_activity_log = Log()

    datetime_pattern = re.compile(r"'\d+-\d+-\d+T\d+:\d+:\d+Z UTC")
    fetch_pattern = re.compile(r"fetch\s+(next|all|forward all|\d+|forward\s+\d+)\s+(from|in)\s+\S+", flags=re.IGNORECASE)
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
                        if fetch_pattern.search(prev_query.text) and fetch_pattern.search(user_activity_log.text):
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


def is_valid_log(log, start_time, end_time):
    """If query doesn't contain problem statements, saves it."""
    problem_keywords = [
        "SPECTRUM INTERNAL QUERY",
        "context: SQL",
        "ERROR:",
        "CONTEXT:  SQL",
        "show ",
        "Undoing transaction",
        "Undo on",
        "pg_terminate_backend",
        "pg_cancel_backend",
        "volt_",
        "pg_temp_",
        "BIND",
    ]

    if log.username == "rdsdb":
        return False

    if start_time and log.record_time < start_time:
        return False

    if end_time and log.record_time > end_time:
        return False

    if any(word in log.text for word in problem_keywords):
        return False

    return True


def is_duplicate(first_query_text, second_query_text):
    dedupe_these = [
        "set",
        "select",
        "create",
        "delete",
        "update",
        "insert",
        "copy",
        "unload",
        "with"
    ]

    first_query_text = first_query_text.strip().replace(";", "")
    second_query_text = second_query_text.strip().replace(";", "")
    second_query_comment_removed = second_query_text
    if second_query_text.startswith("/*"):
        second_query_comment_removed = second_query_text[second_query_text.find('*/')+2:len(second_query_text)].strip()
    return (
        (
                first_query_text == second_query_text
                and any(second_query_comment_removed.startswith(word) for word in dedupe_these))
    )



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

def connection_time_replacement(sorted_connections):
    i = 0
    min_init_time = sorted_connections[0]['session_initiation_time']
    max_disconnect_time = sorted_connections[0]['disconnection_time']
    empty_init_times = []
    empty_disconnect_times = []
    for connection in sorted_connections:
        if connection['session_initiation_time'] == '':
            empty_init_times.append(i)
        elif min_init_time > connection['session_initiation_time']:
            min_init_time = connection['session_initiation_time']

        if connection['disconnection_time'] == '':
            empty_disconnect_times.append(i)

        elif max_disconnect_time == '' or ( max_disconnect_time and max_disconnect_time < connection['disconnection_time']):
            max_disconnect_time = connection['disconnection_time']
            
        i += 1 
    for init_time in empty_init_times:
        sorted_connections[init_time]['session_initiation_time'] = min_init_time

    for init_time in empty_disconnect_times:
        sorted_connections[init_time]['disconnection_time'] = max_disconnect_time
   
    return sorted_connections

"""
Remove single line comments
If a line comment is inside a block comment, then the line comment ends at the end of the comment

param query: the multiline query to remove single line comments from
return: a string of the update query lines
"""
def remove_line_comments(query):
    removed_string = query
    prev_location = 0

    while True:
        line_comment_begin = removed_string.find('--', prev_location)
        prev_location = line_comment_begin

        # no more comments to find
        if line_comment_begin == -1:
            break

        #found_comment = True
        linebreak = removed_string.find('\n', line_comment_begin)
        start_comment = removed_string.find('/*', line_comment_begin, linebreak if linebreak != -1 else len(removed_string))
        end_comment = removed_string.find('*/', line_comment_begin, linebreak if linebreak != -1 else len(removed_string))

        if linebreak != -1:
            if start_comment == -1 and end_comment != -1:
                # if line comment is between start and end, then remove until end of comment
                removed_string = removed_string[:line_comment_begin] + removed_string[end_comment:]
            else:
                # else remove up the end of line
                removed_string = removed_string[:line_comment_begin] + removed_string[linebreak:]
        else:
            # reached end of query
            if start_comment == -1 and end_comment != -1:
                # if line comment is between start and end, then remove until end of comment
            
                removed_string = removed_string[:line_comment_begin] + removed_string[end_comment:]
            else:
                # else remove up the end of line
                removed_string = removed_string[:line_comment_begin]
   
    return removed_string

def save_logs(logs, last_connections, output_directory):
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
    else:
        is_s3 = False
        os.makedirs(output_directory + "/SQLs/")
    missing_audit_log_connections = set()

    # Save the main logs and find replacements
    replacements = set()
    for filename, queries in logs.items():
        file_text = "--Time interval: true\n\n"
        for query in queries:
            query.text = remove_line_comments(query.text).strip()
            time_info = "--Record time: " + query.record_time.isoformat() + "\n"
            if query.start_time:
                time_info += "--Start time: " + query.start_time.isoformat() + "\n"
            if query.end_time:
                time_info += "--End time: " + query.end_time.isoformat() + "\n"

            if "copy " in query.text.lower() and "from 's3:" in query.text.lower(): #Raj
                bucket = re.search(r"from 's3:\/\/[^']*", query.text, re.IGNORECASE).group()[6:]
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
            if not len(query.text) == 0:
                query.text = f"/* Replay source file: {filename} */ {query.text.strip()}"
                if not query.text.endswith(";"):
                    query.text += ";"
                file_text += time_info + query.text + "\n"

            if (
                not hash((query.database_name, query.username, query.pid)) in last_connections         

            ):
                missing_audit_log_connections.add(
                    (query.database_name, query.username, query.pid)
                )

        if is_s3:
            s3_client.put_object(
                Body=file_text.strip(),
                Bucket=bucket_name,
                Key=output_prefix + "/SQLs/" + filename,
            )
        else:
            f = open(output_directory + "/SQLs/" + filename, "w")
            f.write(file_text.strip())
            f.close()

    logger.info(f"Generating {len(missing_audit_log_connections)} missing connections.")
    for missing_audit_log_connection_info in missing_audit_log_connections:
        connection = ConnectionLog(
            start_time,end_time, # for missing connections set start_time and end_time
            missing_audit_log_connection_info[0],
            missing_audit_log_connection_info[1],
            missing_audit_log_connection_info[2],
        )
        pk = connection.get_pk()
        connections[pk]=connection
    logger.info(
        f"Exporting a total of {len(connections.values())} connections to {output_directory}"
    )
    # Save the connections logs
    sorted_connections = connections.values()
    connections_dict = connection_time_replacement([connection.__dict__ for connection in sorted_connections])
    connections_string = json.dumps(
        #connections_dict,
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


def get_cluster_log_location(source_cluster_endpoint):
    logging_status = client("redshift").describe_logging_status(
        ClusterIdentifier=source_cluster_endpoint.split(".")[0]
    )
    if logging_status["LoggingEnabled"]:
        bucket_name = logging_status["BucketName"]

        if "S3KeyPrefix" in logging_status:
            return (bucket_name, logging_status["S3KeyPrefix"])
        else:
            return (bucket_name, "")
    return ("", "")


def get_s3_logs(log_bucket, log_prefix, start_time, end_time):
    connections = {}
    logs = {}
    last_connections = {}
    databases = set()

    conn = client("s3")

    # get first set of 
    response = conn.list_objects_v2(Bucket=log_bucket, 
                                    Prefix=log_prefix
    )
    bucket_objects = response["Contents"]

    if "NextContinuationToken" in response:
        prev_key = response["NextContinuationToken"]
        while True:
            response = conn.list_objects_v2(Bucket=log_bucket, 
                                            Prefix=log_prefix,
                                            ContinuationToken=prev_key
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
    #return (connections, logs, databases)


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
    s3 = boto3.resource("s3")
    #last_connections = {}

    index_of_last_valid_log = len(audit_objects) - 1

    if end_time:
        for index, log in reversed(list(enumerate(audit_objects))):
            filename = log["Key"].split("/")[-1]
            file_datetime = dateutil.parser.parse(filename.split("_")[-1][:-3]).replace(
                tzinfo=datetime.timezone.utc
            )

            if file_datetime < end_time:
                index_of_last_valid_log = min(index_of_last_valid_log, index + 2)
                logger.debug(
                    f'Last audit log file in end_time range: {audit_objects[index_of_last_valid_log]["Key"].split("/")[-1]}'
                )
                break

    is_continue_parsing = True
    curr_index = index_of_last_valid_log

    while is_continue_parsing and curr_index >= 0:
        filename = audit_objects[curr_index]["Key"].split("/")[-1]
        file_datetime = dateutil.parser.parse(filename.split("_")[-1][:-3]).replace(
            tzinfo=datetime.timezone.utc
        )

        curr_connection_length = len(connections)
        curr_logs_length = len(logs)

        log_object = s3.Object(log_bucket, audit_objects[curr_index]["Key"])
        log_file = gzip.GzipFile(fileobj=log_object.get()["Body"])

        parse_log(
            log_file, filename, connections, last_connections, logs, databases, start_time, end_time,
        )

        if (
            start_time
            and file_datetime < start_time
            and len(connections) == curr_connection_length
            and len(logs) == curr_logs_length
        ):
            is_continue_parsing = False
        else:
            curr_index -= 1

    logger.debug(
        f'First audit log in start_time range: {audit_objects[curr_index]["Key"].split("/")[-1]}'
    )
    return (connections, logs, databases, last_connections)


def get_connection_string(cluster_endpoint, username, odbc_driver):
    cluster_endpoint_split = cluster_endpoint.split(".")
    cluster_id = cluster_endpoint_split[0]
    cluster_host = cluster_endpoint.split(":")[0]
    cluster_port = cluster_endpoint_split[5].split("/")[0][4:]
    cluster_database = cluster_endpoint_split[5].split("/")[1]

    try:
        response = client("redshift").get_cluster_credentials(
            DbUser=username, ClusterIdentifier=cluster_id, AutoCreate=False,
        )
        cluster_odbc_url = (
            "Driver={%s}; Server=%s; Database=%s; IAM=1; DbUser=%s; DbPassword=%s; Port=%s"
            % (
                odbc_driver,
                cluster_host,
                cluster_database,
                response["DbUser"].split(":")[1],
                response["DbPassword"],
                cluster_port,
            )
        )
        cluster_psql = {
            "username": response["DbUser"],
            "password": response["DbPassword"],
            "host": cluster_host,
            "port": cluster_port,
            "database": cluster_database,
        }
        return {"odbc": cluster_odbc_url, "psql": cluster_psql}
    except Exception as err:
        logger.error("ERROR: Failed to generate connection string. " + str(err))
        return ""


def unload_system_table(
    source_cluster_urls,
    odbc_driver,
    unload_system_table_queries_file,
    unload_location,
    unload_iam_role,
):
    conn = None
    if odbc_driver:
        conn = pyodbc.connect(source_cluster_urls["odbc"])
    else:
        conn = pg8000.connect(
            user=source_cluster_urls["psql"]["username"],
            password=source_cluster_urls["psql"]["password"],
            host=source_cluster_urls["psql"]["host"],
            port=source_cluster_urls["psql"]["port"],
            database=source_cluster_urls["psql"]["database"],
            ssl_context=True,
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
    if config_file["source_cluster_endpoint"]:
        if (
            not len(config_file["source_cluster_endpoint"].split(".")) == 6
            or not len(config_file["source_cluster_endpoint"].split(":")) == 2
            or not len(config_file["source_cluster_endpoint"].split("/")) == 2
            or not ".redshift.amazonaws.com:" in config_file["source_cluster_endpoint"]
        ):
            logger.error(
                'Config file value for "source_cluster_endpoint" is not a valid endpoint. Endpoints must be in the format of <cluster-name>.<identifier>.<region>.redshift.amazonaws.com:<port>/<database-name>.'
            )
            exit(EXIT_INVALID_VALUE_CONFIG_FILE)
        if not config_file["master_username"]:
            logger.error(
                'Config file missing value for "master_username". Please provide a value or remove the "source_cluster_endpoint" value.'
            )
            exit(EXIT_MISSING_VALUE_CONFIG_FILE)
    else:
        if not config_file["log_location"]:
            logger.error(
                'Config file missing value for "log_location". Please provide a value for "log_location", or provide a value for "source_cluster_endpoint".'
            )
            exit(EXIT_MISSING_VALUE_CONFIG_FILE)
    if not config_file["start_time"]:
        logger.error(
            'Config file is missing "start_time". Please provide a valid "start_time" for extract.'
        )
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    else:
        try:
            dateutil.parser.isoparse(config_file["start_time"])
        except ValueError:
            logger.error(
                'Config file "start_time" value not formatted as ISO 8601. Please format "start_time" as ISO 8601 or remove its value.'
            )
            exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    if not config_file["end_time"]:
        logger.error(
            'Config file is missing "end_time". Please provide a valid "end_time" for extract.'
        )
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    else:
        try:
            dateutil.parser.isoparse(config_file["end_time"])
        except ValueError:
            logger.error(
                'Config file "end_time" value not formatted as ISO 8601. Please format "end_time" as ISO 8601 or remove its value.'
            )
            exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    if not config_file["start_time"]:
        logger.error(
            'Config file missing value for "start_time". Please provide a value for "start_time".'
        )
        exit(EXIT_MISSING_VALUE_CONFIG_FILE)
    if not config_file["end_time"]:
        logger.error(
            'Config file missing value for "end_time". Please provide a value for "end_time".'
        )
        exit(EXIT_MISSING_VALUE_CONFIG_FILE)
    if not config_file["workload_location"]:
        logger.error(
            'Config file missing value for "workload_location". Please provide a value for "workload_location".'
        )
        exit(EXIT_MISSING_VALUE_CONFIG_FILE)
    if not config_file["workload_location"].startswith("s3://") and os.path.exists(
        config_file["workload_location"]
    ):
        logger.error(
            f'Output already exists at "{config_file["workload_location"]}". Please move or delete the existing output, or change the "workload_location" value.'
        )
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    if config_file["source_cluster_system_table_unload_location"] and not config_file[
        "source_cluster_system_table_unload_location"
    ].startswith("s3://"):
        logger.error(
            'Config file value for "source_cluster_system_table_unload_location" must be an S3 location (starts with "s3://"). Please remove this value or put in an S3 location.'
        )
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    if (
        config_file["source_cluster_system_table_unload_location"]
        and not config_file["source_cluster_system_table_unload_iam_role"]
    ):
        logger.error(
            'Config file missing value for "source_cluster_system_table_unload_iam_role". Please provide a value for "source_cluster_system_table_unload_iam_role", or remove the value for "source_cluster_system_table_unload_location".'
        )
        exit(EXIT_MISSING_VALUE_CONFIG_FILE)
    if (
        config_file["source_cluster_system_table_unload_location"]
        and not config_file["unload_system_table_queries"]
    ):
        logger.error(
            'Config file missing value for "unload_system_table_queries". Please provide a value for "unload_system_table_queries", or remove the value for "source_cluster_system_table_unload_location".'
        )
        exit(EXIT_MISSING_VALUE_CONFIG_FILE)
    if config_file["unload_system_table_queries"] and not config_file[
        "unload_system_table_queries"
    ].endswith(".sql"):
        logger.error(
            'Config file value for "unload_system_table_queries" does not end with ".sql". Please ensure the value for "unload_system_table_queries" ends in ".sql". See the provided "unload_system_tables.sql" as an example.'
        )
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "config_file",
        type=argparse.FileType("r"),
        help="Location of extraction config file.",
    )
    args = parser.parse_args()

    config_file = {}
    with args.config_file as stream:
        try:
            config_file = yaml.safe_load(stream)
        except yaml.YAMLError as exception:
            logger.error(f"Failed to load extraction config yaml file.\n{exception}")

    validate_config_file(config_file)

    if config_file["source_cluster_endpoint"]:
        extraction_name = f'Extraction_{config_file["source_cluster_endpoint"].split(".")[0]}_{datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat()}'
    else:
        extraction_name = f"Extraction_{datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat()}"

    try:
        if config_file["odbc_driver"]:
            import pyodbc

            interface = "odbc"
        else:
            import pg8000

            interface = "psql"
    except Exception as err:
        if config_file["odbc_driver"]:
            logger.error(
                'Error while importing pyodbc. Please ensure pyodbc is correctly installed or remove the value for "odbc_driver" to use pg8000.'
            )
        else:
            logger.error(
                'Error while importing pg8000. Please ensure pg8000 is correctly installed or add an ODBC driver name value for "odbc_driver" to use pyodbc.'
            )
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)

    if config_file["start_time"]:
        start_time = dateutil.parser.parse(config_file["start_time"]).astimezone(
            dateutil.tz.tzutc()
        )
    else:
        start_time = ""

    if config_file["end_time"]:
        end_time = dateutil.parser.parse(config_file["end_time"]).astimezone(
            dateutil.tz.tzutc()
        )
    else:
        end_time = ""

    if config_file["log_location"]:
        logger.info(f'Retrieving logs from {config_file["log_location"]}')
        if config_file["log_location"].startswith("s3://"):
            log_bucket = config_file["log_location"][5:].split("/")[0]
            log_prefix = config_file["log_location"][5:].partition("/")[2]
            (connections, audit_logs, databases, last_connections) = get_s3_logs(
                log_bucket, log_prefix, start_time, end_time,
            )
        else:
            (connections, audit_logs, databases, last_connections) = get_local_logs(
                config_file["log_location"], start_time, end_time,
            )
    else:
        logger.info(
            f'Getting log location from {config_file["source_cluster_endpoint"]}'
        )
        (log_bucket, log_prefix) = get_cluster_log_location(
            config_file["source_cluster_endpoint"]
        )

        logger.info(f"Retrieving logs from s3://{log_bucket}/{log_prefix}")
        (connections, audit_logs, databases,last_connections) = get_s3_logs(
            log_bucket, log_prefix, start_time, end_time,
        )

    if config_file["source_cluster_endpoint"]:
        logger.info(f'Retrieving info from {config_file["source_cluster_endpoint"]}')
        source_cluster_urls = get_connection_string(
            config_file["source_cluster_endpoint"],
            config_file["master_username"],
            config_file["odbc_driver"],
        )

        source_cluster_statement_text_logs = retrieve_source_cluster_statement_text(
            source_cluster_urls, databases, start_time, end_time, interface,
        )

        combine_logs(audit_logs, source_cluster_statement_text_logs)

        if (
            config_file["source_cluster_system_table_unload_location"]
            and config_file["unload_system_table_queries"]
            and config_file["source_cluster_system_table_unload_iam_role"]
        ):
            logger.info(
                f'Exporting system tables to {config_file["source_cluster_system_table_unload_location"]}'
            )

            unload_system_table(
                source_cluster_urls,
                config_file["odbc_driver"],
                config_file["unload_system_table_queries"],
                config_file["source_cluster_system_table_unload_location"] + "/" + extraction_name,
                config_file["source_cluster_system_table_unload_iam_role"],
            )

            logger.info(
                f'Exported system tables to {config_file["source_cluster_system_table_unload_location"]}'
            )

    save_logs(
        audit_logs,
        last_connections,
        config_file["workload_location"] + "/" + extraction_name,
    )

