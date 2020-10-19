import argparse
import os
import json
import yaml
import csv
import time
import string
import random
import datetime
import threading
from collections import namedtuple
import re
from contextlib import contextmanager
import logging

from boto3 import client
from boto3 import resource

#import pyodbc
import pg8000
import dateutil.parser

EXIT_MISSING_VALUE_CONFIG_FILE = 100
EXIT_INVALID_VALUE_CONFIG_FILE = 101

logger = logging.getLogger("ReplayLogger")
FORMAT = "[%(levelname)s] %(asctime)s %(message)s"
logging.Formatter.converter = time.gmtime
logging.basicConfig(format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
logger.setLevel(logging.DEBUG)


target_cluster_urls = {}
refresh_target_cluster_urls_thread = None

queries_executed = 0
transactions_executed = 0


class ConnectionLog:
    def __init__(
        self,
        session_initiation_time,
        disconnection_time,
        application_name,
        database_name,
        username,
        pid,
        time_interval_between_transactions,
        time_interval_between_queries,
    ):
        self.session_initiation_time = session_initiation_time
        self.disconnection_time = disconnection_time
        self.application_name = application_name
        self.database_name = database_name
        self.username = username
        self.pid = pid
        self.query_index = 0
        self.time_interval_between_transactions = time_interval_between_transactions
        self.time_interval_between_queries = time_interval_between_queries
        self.transactions = []

    def __str__(self):
        return (
            "Session initiation time: %s, Disconnection time: %s, Application name: %s, Database name: %s, Username; %s, PID: %s, Time interval between transactions: %s, Time interval between queries: %s, Number of transactions: %s"
            % (
                self.session_initiation_time.isoformat(),
                self.disconnection_time.isoformat(),
                self.application_name,
                self.database_name,
                self.username,
                self.pid,
                self.time_interval_between_transactions,
                self.time_interval_between_queries,
                len(self.transactions),
            )
        )


class Transaction:
    def __init__(self, time_interval, database_name, username, pid, xid, queries):
        self.time_interval = time_interval
        self.database_name = database_name
        self.username = username
        self.pid = pid
        self.xid = xid
        self.queries = queries

    def __str__(self):
        return (
            "Time interval: %s, Database name: %s, Username: %s, PID: %s, XID: %s, Num queries: %s"
            % (
                self.time_interval,
                self.database_name,
                self.username,
                self.pid,
                self.xid,
                len(self.queries),
            )
        )

    def get_base_filename(self):
        return (
            self.database_name + "-" + self.username + "-" + self.pid + "-" + self.xid
        )


class Query:
    def __init__(self, start_time, end_time, text):
        self.start_time = start_time
        self.end_time = end_time
        self.time_interval = 0
        self.text = text

    def __str__(self):
        return "Start time: %s, End time: %s, Time interval: %s, Text: %s" % (
            self.start_time.isoformat(),
            self.end_time.isoformat(),
            self.time_interval,
            self.text.strip(),
        )


class ConnectionThread(threading.Thread):
    def __init__(
        self,
        connection_log,
        default_interface,
        connection_errors,
        transaction_errors,
        replay_time,
    ):
        threading.Thread.__init__(self)
        self.connection_log = connection_log
        self.default_interface = default_interface
        self.replay_time = replay_time
        self.connection_errors = connection_errors
        self.transaction_errors = transaction_errors

    def run(self):
        with self.initiate_connection() as connection:
            if connection:
                time_diff = (
                    datetime.datetime.now().replace(tzinfo=datetime.timezone.utc)
                    - self.replay_time
                )

                self.set_session_authorization(connection)

                self.execute_transactions(time_diff, connection)

                if self.connection_log.time_interval_between_transactions == True:
                    while (
                        datetime.datetime.now().replace(tzinfo=datetime.timezone.utc)
                        - time_diff
                        < self.connection_log.disconnection_time
                    ):
                        time.sleep(0.5)

        logger.debug(f"Disconnected PID: {self.connection_log.pid}")

    @contextmanager
    def initiate_connection(self):
        conn = None

        interface = self.default_interface
        if "psql" in self.connection_log.application_name.lower():
            interface = "psql"
        elif "odbc" in self.connection_log.application_name.lower():
            interface = "odbc"

        try:
            try:
                if interface == "psql":
                    conn = pg8000.connect(
                        user=target_cluster_urls["psql"]["username"],
                        password=target_cluster_urls["psql"]["password"],
                        host=target_cluster_urls["psql"]["host"],
                        port=target_cluster_urls["psql"]["port"],
                        database=self.connection_log.database_name,
                        ssl_context=True,
                    )
                else:
                    target_cluster_split = target_cluster_urls["odbc"].split(";")
                    target_cluster_split[2] = (
                        "Database=" + self.connection_log.database_name
                    )
                    target_cluster = ";".join(target_cluster_split)
                    conn = pyodbc.connect(target_cluster)

                conn.autocommit = False
                logger.debug(
                    f"Connected using {interface} for PID: {self.connection_log.pid}"
                )
            except Exception as err:
                logger.debug(err)
                self.connection_errors[
                    f"{self.connection_log.database_name}-{self.connection_log.username}-{self.connection_log.pid}"
                ] = f"{self.connection_log}\n\n{err}"
                pass
            yield conn
        finally:
            if conn is not None:
                conn.close()

    def set_session_authorization(self, connection):
        cursor = connection.cursor()

        session_auth_username = self.connection_log.username
        if session_auth_username.startswith("IAM:"):
            session_auth_username = session_auth_username[4:]
        cursor.execute(f"SET SESSION AUTHORIZATION '{session_auth_username}';")

        connection.commit()
        cursor.close()

    def execute_transactions(self, time_diff, connection):
        if self.connection_log.time_interval_between_transactions == True:
            curr_transaction_index = 0

            while curr_transaction_index < len(self.connection_log.transactions):
                curr_transaction = self.connection_log.transactions[
                    curr_transaction_index
                ]
                if (
                    curr_transaction.queries[0].start_time
                    <= datetime.datetime.now().replace(tzinfo=datetime.timezone.utc)
                    - time_diff
                ):
                    self.execute_transaction(curr_transaction, connection)
                    curr_transaction_index += 1
                else:
                    time.sleep(0.5)
        else:
            for transaction in self.connection_log.transactions:
                self.execute_transaction(transaction, connection)

    def execute_transaction(self, transaction, connection):
        errors = []
        cursor = connection.cursor()

        is_transaction_success = True

        for query_index, query in enumerate(transaction.queries):
            try:
                #cursor.execute(query.text)
                if (config_file["execute_copy_statements"] == "true" and  "from 's3:" in query.text.lower()):
                    cursor.execute(query.text)
                elif (config_file["execute_unload_statements"] == "true" and  "to 's3:" in query.text.lower()):
                    cursor.execute(query.text)
                elif ("from 's3:" not in query.text.lower()) and ("to 's3:" not in query.text.lower()):
                    
                    cursor.execute(query.text)
                
                logger.debug(
                    f"Replayed PID={transaction.pid}, XID:{transaction.xid}, Query: {query_index + 1}/{len(transaction.queries)}"
                )
                global queries_executed
                queries_executed += 1
            except Exception as err:
                errors.append([query, err])
                logger.debug(
                    f"Failed PID={transaction.pid}, XID:{transaction.xid}, Query: {query_index + 1}/{len(transaction.queries)}. {err}"
                )
                is_transaction_success = False
                pass

            if not query.time_interval <= 0.0:
                time.sleep(query.time_interval)

        cursor.close()
        connection.commit()

        if is_transaction_success:
            global transactions_executed
            transactions_executed += 1

        if errors:
            self.transaction_errors[transaction.get_base_filename()] = errors


def parse_connections(
    workload_directory,
    time_interval_between_transactions,
    time_interval_between_queries,
):
    connections = []

    if workload_directory.startswith("s3://"):
        workload_s3_location = workload_directory[5:].partition("/")
        bucket_name = workload_s3_location[0]
        prefix = workload_s3_location[2]

        s3_object = client("s3").get_object(
            Bucket=bucket_name, Key=prefix + "/connections.json"
        )
        connections_json = json.loads(s3_object["Body"].read())
    else:
        connections_file = open(workload_directory + "/connections.json", "r")
        connections_json = json.loads(connections_file.read())
        connections_file.close()

    for connection_json in connections_json:
        is_time_interval_between_transactions = {
            "": connection_json["time_interval_between_transactions"],
            "all on": True,
            "all off": False,
        }[time_interval_between_transactions]
        is_time_interval_between_queries = {
            "": connection_json["time_interval_between_queries"],
            "all on": "all on",
            "all off": "all off",
        }[time_interval_between_queries]

        try:
            if connection_json["session_initiation_time"]:
                session_initiation_time = dateutil.parser.isoparse(
                    connection_json["session_initiation_time"]
                ).replace(tzinfo=datetime.timezone.utc)
            else:
                session_initiation_time = None

            if connection_json["disconnection_time"]:
                disconnection_time = dateutil.parser.isoparse(
                    connection_json["disconnection_time"]
                ).replace(tzinfo=datetime.timezone.utc)
            else:
                disconnection_time = None

            connection = ConnectionLog(
                session_initiation_time,
                disconnection_time,
                connection_json["application_name"],
                connection_json["database_name"],
                connection_json["username"],
                connection_json["pid"],
                is_time_interval_between_transactions,
                is_time_interval_between_queries,
            )
            connections.append(connection)
        except Exception as err:
            logger.error(f"Could not parse connection: \n{str(connection_json)}\n{err}")
            pass

    return connections


def parse_transactions(workload_directory):
    connection_transactions = {}

    if workload_directory.startswith("s3://"):
        workload_s3_location = workload_directory[5:].partition("/")
        bucket_name = workload_s3_location[0]
        prefix = workload_s3_location[2]
        #bucket_name = workload_directory.split("/")[2]
        #prefix = workload_directory.split("/")[3]

        conn = client("s3")
        s3 = resource("s3")
        for log in conn.list_objects_v2(Bucket=bucket_name, Prefix=prefix + "/SQLs/")[
            "Contents"
        ]:
            filename = log["Key"].split("/")[-1]
            if filename.endswith(".sql"):
                sql_file_text = (
                    s3.Object(bucket_name, log["Key"])
                    .get()["Body"]
                    .read()
                    .decode("utf-8")
                )
                transaction = parse_transaction(filename, sql_file_text)

                if transaction.pid in connection_transactions:
                    connection_transactions[transaction.pid].append(transaction)
                else:
                    connection_transactions[transaction.pid] = [transaction]
    else:
        sqls_directory = os.listdir(workload_directory + "/SQLs/")
        for sql_filename in sqls_directory:
            if sql_filename.endswith(".sql"):
                sql_file_text = open(
                    workload_directory + "/SQLs/" + sql_filename, "r"
                ).read()
                transaction = parse_transaction(sql_filename, sql_file_text)
                ## Raj added
                #if not transaction.queries[0].start_time:
                #    print(transaction.pid, " transaction.pid ")
                
                
                if transaction.queries[0].start_time:
                    if transaction.pid in connection_transactions:
                        connection_transactions[transaction.pid].append(transaction)
                    else:
                        connection_transactions[transaction.pid] = [transaction]

    for transactions in connection_transactions.values():
        #print(transaction.xid)
        #print("1")
        #print(transaction.queries[0].start_time)
        transactions.sort(
            key=lambda transaction: (transaction.queries[0].start_time, transaction.xid)
        )

    return connection_transactions


def parse_transaction(sql_filename, sql_file_text):
    #print(sql_filename)
    sql_filename_split = sql_filename.split(".")[0].split("-")
    database_name = sql_filename_split[0]
    username = sql_filename_split[1]
    pid = sql_filename_split[2]
    xid = sql_filename_split[3]

    if xid.endswith(".sql"):
        xid = xid[:-4]

    queries = []
    time_interval = True

    query_start_time = ""
    query_end_time = ""
    query_text = ""
    for line in sql_file_text.splitlines():
        if line.startswith("--Time interval"):
            time_interval = line.split("--Time interval: ")[1].strip()
        if line.startswith("--Record time"):
            if query_text.strip():
                query = Query(query_start_time, query_end_time, query_text.strip())
                queries.append(query)
                query_text = ""

            query_start_time = dateutil.parser.isoparse(line.split(": ")[1].strip())
            #if not query_start_time:
                #print("query_start_time ",query_start_time," xid ",xid)
            query_end_time = query_start_time
        elif line.startswith("--Start time"):
            query_start_time = dateutil.parser.isoparse(line.split(": ")[1].strip())
            #if not query_start_time:
                #print("query_start_time ",query_start_time, " xid ", xid )
        elif line.startswith("--End time"):
            query_end_time = dateutil.parser.isoparse(line.split(": ")[1].strip())
        elif not line.startswith("--"):
            query_text += " " + line

    queries.append(Query(query_start_time, query_end_time, query_text.strip()))
    #print("debug point 10")
    queries.sort(key=lambda query: query.start_time)
    #print("debug point 11")
    return Transaction(time_interval, database_name, username, pid, xid, queries)


def parse_copy_replacements(copy_replacements_filepath):
    copy_replacements = {}

    with open(copy_replacements_filepath) as csvfile:
        copy_replacements_reader = csv.reader(csvfile)
        next(copy_replacements_reader)  # Skip header
        for row in copy_replacements_reader:
            if len(row) == 3 and row[2]:
                copy_replacements[row[0]] = [row[1], row[2]]
    return copy_replacements


def parse_copy_replacements_s3(workload_directory):
    copy_replacements = {}
    copy_replacements_reader = None

    if workload_directory.startswith("s3://"):
        workload_s3_location = workload_directory[5:].partition("/")
        bucket_name = workload_s3_location[0]
        prefix = workload_s3_location[2]

        s3_object = client("s3").get_object(
            Bucket=bucket_name, Key=prefix + "/copy_replacements.csv"
        )

        csv_string = s3_object["Body"].read().decode("utf-8")
        copy_replacements_reader = csv.reader(csv_string.split('\r\n'))
        next(copy_replacements_reader)  # Skip header
        for row in copy_replacements_reader:
            if len(row) == 3 and row[2]:
                copy_replacements[row[0]] = [row[1], row[2]]
    else:
        with open(copy_replacements_filepath) as csvfile:
            copy_replacements_reader = csv.reader(csvfile)
            next(copy_replacements_reader)  # Skip header
            for row in copy_replacements_reader:
                if len(row) == 3 and row[2]:
                    #print(" current loc: ",row[0])
                    copy_replacements[row[0]] = [row[1], row[2]]

    return copy_replacements

def start_replay(connection_logs, default_interface, first_event_time, last_event_time):
    replay_time = first_event_time - datetime.timedelta(seconds=1)
    i=0
    active_connection_threads = {}
    connection_errors = {}
    transaction_errors = {}
    print(" total connection log :" , len(connection_logs))
    remaining_query_count=1 #start the while loop
    #while not len(active_connection_threads) == len(connection_logs):
    while not remaining_query_count == 0:
        remaining_query_count=0
        for connection_log in connection_logs:
            if (
                not connection_log.session_initiation_time
                or connection_log.session_initiation_time <= replay_time
            ) and connection_log.pid not in active_connection_threads:
                if not connection_log.disconnection_time:
                    connection_log.disconnection_time = last_event_time
                
                
                connection_thread = ConnectionThread(
                    connection_log,
                    default_interface,
                    connection_errors,
                    transaction_errors,
                    replay_time,
                )
                connection_thread.start()
                active_connection_threads[connection_log.pid] = connection_thread
                print(" current active conn: ",len(active_connection_threads))

            if connection_log.pid not in active_connection_threads:
                #print("in the else loop, un counted txns, pid: ", connection_log.pid)
        
                #j=0
                for transaction in connection_log.transactions:
                    for query in transaction.queries:
                        if query.text:
                            remaining_query_count=remaining_query_count+1
                #print(" number of queries in side this connection :", j, " pid is: ", connection_log.pid)
                #if j == 0 and connection_log.pid not in active_connection_threads:
                #    
                #    connection_thread = ConnectionThread(
                #        connection_log,
                #        default_interface,
                #        connection_errors,
                #        transaction_errors,
                #        replay_time,
                #    )
                #    connection_thread.start()
                #    active_connection_threads[connection_log.pid] = connection_thread


        #print("debug point 15")
        time.sleep(0.01)
        replay_time += datetime.timedelta(milliseconds=10)
    #print("debug point 14")
    
    for connection_log in connection_logs:
        if connection_log.pid not in active_connection_threads:
            connection_thread = ConnectionThread(
                    connection_log,
                    default_interface,
                    connection_errors,
                    transaction_errors,
                    replay_time,
            )
            connection_thread.start()
            active_connection_threads[connection_log.pid] = connection_thread
    
    
    
    for active_connection_log in active_connection_threads.values():
        active_connection_log.join()
    #print("debug point 13")
    return (connection_errors, transaction_errors)


def export_errors(
    connection_errors, transaction_errors, workload_location, replay_name
):
    connection_error_location = (
        workload_location + "/" + replay_name + "/connection_errors"
    )
    transaction_error_location = (
        workload_location + "/" + replay_name + "/transaction_errors"
    )
    logger.info(f"Exporting connection errors to {connection_error_location}")
    logger.info(f"Exporting transaction errors to {transaction_error_location}")

    if workload_location.startswith("s3://"):
        workload_s3_location = workload_location[5:].partition("/")
        bucket_name = workload_s3_location[0]
        prefix = workload_s3_location[2]
        s3_client = client("s3")
    else:
        os.makedirs(connection_error_location)
        os.makedirs(transaction_error_location)

    for filename, connection_error_text in connection_errors.items():
        if workload_location.startswith("s3://"):
            if prefix:
                key_loc = "%s/%s/connection_errors/%s.txt" % (prefix, replay_name,filename)
            else:
                key_loc = "%s/connection_errors/%s.txt" % (replay_name,filename)
            s3_client.put_object(
                Body=connection_error_text,
                Bucket=bucket_name,
                Key=key_loc,
            )
        else:
            error_file = open(connection_error_location + "/" + filename + ".txt", "w")
            error_file.write(connection_error_text)
            error_file.close()

    for filename, transaction_errors in transaction_errors.items():
        error_file_text = ""
        for transaction_error in transaction_errors:
            error_file_text += f"{transaction_error[0]}\n{transaction_error[1]}\n\n"

        if workload_location.startswith("s3://"):
            if prefix:
                key_loc = "%s/%s/transaction_errors/%s.txt" % (prefix, replay_name,filename)
            else:
                key_loc = "%s/transaction_errors/%s.txt" % (replay_name,filename)
            s3_client.put_object(
                Body=error_file_text,
                Bucket=bucket_name,
                Key=key_loc,
            )
        else:
            error_file = open(transaction_error_location + "/" + filename + ".txt", "w")
            error_file.write(error_file_text)
            error_file.close()


def assign_copy_replacements(connection_transactions, replacements):
    for connection_log in connection_logs:
        for transaction in connection_log.transactions:
            for query in transaction.queries:
                if "copy " in query.text.lower() and "from 's3:" in query.text.lower():
                    #from_text = re.search(r"FROM 's3:\/\/[^']*", query.text)
                    from_text = re.search(r"from 's3:\/\/[^']*", query.text, re.IGNORECASE)
                    if from_text:
                        #print("from txt ",from_text)
                        existing_copy_location = from_text.group()[6:]
                        #print("existing loc ", existing_copy_location)
                        #print("right before the error")
                        try:
                            replacement_exists = replacements[existing_copy_location][0]
                            matched_flag = 1
                        except KeyError as ke:
                            print('Key Not Found in csv Dictionary:', ke)
                            matched_flag = 0
                        
                        if matched_flag:
                            #print("replacing loc for ", existing_copy_location)
                            replacement_copy_location = replacements[existing_copy_location][0]
                            #print("replacement location", replacement_copy_location)
                            if not replacement_copy_location:
                                replacement_copy_location = existing_copy_location
                            replacement_copy_iam_role = replacements[existing_copy_location][1]

                            query.text = query.text.replace(
                                existing_copy_location, replacement_copy_location
                            )
                            query.text = re.sub(
                                r"credentials ''",
                                "IAM_ROLE '%s'" % (replacement_copy_iam_role),
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            query.text = re.sub(
                                r"with credentials as ''",
                                "IAM_ROLE '%s'" % (replacement_copy_iam_role),
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            query.text = re.sub(
                                r"IAM_ROLE ''",
                                "IAM_ROLE '%s'" % (replacement_copy_iam_role),
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            query.text = re.sub(
                                r"ACCESS_KEY_ID '' SECRET_ACCESS_KEY '' SESSION_TOKEN ''",
                                "IAM_ROLE '%s'" % (replacement_copy_iam_role),
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            query.text = re.sub(
                                r"ACCESS_KEY_ID '' SECRET_ACCESS_KEY ''",
                                "IAM_ROLE '%s'" % (replacement_copy_iam_role),
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            print(query.text," copy stmt")

def assign_unloads(connection_logs, replay_output, replay_name, unload_iam_role):
    for connection_log in connection_logs:
        for transaction in connection_log.transactions:
            for query in transaction.queries:
                #if "unload" in query.text.lower():
                if "unload" in query.text.lower() and "to 's3:" in query.text.lower():
                    #to_text = re.search(r"TO 's3:\/\/[^']*", query.text)
                    #to_text = re.search(r"from 's3:\/\/[^']*", query.text, re.IGNORECASE).group()[6:]
                    #print(" query txt ", query.text)
                    to_text = re.search(r"to 's3:\/\/[^']*", query.text, re.IGNORECASE).group()[9:]
                    #print(" to text", to_text)
                    #print(" replay output ", replay_output)

                    if to_text:
                        #existing_unload_location = to_text
                        existing_unload_location = re.search(r"to 's3:\/\/[^']*", query.text, re.IGNORECASE).group()[4:]
                        #print(" replay name ", replay_name)
                        #print(" existing unload location ",  existing_unload_location)
                        
                        replacement_unload_location = (
                            replay_output
                            + "/"
                            + replay_name
                            + "/UNLOADs/"
                            + to_text
                            ##+ existing_unload_location
                        )
                        #print(" replament unload stmt ",   replacement_unload_location)
                        new_query_text = query.text.replace(
                            existing_unload_location, replacement_unload_location
                        )
                        if not new_query_text == query.text:
                            query.text = new_query_text
                            query.text = re.sub(
                                r"credentials ''",
                                "IAM_ROLE '%s'" % (unload_iam_role),
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            query.text = re.sub(
                                r"with credentials as ''",
                                "IAM_ROLE '%s'" % (unload_iam_role),
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            query.text = re.sub(
                                r"IAM_ROLE ''",
                                "IAM_ROLE '%s'" % (unload_iam_role),
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            query.text = re.sub(
                                r"ACCESS_KEY_ID '' SECRET_ACCESS_KEY '' SESSION_TOKEN ''",
                                "IAM_ROLE '%s'" % (unload_iam_role),
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            query.text = re.sub(
                                r"ACCESS_KEY_ID '' SECRET_ACCESS_KEY ''",
                                "IAM_ROLE '%s'" % (unload_iam_role),
                                query.text,
                                flags=re.IGNORECASE,
                            )

                            print(" modified unload ", query.text)
def assign_time_intervals(connection_logs):
    for connection_log in connection_logs:
        for transaction in connection_log.transactions:
            if connection_log.time_interval_between_queries == "all on":
                is_calculate_time_interval = True
            elif connection_log.time_interval_between_queries == "all off":
                is_calculate_time_interval = False
            else:
                is_calculate_time_interval = transaction.time_interval.lower() == "true"
            if is_calculate_time_interval:
                for index, sql in enumerate(transaction.queries[1:]):
                    prev_sql = transaction.queries[
                        index
                    ]  # It is not supposed to be index - 1 due to how we're enumerating transaction.queries
                    prev_sql.time_interval = (
                        sql.start_time - prev_sql.end_time
                    ).total_seconds()


def assign_create_user_password(connection_logs):
    for connection_log in connection_logs:
        for transaction in connection_log.transactions:
            for query in transaction.queries:
                if "create user" in query.text.lower():
                    random_password = "".join(
                        random.choices(
                            string.ascii_uppercase
                            + string.ascii_lowercase
                            + string.digits,
                            k=61,
                        )
                    )
                    query.text = re.sub(
                        r"PASSWORD '\*\*\*'",
                        f"PASSWORD '{random_password}aA0'",
                        query.text,
                        flags=re.IGNORECASE,
                    )


def get_connection_string(cluster_endpoint, username, odbc_driver):
    # Constantly refresh connection string since credentials are temporary
    #print("debug point 1")
    global refresh_target_cluster_urls_thread
    #print("debug point 2")
    refresh_target_cluster_urls_thread = threading.Timer(
        600.0, get_connection_string, [cluster_endpoint, username, odbc_driver]
    )
    #print("debug point 3")
    refresh_target_cluster_urls_thread.start()

    #print("debug point 4")
    
    cluster_endpoint_split = cluster_endpoint.split(".")
    cluster_id = cluster_endpoint_split[0]
    cluster_host = cluster_endpoint.split(":")[0]
    cluster_port = cluster_endpoint_split[5].split("/")[0][4:]
    cluster_database = cluster_endpoint_split[5].split("/")[1]

    #print("debug point 5")
    try:
        response = client("redshift").get_cluster_credentials(
            DbUser=username, ClusterIdentifier=cluster_id, AutoCreate=False,
        )
        #print("debug point 6")
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

        #print("debug point 7")
        cluster_psql = {
            "username": response["DbUser"],
            "password": response["DbPassword"],
            "host": cluster_host,
            "port": cluster_port,
            "database": cluster_database,
        }

        #print("debug point 8")
        global target_cluster_urls
        target_cluster_urls = {"odbc": cluster_odbc_url, "psql": cluster_psql}
        return target_cluster_urls
    except Exception as err:
        logger.error(f"ERROR: Failed to generate connection string. {err}")
        return {}


def unload_system_table(
    default_interface,
    unload_system_table_queries_file,
    unload_location,
    unload_iam_role,
):
    conn = None
    if default_interface == "odbc":
        conn = pyodbc.connect(target_cluster_urls["odbc"])
    else:
        conn = pg8000.connect(
            user=target_cluster_urls["psql"]["username"],
            password=target_cluster_urls["psql"]["password"],
            host=target_cluster_urls["psql"]["host"],
            port=target_cluster_urls["psql"]["port"],
            database=target_cluster_urls["psql"]["database"],
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
    #print(len(config_file["target_cluster_endpoint"].split(".")))
    #print(len(config_file["target_cluster_endpoint"].split(":")))
    #print(len(config_file["target_cluster_endpoint"].split("/")))
    if (
        not len(config_file["target_cluster_endpoint"].split(".")) == 6
        or not len(config_file["target_cluster_endpoint"].split(":")) == 2
        or not len(config_file["target_cluster_endpoint"].split("/")) == 2
        or not ".redshift.amazonaws.com:" in config_file["target_cluster_endpoint"]
    ):
        logger.error(
            'Config file value for "target_cluster_endpoint" is not a valid endpoint. Endpoints must be in the format of <cluster-name>.<identifier>.<region>.redshift.amazonaws.com:<port>/<database-name>.'
        )
        exit(EXIT_MISSING_VALUE_CONFIG_FILE)
    if not config_file["master_username"]:
        logger.error(
            'Config file missing value for "master_username". Please provide a value for "master_username".'
        )
        exit(EXIT_MISSING_VALUE_CONFIG_FILE)
    if not config_file["odbc_driver"]:
        logger.error(
            'Config file missing value for "odbc_driver". Therefore, replay will not use ODBC, and will use psql instead. Please provide a value for "odbc_driver" if playback using ODBC instead of psql is necessary.'
        )
    if not (
        config_file["default_interface"] == "psql"
        or config_file["default_interface"] == "odbc"
    ):
        logger.error(
            'Config file value for "default_interface" must be either "psql" or "odbc". Please change the value for "default_interface" to either "psql" or "odbc".'
        )
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    if not (
        config_file["time_interval_between_transactions"] == ""
        or config_file["time_interval_between_transactions"] == "all on"
        or config_file["time_interval_between_transactions"] == "all off"
    ):
        logger.error(
            'Config file value for "time_interval_between_transactions" must be either "", "all on", or "all off". Please change the value for "time_interval_between_transactions" to be "", "all on", or "all off".'
        )
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    if not (
        config_file["time_interval_between_queries"] == ""
        or config_file["time_interval_between_queries"] == "all on"
        or config_file["time_interval_between_queries"] == "all off"
    ):
        logger.error(
            'Config file value for "time_interval_between_queries" must be either "", "all on", or "all off". Please change the value for "time_interval_between_queries" to be "", "all on", or "all off".'
        )
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    if not (
        config_file["execute_copy_statements"] == "true"
        or config_file["execute_copy_statements"] == "false"
    ):
        logger.error(
            'Config file value for "execute_copy_statements" must be either "true" or "false". Please change the value for "execute_copy_statements" to either "true" or "false".'
        )
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    if not (
        config_file["execute_unload_statements"] == "true"
        or config_file["execute_unload_statements"] == "false"
    ):
        logger.error(
            'Config file value for "execute_unload_statements" must be either "true" or "false". Please change the value for "execute_unload_statements" to either "true" or "false".'
        )
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    if config_file["replay_output"] and not config_file["replay_output"].startswith(
        "s3://"
    ):
        logger.error(
            'Config file value for "replay_output" must be an S3 location (starts with "s3://"). Please remove this value or put in an S3 location.'
        )
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    if (
        config_file["replay_output"]
        and config_file["target_cluster_system_table_unload_iam_role"]
        and not config_file["unload_system_table_queries"]
    ):
        logger.error(
            'Config file missing value for "unload_system_table_queries". Please provide a value for "unload_system_table_queries", or remove the value for "target_cluster_system_table_unload_iam_role".'
        )
        exit(EXIT_MISSING_VALUE_CONFIG_FILE)
    if (
        config_file["replay_output"]
        and config_file["target_cluster_system_table_unload_iam_role"]
        and config_file["unload_system_table_queries"]
        and not config_file["unload_system_table_queries"].endswith(".sql")
    ):
        logger.error(
            'Config file value for "unload_system_table_queries" does not end with ".sql". Please ensure the value for "unload_system_table_queries" ends in ".sql". See the provided "unload_system_tables.sql" as an example.'
        )
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    if not config_file["workload_location"]:
        logger.error(
            'Config file missing value for "workload_location". Please provide a value for "workload_location".'
        )
        exit(EXIT_MISSING_VALUE_CONFIG_FILE)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "config_file",
        type=argparse.FileType("r"),
        help="Location of replay config file.",
    )
    args = parser.parse_args()

    config_file = {}
    with args.config_file as stream:
        try:
            config_file = yaml.safe_load(stream)
        except yaml.YAMLError as exception:
            logger.error(exception)

    validate_config_file(config_file)

    replay_name = f'Replay_{config_file["target_cluster_endpoint"].split(".")[0]}_{datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat()}'
    print(replay_name)
    # Stores connection string in global target_cluster_urls. Refreshes periodically to since credentials retrieved from get_cluster_credentials are temporary and expire.
    get_connection_string(
        config_file["target_cluster_endpoint"],
        config_file["master_username"],
        config_file["odbc_driver"],
    )
    print("after connection")
    if not "odbc" in target_cluster_urls:
        print("why I am here?")
        logger.error("Replay failed. Invalid target cluster url.")
        refresh_target_cluster_urls_thread.cancel()
        print("after cancel")
        exit(EXIT_INVALID_VALUE_CONFIG_FILE)
    print("after odbc")
    connection_logs = parse_connections(
        config_file["workload_location"],
        config_file["time_interval_between_transactions"],
        config_file["time_interval_between_queries"],
    )
    connection_logs.sort(
        key=lambda connection: connection.session_initiation_time
        or datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc)
    )
    logger.info(f"Found {len(connection_logs)} connections.")

    first_event_time = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc)
    last_event_time = datetime.datetime.utcfromtimestamp(0).replace(
        tzinfo=datetime.timezone.utc
    )
    transaction_count = 0
    query_count = 0

    # Associate transactions with connections
    logger.info(
        f"Finding transactions in {config_file['workload_location']}, this might take some time."
    )
    connection_transactions = parse_transactions(config_file["workload_location"])
    #print("first_event_time ",first_event_time)
    for connection in connection_logs:
        if (
            connection.session_initiation_time
            and connection.session_initiation_time < first_event_time
        ):
            first_event_time = connection.session_initiation_time
        if (
            connection.disconnection_time
            and connection.disconnection_time > last_event_time
        ):
            last_event_time = connection.disconnection_time

        if connection.pid in connection_transactions:
            connection.transactions = connection_transactions[connection.pid]

            transaction_count += len(connection.transactions)
            for transaction in connection.transactions:
                query_count += len(transaction.queries)
            #print("debug point 9")
            #print("first_event_time ",first_event_time)
            ## Raj changed to add a condition to handle connection.transactions[0].queries[0].start_time is null
            #print("q start time",connection.transactions[0].queries[0].start_time)
            if connection.transactions[0].queries[0].start_time and connection.transactions[0].queries[0].start_time < first_event_time:
                first_event_time = connection.transactions[0].queries[0].start_time
            if connection.transactions[-1].queries[-1].end_time and connection.transactions[-1].queries[-1].end_time > last_event_time:
                last_event_time = connection.transactions[-1].queries[-1].end_time
            #print("debug point 10")
    logger.info(f"Found {transaction_count} transactions ({query_count} queries)")
    logger.info(
        "Estimated original workload execution time: "
        + str((last_event_time - first_event_time))
    )

    if config_file["execute_copy_statements"] == "true":
        logger.debug("Configuring COPY replacements")
        copy_replacements_filepath = (
            config_file["workload_location"] + "/copy_replacements.csv"
        )
        replacements = parse_copy_replacements_s3(config_file["workload_location"])
        logger.debug(
            f"Making {len(replacements)} COPY replacements with {copy_replacements_filepath}"
        )
        assign_copy_replacements(connection_logs, replacements)

    if config_file["execute_unload_statements"] == "true":
        if config_file["unload_iam_role"]:
            if config_file["replay_output"].startswith("s3://"):
                logger.debug("Configuring UNLOADs")
                assign_unloads(
                    connection_logs,
                    config_file["replay_output"],
                    replay_name,
                    config_file["unload_iam_role"],
                )
            else:
                logger.debug(
                    'UNLOADs not configured since "replay_output" is not an S3 location.'
                )

    logger.debug("Configuring time intervals")
    assign_time_intervals(connection_logs)

    logger.debug("Configuring CREATE USER PASSWORD random replacements")
    assign_create_user_password(connection_logs)

    replay_start_time = datetime.datetime.now()
    # Actual replay
    logger.info("Replaying...")
    (connection_errors, transaction_errors) = start_replay(
        connection_logs,
        config_file["default_interface"],
        first_event_time,
        last_event_time,
    )
    logger.info("Replay summary:")
    logger.info(
        f"Attempted to replay {transaction_count} transactions ({query_count} total queries) with {len(connection_logs)} connections."
    )
    logger.info(
        f"Successfully replayed {transactions_executed} out of {transaction_count} ({round((transactions_executed/transaction_count)*100)}%) transactions."
    )
    logger.info(
        f"Successfully replayed {queries_executed} out of {query_count} ({round((queries_executed/query_count)*100)}%) queries."
    )
    if config_file["replay_output"]:
        error_location = config_file["replay_output"]
    else:
        error_location = config_file["workload_location"]
    export_errors(
        connection_errors,
        transaction_errors,
        error_location,
        replay_name,
    )

    
    logger.info(f"Replay finished in {datetime.datetime.now() - replay_start_time}.")

    if (
        config_file["replay_output"]
        and config_file["unload_system_table_queries"]
        and config_file["target_cluster_system_table_unload_iam_role"]
    ):
        logger.info(f'Exporting system tables to {config_file["replay_output"]}')

        unload_system_table(
            config_file["default_interface"],
            config_file["unload_system_table_queries"],
            config_file["replay_output"] + "/" + replay_name,
            config_file["target_cluster_system_table_unload_iam_role"],
        )

        logger.info(f'Exported system tables to {config_file["replay_output"]}')
    refresh_target_cluster_urls_thread.cancel()
