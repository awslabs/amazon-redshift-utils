import argparse
import copy
import csv
import datetime
import hashlib
import json
import logging
import multiprocessing
import os
import random
import re
import signal

import boto3
import sqlparse
import string
import sys
import threading
import time
import traceback
import yaml

from boto3 import client, resource
from botocore.exceptions import NoCredentialsError
from collections import OrderedDict
from contextlib import contextmanager
from multiprocessing.managers import SyncManager
from pathlib import Path
from queue import Empty, Full
from urllib.parse import urlparse

from util import init_logging, set_log_level, prepend_ids_to_logs, add_logfile, log_version, db_connect, cluster_dict, \
    load_config, load_file, retrieve_compressed_json, get_secret, parse_error, bucket_dict
from replay_analysis import run_replay_analysis

import redshift_connector
import dateutil.parser

logger = None

g_total_connections = 0
g_queries_executed = 0

# map username to credential strings and timestamp
g_credentials_cache = {}

g_workers = []
g_exit = False

g_copy_replacements_filename = 'copy_replacements.csv'

g_config = {}

g_replay_timestamp = None

g_is_serverless = False
g_serverless_cluster_endpoint_pattern = r"(.+)\.(.+)\.(.+).redshift-serverless(-dev)?\.amazonaws\.com:[0-9]{4}\/(.)+"
g_cluster_endpoint_pattern = r"(.+)\.(.+)\.(.+).redshift(-serverless)?\.amazonaws\.com:[0-9]{4}\/(.)+"

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
            connection_key,
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
        self.connection_key = connection_key
        self.transactions = []

    def __str__(self):
        return (
                "Session initiation time: %s, Disconnection time: %s, Application name: %s, Database name: %s, "
                "Username; %s, PID: %s, Time interval between transactions: %s, Time interval between queries: %s, "
                "Number of transactions: %s"
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

    def offset_ms(self, ref_time):
        return (self.session_initiation_time - ref_time).total_seconds() * 1000.0

    @staticmethod
    def supported_filters():
        return {'database_name', 'username', 'pid'}


class Transaction:
    def __init__(self, time_interval, database_name, username, pid, xid, queries, transaction_key):
        self.time_interval = time_interval
        self.database_name = database_name
        self.username = username
        self.pid = pid
        self.xid = xid
        self.queries = queries
        self.transaction_key = transaction_key

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

    def start_time(self):
        return self.queries[0].start_time

    def end_time(self):
        return self.queries[-1].end_time

    def offset_ms(self, replay_start_time):
        return self.queries[0].offset_ms(replay_start_time)

    @staticmethod
    def supported_filters():
        return {'database_name', 'username', 'pid'}


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

    def offset_ms(self, ref_time):
        return (self.start_time - ref_time).total_seconds() * 1000.0


class ConnectionThread(threading.Thread):
    def __init__(
        self,
        process_idx,
        job_id,
        connection_log,
        default_interface,
        odbc_driver,
        replay_start,
        first_event_time,
        error_logger,
        thread_stats,
        num_connections,
        peak_connections,
        connection_semaphore,
        perf_lock
    ):
        threading.Thread.__init__(self)
        self.process_idx = process_idx
        self.job_id = job_id
        self.connection_log = connection_log
        self.default_interface = default_interface
        self.odbc_driver = odbc_driver
        self.replay_start = replay_start
        self.first_event_time = first_event_time
        self.error_logger = error_logger
        self.thread_stats = thread_stats
        self.num_connections = num_connections
        self.peak_connections = peak_connections
        self.connection_semaphore = connection_semaphore
        self.perf_lock = perf_lock

        prepend_ids_to_logs(self.process_idx, self.job_id + 1)

    @contextmanager
    def initiate_connection(self, username):
        conn = None

        # check if this connection is happening at the right time
        expected_elapsed_sec = (self.connection_log.session_initiation_time - self.first_event_time).total_seconds()
        elapsed_sec = (datetime.datetime.now(tz=datetime.timezone.utc) - self.replay_start).total_seconds()
        connection_diff_sec = elapsed_sec - expected_elapsed_sec
        connection_duration_sec = (self.connection_log.disconnection_time -
                                   self.connection_log.session_initiation_time).total_seconds()

        logger.debug(f"Establishing connection {self.job_id + 1} of {g_total_connections} at {elapsed_sec:.3f} "
                     f"(expected: {expected_elapsed_sec:.3f}, {connection_diff_sec:+.3f}).  "
                     f"Pid: {self.connection_log.pid}, Connection times: {self.connection_log.session_initiation_time} "
                     f"to {self.connection_log.disconnection_time}, {connection_duration_sec} sec")

        # save the connection difference
        self.thread_stats['connection_diff_sec'] = connection_diff_sec

        # and emit a warning if we're behind
        if abs(connection_diff_sec) > g_config.get('connection_tolerance_sec', 300):
            logger.warning("Connection at {} offset by {:+.3f} sec".format(self.connection_log.session_initiation_time,
                                                                           connection_diff_sec))

        interface = self.default_interface

        if "psql" in self.connection_log.application_name.lower():
            interface = "psql"
        elif "odbc" in self.connection_log.application_name.lower() and self.odbc_driver is not None:
            interface = "odbc"
        elif self.default_interface == "odbc" and self.odbc_driver is None:
            logger.warning("Default driver is set to ODBC. But no ODBC DSN provided. Replay will use PSQL as "
                           "default driver.")
            interface = "psql"
        else:
            interface = "psql"

        credentials = get_connection_credentials(username, database=self.connection_log.database_name)

        try:
            try:
                conn = db_connect(interface,
                                  host=credentials['host'],
                                  port=int(credentials['port']),
                                  username=credentials['username'],
                                  password=credentials['password'],
                                  database=credentials['database'],
                                  odbc_driver=credentials['odbc_driver'],
                                  drop_return=g_config.get('drop_return'))
                logger.debug(f"Connected using {interface} for PID: {self.connection_log.pid}")
                self.num_connections.value += 1
            except Exception as err:
                hashed_cluster_url = copy.deepcopy(credentials)
                hashed_cluster_url["password"] = "***"
                logger.error(
                    f'({self.job_id + 1}) Failed to initiate connection for {self.connection_log.database_name}-'
                    f'{self.connection_log.username}-{self.connection_log.pid} ({hashed_cluster_url}): {err}')
                self.thread_stats['connection_error_log'][
                    f"{self.connection_log.database_name}-{self.connection_log.username}-{self.connection_log.pid}"] = f"{self.connection_log}\n\n{err}"
            yield conn
        except Exception as e:
            logger.error(f"Exception in connect: {e}")
        finally:
            logger.debug(f"Context closing for pid: {self.connection_log.pid}")
            if conn is not None:
                conn.close()
                logger.debug(f"Disconnected for PID: {self.connection_log.pid}")
            self.num_connections.value -= 1
            if self.connection_semaphore is not None:
                logger.debug(f"Releasing semaphore ({self.num_connections.value} / "
                             f"{g_config['limit_concurrent_connections']} active connections)")
                self.connection_semaphore.release()

    def run(self):
        try:
            with self.initiate_connection(self.connection_log.username) as connection:
                if connection:
                    self.execute_transactions(connection)
                    if self.connection_log.time_interval_between_transactions is True:
                        disconnect_offset_sec = (self.connection_log.disconnection_time -
                                                 self.first_event_time).total_seconds()
                        if disconnect_offset_sec > current_offset_ms(self.replay_start):
                            logger.debug(f"Waiting to disconnect {time_until_disconnect_sec} sec (pid "
                                         f"{self.connection_log.pid})")
                            time.sleep(time_until_disconnect_sec)
                else:
                    logger.warning("Failed to connect")
        except Exception as e:
            logger.error(f"Exception thrown for pid {self.connection_log.pid}: {e}")

    def execute_transactions(self, connection):
        if self.connection_log.time_interval_between_transactions is True:
            for idx, transaction in enumerate(self.connection_log.transactions):
                # we can use this if we want to run transactions based on their offset from the start of the replay
                # time_until_start_ms = transaction.offset_ms(self.first_event_time) -
                # current_offset_ms(self.replay_start)

                # or use this to preserve the time between transactions
                if idx == 0:
                    time_until_start_ms = (transaction.start_time() -
                                           self.connection_log.session_initiation_time).total_seconds() * 1000.0
                else:
                    prev_transaction = self.connection_log.transactions[idx - 1]
                    time_until_start_ms = (transaction.start_time() -
                                           prev_transaction.end_time()).total_seconds() * 1000.0

                # wait for the transaction to start
                if time_until_start_ms > 10:
                    logger.debug(f"Waiting {time_until_start_ms / 1000:.1f} sec for transaction to start")
                    time.sleep(time_until_start_ms / 1000.0)
                self.execute_transaction(transaction, connection)
        else:
            for transaction in self.connection_log.transactions:
                self.execute_transaction(transaction, connection)


    def save_query_stats(self, starttime, endtime, xid, query_idx):
        with self.perf_lock:
            sr_dir = g_config.get("logging_dir", "simplereplay_logs") + '/' + g_replay_timestamp.isoformat()
            Path(sr_dir).mkdir(parents=True, exist_ok=True)
            filename = f"{sr_dir}/{self.process_idx}_times.csv"
            elapsed_sec = 0
            if endtime is not None:
                elapsed_sec = "{:.6f}".format((endtime - starttime).total_seconds())
            with open(filename, "a+") as fp:
                if fp.tell() == 0:
                    fp.write("# process,query,start_time,end_time,elapsed_sec,rows\n")
                query_id = f"{xid}-{query_idx}"
                fp.write("{},{},{},{},{},{}\n".format(self.process_idx, query_id, starttime, endtime, elapsed_sec, 0))


    def get_tagged_sql(self, query_text, idx, transaction, connection):
        json_tags = {"xid": transaction.xid,
                     "query_idx": idx,
                     "replay_start": g_replay_timestamp.isoformat()}
        return "/* {} */ {}".format(json.dumps(json_tags), query_text)

    def execute_transaction(self, transaction, connection):
        errors = []
        cursor = connection.cursor()

        transaction_query_idx = 0
        for idx, query in enumerate(transaction.queries):
            time_until_start_ms = query.offset_ms(self.first_event_time) - current_offset_ms(self.replay_start)
            truncated_query = (query.text[:60] + '...' if len(query.text) > 60 else query.text).replace("\n", " ")
            logger.debug(f"Executing [{truncated_query}] in {time_until_start_ms/1000.0:.1f} sec")

            if time_until_start_ms > 10:
                time.sleep(time_until_start_ms / 1000.0)

            if g_config.get("split_multi", True):
                split_statements = sqlparse.split(query.text)
                # exclude empty statements. Some customers' queries have been
                # found to end in multiple ; characters;
                split_statements = [_ for _ in split_statements if _ != ';']
            else:
                split_statements = [query.text]

            if len(split_statements) > 1:
                self.thread_stats['multi_statements'] += 1
            self.thread_stats['executed_queries'] += len(split_statements)

            success = True
            for s_idx, sql_text in enumerate(split_statements):
                sql_text = self.get_tagged_sql(sql_text, transaction_query_idx, transaction, connection)
                transaction_query_idx += 1

                substatement_txt = ""
                if len(split_statements) > 1:
                    substatement_txt = f", Multistatement: {s_idx+1}/{len(split_statements)}"

                exec_start = datetime.datetime.now(tz=datetime.timezone.utc)
                exec_end = None
                try:
                    status = ''
                    if (g_config["execute_copy_statements"] == "true" and "from 's3:" in sql_text.lower()):
                        cursor.execute(sql_text)
                    elif (g_config["execute_unload_statements"] == "true" and "to 's3:" in sql_text.lower() and g_config["replay_output"] is not None):
                        cursor.execute(sql_text)
                    elif ("from 's3:" not in sql_text.lower()) and ("to 's3:" not in sql_text.lower()): ## removed condition to exclude bind variables
                        cursor.execute(sql_text)
                    else:
                        status = 'Not '
                    exec_end = datetime.datetime.now(tz=datetime.timezone.utc)
                    exec_sec = (exec_end - exec_start).total_seconds()

                    logger.debug(
                        f"{status}Replayed DB={transaction.database_name}, USER={transaction.username}, PID={transaction.pid}, XID:{transaction.xid}, Query: {idx+1}/{len(transaction.queries)}{substatement_txt} ({exec_sec} sec)"
                    )
                    success = success & True
                except Exception as err:
                    success = False
                    errors.append([sql_text, str(err)])
                    logger.debug(
                        f"Failed DB={transaction.database_name}, USER={transaction.username}, PID={transaction.pid}, "
                        f"XID:{transaction.xid}, Query: {idx + 1}/{len(transaction.queries)}{substatement_txt}: {err}"
                    )
                    self.error_logger.append(parse_error(err,
                                                         transaction.username,
                                                         g_config["target_cluster_endpoint"].split('/')[-1],
                                                         query.text))

                self.save_query_stats(exec_start, exec_end, transaction.xid, transaction_query_idx)
            if success:
                self.thread_stats['query_success'] += 1
            else:
                self.thread_stats['query_error'] += 1

            if query.time_interval > 0.0:
                logger.debug(f"Waiting {query.time_interval} sec between queries")
                time.sleep(query.time_interval)

        cursor.close()
        connection.commit()

        if self.thread_stats['query_error'] == 0:
            self.thread_stats['transaction_success'] += 1
        else:
            self.thread_stats['transaction_error'] += 1
            self.thread_stats['transaction_error_log'][transaction.get_base_filename()] = errors


# exception thrown if any filters are invalid
class InvalidFilterException(Exception):
    pass


# exception thrown if credentials can't be retrieved
class CredentialsException(Exception):
    pass


# exception thrown if cluster doesn't exist
class ClusterNotExist(Exception):
    pass


def validate_and_normalize_filters(object, filters):
    """ validate filters and set defaults. The object just needs to
        provide a supported_filters() function. """

    normalized_filters = copy.deepcopy(filters)

    if 'include' not in normalized_filters:
        normalized_filters['include'] = {}
    if 'exclude' not in normalized_filters:
        normalized_filters['exclude'] = {}

    for f in object.supported_filters(): 
        normalized_filters['include'].setdefault(f, ['*'])
        normalized_filters['exclude'].setdefault(f, [])

    include_overlap = set(normalized_filters['include'].keys()) - set(object.supported_filters())
    if len(include_overlap) > 0:
        raise InvalidFilterException(f"Unknown filters: {include_overlap}")

    exclude_overlap = set(normalized_filters['exclude'].keys()) - set(object.supported_filters())
    if len(exclude_overlap) > 0:
        raise InvalidFilterException(f"Unknown filters: {exclude_overlap}")

    for f in object.supported_filters():
        include = normalized_filters['include'][f]
        exclude = normalized_filters['exclude'][f]

        if len(include) == 0:
            raise InvalidFilterException("Include filter must not be empty")

        overlap = set(include).intersection(set(exclude))
        if len(overlap) > 0:
            raise InvalidFilterException(f"Can't include the same values in both include and exclude for filter: "
                                         f"{overlap}")

        for x in (include, exclude):
            if len(x) > 1 and '*' in x:
                raise InvalidFilterException("'*' can not be used with other filter values filter")

    return normalized_filters


def matches_filters(object, filters):
    """ Check if the object matches the filters.  The object just needs to
        provide a supported_filters() function.  This also assumes filters has already
        been validated """

    included = 0
    for field in object.supported_filters():
        include = filters['include'][field]
        exclude = filters['exclude'][field]

        # if include values were passed and its not a wildcard, check against it
        if '*' in include or getattr(object, field) in include:
            included += 1
        # if include = * and the user is in the exclude list, reject
        if getattr(object, field) in exclude:
            return False

        # otherwise include = * and there's no exclude, so continue checking

    if included == len(object.supported_filters()):
        return True
    else:
        return False


def current_offset_ms(ref_time):
    return (datetime.datetime.now(tz=datetime.timezone.utc) - ref_time).total_seconds() * 1000.0


def parse_connections(workload_directory, time_interval_between_transactions, time_interval_between_queries):
    connections = []

    # total number of connections before filters are applied
    total_connections = 0

    if workload_directory.startswith("s3://"):
        workload_s3_location = workload_directory[5:].partition("/")
        bucket_name = workload_s3_location[0]
        prefix = workload_s3_location[2]
        

        s3_object = client("s3").get_object(
            Bucket=bucket_name, Key=prefix.rstrip('/') + "/connections.json"
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
            connection_key = f'{connection_json["database_name"]}_{connection_json["username"]}_' \
                             f'{connection_json["pid"]}'
            connection = ConnectionLog(
                session_initiation_time,
                disconnection_time,
                connection_json["application_name"],
                connection_json["database_name"],
                connection_json["username"],
                connection_json["pid"],
                is_time_interval_between_transactions,
                is_time_interval_between_queries,
                connection_key,
            )
            if matches_filters(connection, g_config['filters']):
                connections.append(connection)
            total_connections += 1
        except Exception as err:
            logger.error(f"Could not parse connection: \n{str(connection_json)}\n{err}")

    connections.sort(
        key=lambda connection: connection.session_initiation_time
                               or datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc)
    )

    return connections, total_connections


def parse_transactions(workload_directory):
    transactions = []
    gz_path = workload_directory.rstrip("/") + "/SQLs.json.gz"

    sql_json = retrieve_compressed_json(gz_path)
    for xid, transaction_dict in sql_json['transactions'].items():
        transaction = parse_transaction(transaction_dict)
        if transaction.start_time() and matches_filters(transaction, g_config['filters']):
            transactions.append(transaction)

    transactions.sort(
        key=lambda transaction: (transaction.start_time(), transaction.xid)
    )

    return transactions

def parse_transactions_old(workload_directory):
    transactions = []

    if workload_directory.startswith("s3://"):
        workload_s3_location = workload_directory[5:].partition("/")
        bucket_name = workload_s3_location[0]
        prefix = workload_s3_location[2]

        conn = client("s3")
        s3 = resource("s3")
        paginator = conn.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix.rstrip('/') + "/SQLs/")
        for page in page_iterator:
            for log in page.get('Contents'):
                filename = log["Key"].split("/")[-1]
                if filename.endswith(".sql"):
                    sql_file_text = (
                        s3.Object(bucket_name, log["Key"])
                            .get()["Body"]
                            .read()
                            .decode("utf-8")
                    )
                    transaction = parse_transaction(filename, sql_file_text)
                    if transaction.start_time() and matches_filters(transaction, g_config['filters']):
                        transactions.append(transaction)
    else:
        sqls_directory = os.listdir(workload_directory + "/SQLs/")
        for sql_filename in sqls_directory:
            if sql_filename.endswith(".sql"):
                sql_file_text = open(
                    workload_directory + "/SQLs/" + sql_filename, "r"
                ).read()
                transaction = parse_transaction(sql_filename, sql_file_text)
                if transaction.start_time() and matches_filters(transaction, g_config['filters']):
                    transactions.append(transaction)

    transactions.sort(
        key=lambda transaction: (transaction.start_time(), transaction.xid)
    )

    return transactions


def get_connection_key(database_name, username, pid):
    return f"{database_name}_{username}_{pid}"


def parse_transaction(transaction_dict):
    queries = []

    for q in transaction_dict['queries']:
        start_time = dateutil.parser.isoparse(q['record_time'])
        if q['start_time'] is not None:
            start_time = dateutil.parser.isoparse(q['start_time'])
        end_time = dateutil.parser.isoparse(q['record_time'])
        if q['end_time'] is not None:
            end_time = dateutil.parser.isoparse(q['end_time'])
        queries.append(Query(start_time, end_time, q['text']))

    queries.sort(key=lambda query: query.start_time)
    transaction_key = get_connection_key(transaction_dict['db'], transaction_dict['user'], transaction_dict['pid'])
    return Transaction(transaction_dict['time_interval'], transaction_dict['db'], transaction_dict['user'],
                       transaction_dict['pid'], transaction_dict['xid'], queries, transaction_key)


def parse_transaction_old(sql_filename, sql_file_text):
    queries = []
    time_interval = True

    database_name = None
    username = None
    pid = None
    xid = None
    query_start_time = ""
    query_end_time = ""
    query_text = ""
    for line in sql_file_text.splitlines():
        if line.startswith("--Time interval"):
            time_interval = line.split("--Time interval: ")[1].strip()
        elif line.startswith("--Record time"):
            if query_text.strip():
                query = Query(query_start_time, query_end_time, query_text.strip())
                queries.append(query)
                query_text = ""

            query_start_time = dateutil.parser.isoparse(line.split(": ")[1].strip())
            query_end_time = query_start_time
        elif line.startswith("--Start time"):
            query_start_time = dateutil.parser.isoparse(line.split(": ")[1].strip())
        elif line.startswith("--End time"):
            query_end_time = dateutil.parser.isoparse(line.split(": ")[1].strip())
        elif line.startswith("--Database"):
            database_name = line.split(": ")[1].strip()
        elif line.startswith("--Username"):
            username = line.split(": ")[1].strip()
        elif line.startswith("--Pid"):
            pid = line.split(": ")[1].strip()
        elif line.startswith("--Xid"):
            xid = line.split(": ")[1].strip()
        # remove all other comments
        elif not line.startswith("--"):
            query_text += " " + line

    # fallback to using the filename to retrieve the query details. This should only happen if
    # replay is run over an old extraction.
    if not all([database_name, username, pid, xid]):
        database_name, username, pid, xid = parse_filename(sql_filename)
        if not all([database_name, username, pid, xid]):
            logger.error(f"Failed to parse filename {sql_filename}")

    queries.append(Query(query_start_time, query_end_time, query_text.strip()))

    queries.sort(key=lambda query: query.start_time)

    transaction_key = get_connection_key(database_name, username, pid)
    return Transaction(time_interval, database_name, username, pid, xid, queries, transaction_key)


def parse_filename(filename):
    # Try to parse the info from the filename. Filename format is:
    #  {database_name}-{username}-{pid}-{xid}
    # Both database_name and username can contain "-" characters.  In that case, we'll
    # take a guess that the - is in the username rather than the database name.
    match = re.search(r"^([^-]+)-(.+)-(\d+)-(\d+)", filename)
    if not match:
        return None, None, None, None
    return match.groups()


def parse_copy_replacements(workload_directory):
    copy_replacements = {}
    copy_replacements_reader = None

    replacements_path = workload_directory + '/' + g_copy_replacements_filename

    if replacements_path.startswith("s3://"):
        workload_s3_location = replacements_path[5:].partition("/")
        bucket_name = workload_s3_location[0]
        prefix = workload_s3_location[2]

        s3_object = client("s3").get_object(
            Bucket=bucket_name, Key=prefix
        )

        csv_string = s3_object["Body"].read().decode("utf-8")
        copy_replacements_reader = csv.reader(csv_string.splitlines())
        next(copy_replacements_reader)  # Skip header
        for row in copy_replacements_reader:
            if len(row) == 3 and row[2]:
                copy_replacements[row[0]] = [row[1], row[2]]
    else:
        with open(replacements_path, 'r') as csvfile:
            copy_replacements_reader = csv.reader(csvfile)
            next(copy_replacements_reader)  # Skip header
            for idx, row in enumerate(copy_replacements_reader):
                if len(row) != 3:
                    logger.error(f"Replacements file {replacements_path} is malformed (row {idx}, line:\n{row}")
                    sys.exit()
                copy_replacements[row[0]] = [row[1], row[2]]

    logger.info(
        f"Loaded {len(copy_replacements)} COPY replacements from {replacements_path}"
    )
    return copy_replacements


def collect_stats(aggregated_stats, stats):
    """  Aggregate the per-thread stats into the overall stats for this aggregated process """

    if not stats:
        return

    # take the maximum absolute connection difference between actual and expected
    if abs(stats['connection_diff_sec']) >= abs(aggregated_stats.get('connection_diff_sec', 0)):
        aggregated_stats['connection_diff_sec'] = stats['connection_diff_sec']

    # for each aggregated, add up these scalars across all threads
    for stat in ('transaction_success', 'transaction_error', 'query_success', 'query_error'):
        aggregated_stats[stat] += stats[stat]

    # same for arrays.
    for stat in ('transaction_error_log', 'connection_error_log'):
        # note that per the Manager python docs, this extra copy is required to
        # get manager to notice the update
        new_stats = aggregated_stats[stat]
        new_stats.update(stats[stat])
        aggregated_stats[stat] = new_stats


def percent(num, den):
    if den == 0:
        return 0
    return float(num) / den * 100.0


def init_stats(stats_dict):
    # init by key to ensure Manager is notified of change, if applicable
    stats_dict['connection_diff_sec'] = 0
    stats_dict['transaction_success'] = 0
    stats_dict['transaction_error'] = 0
    stats_dict['query_success'] = 0
    stats_dict['query_error'] = 0
    stats_dict['connection_error_log'] = {}  # map filename to array of connection errors
    stats_dict['transaction_error_log'] = {}  # map filename to array of transaction errors
    stats_dict['multi_statements'] = 0
    stats_dict['executed_queries'] = 0 # includes multi-statement queries
    return stats_dict


def display_stats(stats, total_connections, total_transactions, total_queries, peak_connections):
    stats_str = ""
    stats_str += f"Queries executed: {stats['query_success'] + stats['query_error']} of {total_queries} " \
                 f"({percent(stats['query_success'] + stats['query_error'], total_queries):.1f}%)"
    stats_str += "  ["
    stats_str += f"Success: {stats['query_success']} ({percent(stats['query_success'], stats['query_success'] + stats['query_error']):.1f}%), "
    stats_str += f"Failed: {stats['query_error']} ({percent(stats['query_error'], stats['query_success'] + stats['query_error']):.1f}%), "
    stats_str += f"Peak connections: {peak_connections.value}"
    stats_str += "]"

    logger.info(f"{stats_str}")


def join_finished_threads(connection_threads, worker_stats, wait=False):
    # join any finished threads
    finished_threads = []
    for t in connection_threads:
        if not t.is_alive() or wait:
            logger.debug(f"Joining thread {t.connection_log.session_initiation_time}")
            t.join()
            collect_stats(worker_stats, connection_threads[t])
            finished_threads.append(t)

    # remove the joined threads from the list of active ones
    for t in finished_threads:
        del connection_threads[t]

    logger.debug(f"Joined {len(finished_threads)} threads, {len(connection_threads)} still active.")
    return len(finished_threads)


def replay_worker(process_idx, replay_start_time, first_event_time, queue, error_logger, worker_stats, default_interface, odbc_driver,
                  connection_semaphore,
                  num_connections, peak_connections):
    """ Worker process to distribute the work among several processes.  Each
        worker pulls a connection off the queue, waits until its time to start
        it, spawns a thread to execute the actual connection and associated
        transactions, and then repeats. """

    # map thread to stats dict
    connection_threads = {}
    connections_processed = 0

    threading.current_thread().name = '0'

    perf_lock = threading.Lock()

    try:
        # prepend the process index to all log messages in this worker
        prepend_ids_to_logs(process_idx)

        # stagger worker startup to not hammer the get_cluster_credentials api
        time.sleep(random.randrange(1, 3))
        logger.debug(f"Worker {process_idx} ready for jobs")

        # time to block waiting for jobs on the queue
        timeout_sec = 10

        last_empty_queue_time = None

        # get the next job off the queue and wait until its due
        # loop terminates when a False is received over the queue
        while True:
            try:
                if connection_semaphore is not None:
                    logger.debug(
                        f"Checking for connection throttling ({num_connections.value} / {g_config['limit_concurrent_connections']} active connections)")
                    sem_start = time.time()
                    connection_semaphore.acquire()
                    sem_elapsed = time.time() - sem_start
                    logger.debug(f"Waited {sem_elapsed} sec for semaphore")

                job = queue.get(timeout=timeout_sec)
            except Empty:
                if connection_semaphore is not None:
                    connection_semaphore.release()

                elapsed = int(time.time() - last_empty_queue_time) if last_empty_queue_time else 0
                # take into account the initial timeout
                elapsed += timeout_sec
                empty_queue_timeout_sec = g_config.get("empty_queue_timeout_sec", 120)
                logger.debug(f"No jobs for {elapsed} seconds (timeout {empty_queue_timeout_sec})")
                # normally processes exit when they get a False on the queue,
                # but in case of some error we exit if the queue is empty for some time
                if elapsed > empty_queue_timeout_sec:
                    logger.warning(f"Queue empty for {elapsed} sec, exiting")
                    break
                if last_empty_queue_time is None:
                    last_empty_queue_time = time.time()
                continue

            last_empty_queue_time = None

            if job is False:
                logger.debug("Got termination signal, finishing up.")
                break

            thread_stats = init_stats({})

            # how much time has elapsed since the replay started
            time_elapsed_ms = current_offset_ms(replay_start_time)

            # what is the time offset of this connection job relative to the first event
            connection_offset_ms = job['connection'].offset_ms(first_event_time)
            delay_sec = (connection_offset_ms - time_elapsed_ms) / 1000.0

            logger.debug(
                f"Got job {job['job_id'] + 1}, delay {delay_sec:+.3f} sec (extracted connection time: {job['connection'].session_initiation_time})")

            # if connection time is more than a few ms in the future, sleep until its due.
            # this is approximate and we use "a few ms" here due to the imprecision of
            # sleep as well as the time for the remaining code to spawn a thread and actually
            # make the db connection.
            if connection_offset_ms - time_elapsed_ms > 10:
                time.sleep(delay_sec)

            logger.debug(
                f"Starting job {job['job_id'] + 1} (extracted connection time: {job['connection'].session_initiation_time}). {len(threading.enumerate())}, {threading.active_count()} connections active.")

            connection_thread = ConnectionThread(
                process_idx,
                job['job_id'],
                job['connection'],
                default_interface,
                odbc_driver,
                replay_start_time,
                first_event_time,
                error_logger,
                thread_stats,
                num_connections,
                peak_connections,
                connection_semaphore,
                perf_lock
            )
            connection_thread.name = f"{job['job_id']}"
            connection_thread.start()
            connection_threads[connection_thread] = thread_stats

            join_finished_threads(connection_threads, worker_stats, wait=False)

            connections_processed += 1

        logger.debug(f"Waiting for {len(connection_threads)} connections to finish...")
        join_finished_threads(connection_threads, worker_stats, wait=True)
    except Exception as e:
        logger.error(f"Process {process_idx} threw exception: {e}")
        logger.debug("".join(traceback.format_exception(*sys.exc_info())))

    if connections_processed:
        logger.debug(f"Max connection offset for this process: {worker_stats['connection_diff_sec']:.3f} sec")

    logger.debug(f"Process {process_idx} finished")


def put_and_retry(job, queue, timeout=10, non_workers=0):
    """ Retry adding to the queue indefinitely until it succeeds. This
        should only raise an exception and retry if the queue limit is hit. """
    attempt = 0
    while True:
        try:
            queue.put(job, block=True, timeout=timeout)
            break
        except Full:
            # check to make sure child processes are alive
            active_workers = len(multiprocessing.active_children()) - non_workers
            if active_workers == 0:
                logger.error("Queue full, but there don't appear to be any active workers.  Giving up.")
                return False
            attempt += 1
            logger.info(f"Queue full, retrying attempt {attempt}")

    return True


def sigint_handler(signum, frame):
    logger.error("Received SIGINT, shutting down...")

    for worker in g_workers:
        worker.terminate()

    for worker in g_workers:
        worker.join()
    logger.info("Workers terminated")

    raise KeyboardInterrupt


def start_replay(connection_logs, default_interface, odbc_driver, first_event_time, last_event_time,
                 num_workers, manager, error_logger, per_process_stats, total_transactions, total_queries):
    """ create a queue for passing jobs to the workers.  the limit will cause
    put() to block if the queue is full """
    queue = manager.Queue(maxsize=1000000)

    if not num_workers:
        # get number of available cpus, leave 1 for main thread and manager
        num_workers = os.cpu_count()
        if num_workers > 0:
            num_workers = max(num_workers - 1, 4)
        else:
            num_workers = 4
            logger.warning(
                f"Couldn't determine the number of cpus, defaulting to {num_workers} processes.  Use the configuration parameter num_workers to change this.")

    logger.debug(f"Running with {num_workers} workers")

    # find out how many processes we started with.  This is probably 1, due to the Manager
    initial_processes = len(multiprocessing.active_children())
    logger.debug(f"Initial child processes: {initial_processes}")

    global g_workers
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    connection_semaphore = None
    num_connections = manager.Value(int, 0)
    peak_connections = manager.Value(int, 0)
    if g_config.get('limit_concurrent_connections'):
        # create an IPC semaphore to limit the total concurrency
        connection_semaphore = manager.Semaphore(g_config.get('limit_concurrent_connections'))

    for idx in range(num_workers):
        per_process_stats[idx] = manager.dict()
        init_stats(per_process_stats[idx])
        g_workers.append(multiprocessing.Process(target=replay_worker,
                                                 args=(idx, g_replay_timestamp, first_event_time, queue, error_logger,
                                                       per_process_stats[idx], default_interface, odbc_driver,
                                                       connection_semaphore, num_connections, peak_connections)))
        g_workers[-1].start()

    signal.signal(signal.SIGINT, sigint_handler)

    logger.debug(f"Total connections in the connection log: {len(connection_logs)}")

    # add all the jobs to the work queue
    for idx, connection in enumerate(connection_logs):
        # if idx > 5:
        #     break
        if not put_and_retry({"job_id": idx, "connection": connection}, queue, non_workers=initial_processes):
            break

    # and add one termination "job"/signal for each worker so signal them to exit when
    # there is no more work
    for idx in range(num_workers):
        if not put_and_retry(False, queue, non_workers=initial_processes):
            break

    active_processes = len(multiprocessing.active_children()) - initial_processes
    logger.debug("Active processes: {}".format(active_processes))

    # and wait for the work to get done.
    logger.debug(f"{active_processes} processes running")
    cnt = 0

    while active_processes:
        cnt += 1
        active_processes = len(multiprocessing.active_children()) - initial_processes
        if cnt % 60 == 0:
            logger.debug(f"Waiting for {active_processes} processes to finish")
            try:
                queue_length = queue.qsize()
                logger.debug(f"Remaining connections: {queue_length - num_workers}")
            except NotImplementedError:
                # support for qsize is platform-dependent
                logger.debug("Queue length not supported.")

        # aggregate stats across all threads so far
        try:
            aggregated_stats = init_stats({})
            for idx, stat in per_process_stats.items():
                collect_stats(aggregated_stats, stat)
            if cnt % 5 == 0:
                display_stats(aggregated_stats, len(connection_logs), total_transactions, total_queries,
                              peak_connections)
                peak_connections.value = num_connections.value
        except KeyError:
            logger.debug("No stats to display yet.")

        time.sleep(1)

    # cleanup in case of error
    remaining_events = 0
    try:
        # clear out the queue in case of error to prevent broken pipe
        # exceptions from internal Queue thread
        while not queue.empty():
            remaining_events += 1
            queue.get_nowait()
    except Empty:
        pass

    if remaining_events > 0:
        logger.error("Not all jobs processed, replay unsuccessful")

    return True


def export_errors(connection_errors, transaction_errors, workload_location, replay_name):
    """ Save any errors that occurred during replay to a local directory or s3 """

    if len(connection_errors) == len(transaction_errors) == 0:
        logger.info("No errors, nothing to save")
        return

    logger.info(f"Saving {len(connection_errors)} connection errors, {len(transaction_errors)} transaction_errors")

    connection_error_location = (
            workload_location + "/" + replay_name + "/connection_errors"
    )
    transaction_error_location = (
            workload_location + "/" + replay_name + "/transaction_errors"
    )
    logger.info(f"Exporting connection errors to {connection_error_location}/")
    logger.info(f"Exporting transaction errors to {transaction_error_location}/")

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
                key_loc = "%s/%s/connection_errors/%s.txt" % (prefix, replay_name, filename)
            else:
                key_loc = "%s/connection_errors/%s.txt" % (replay_name, filename)
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
                key_loc = "%s/%s/transaction_errors/%s.txt" % (prefix, replay_name, filename)
            else:
                key_loc = "%s/transaction_errors/%s.txt" % (replay_name, filename)
            s3_client.put_object(
                Body=error_file_text,
                Bucket=bucket_name,
                Key=key_loc,
            )
        else:
            error_file = open(transaction_error_location + "/" + filename + ".txt", "w")
            error_file.write(error_file_text)
            error_file.close()


def assign_copy_replacements(connection_logs, replacements):
    for connection_log in connection_logs:
        for transaction in connection_log.transactions:
            for query in transaction.queries:
                if "copy " in query.text.lower() and "from 's3:" in query.text.lower():

                    from_text = re.search(r"from 's3:\/\/[^']*", query.text, re.IGNORECASE)
                    if from_text:
                        existing_copy_location = from_text.group()[6:]

                        try:
                            replacement_copy_location = replacements[existing_copy_location][0]
                        except KeyError:
                            logger.info(f"No COPY replacement found for {existing_copy_location}")
                            continue

                        if not replacement_copy_location:
                            replacement_copy_location = existing_copy_location

                        replacement_copy_iam_role = replacements[existing_copy_location][1]
                        if not replacement_copy_iam_role:
                            logger.error(
                                f"COPY replacement {existing_copy_location} is missing IAM role or credentials in {g_copy_replacements_filename}. Please add credentials or remove replacement.")
                            sys.exit()

                        query.text = query.text.replace(
                            existing_copy_location, replacement_copy_location
                        )

                        iam_replacements = [
                            (r"IAM_ROLE 'arn:aws:iam::\d+:role/\S+'", f" IAM_ROLE '{replacement_copy_iam_role}'"),
                            (r"credentials ''", f" IAM_ROLE '{replacement_copy_iam_role}'"),
                            (r"with credentials as ''", f" IAM_ROLE '{replacement_copy_iam_role}'"),
                            (r"IAM_ROLE ''", f" IAM_ROLE '{replacement_copy_iam_role}'"),
                            (r"ACCESS_KEY_ID '' SECRET_ACCESS_KEY '' SESSION_TOKEN ''",
                             f" IAM_ROLE '{replacement_copy_iam_role}'"),
                            (r"ACCESS_KEY_ID '' SECRET_ACCESS_KEY ''", f" IAM_ROLE '{replacement_copy_iam_role}'")
                        ]

                        for r in iam_replacements:
                            query.text = re.sub(r[0], r[1], query.text, flags=re.IGNORECASE)


def assign_unloads(connection_logs, replay_output, replay_name, unload_iam_role):
    for connection_log in connection_logs:
        for transaction in connection_log.transactions:
            for query in transaction.queries:

                if "unload" in query.text.lower() and "to 's3:" in query.text.lower():
                    to_text = re.search(r"to 's3:\/\/[^']*", query.text, re.IGNORECASE).group()[9:]

                    if to_text:
                        existing_unload_location = re.search(r"to 's3:\/\/[^']*", query.text, re.IGNORECASE).group()[4:]
                        replacement_unload_location = (
                                replay_output
                                + "/"
                                + replay_name
                                + "/UNLOADs/"
                                + to_text
                        )

                        new_query_text = query.text.replace(
                            existing_unload_location, replacement_unload_location
                        )
                        if not new_query_text == query.text:
                            query.text = new_query_text
                            query.text = re.sub(
                                r"IAM_ROLE 'arn:aws:iam::\d+:role/\S+'",
                                f" IAM_ROLE '{unload_iam_role}'",
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            query.text = re.sub(
                                r"credentials ''",
                                " IAM_ROLE '%s'" % unload_iam_role,
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            query.text = re.sub(
                                r"with credentials as ''",
                                " IAM_ROLE '%s'" % unload_iam_role,
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            query.text = re.sub(
                                r"IAM_ROLE ''",
                                " IAM_ROLE '%s'" % unload_iam_role,
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            query.text = re.sub(
                                r"ACCESS_KEY_ID '' SECRET_ACCESS_KEY '' SESSION_TOKEN ''",
                                " IAM_ROLE '%s'" % unload_iam_role,
                                query.text,
                                flags=re.IGNORECASE,
                            )
                            query.text = re.sub(
                                r"ACCESS_KEY_ID '' SECRET_ACCESS_KEY ''",
                                " IAM_ROLE '%s'" % unload_iam_role,
                                query.text,
                                flags=re.IGNORECASE,
                            )


def assign_time_intervals(connection_logs):
    for connection_log in connection_logs:
        for transaction in connection_log.transactions:
            if connection_log.time_interval_between_queries == "all on":
                is_calculate_time_interval = True
            elif connection_log.time_interval_between_queries == "all off":
                is_calculate_time_interval = False
            else:
                is_calculate_time_interval = str(transaction.time_interval).lower() == "true"
            if is_calculate_time_interval:
                for index, sql in enumerate(transaction.queries[1:]):
                    prev_sql = transaction.queries[
                        index
                    ]
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


def get_connection_credentials(username, database=None, max_attempts=10, skip_cache=False):
    credentials_timeout_sec = 3600
    retry_delay_sec = 10

    # how long to cache credentials per user
    cache_timeout_sec = 1800

    # check the cache
    if not skip_cache and g_credentials_cache.get(username) is not None:
        record = g_credentials_cache.get(username)
        if (datetime.datetime.now(tz=datetime.timezone.utc) - record[
            'last_update']).total_seconds() < cache_timeout_sec:
            logger.debug(f'Using {username} credentials from cache')
            return record['target_cluster_urls']
        del g_credentials_cache[username]

    cluster_endpoint = g_config["target_cluster_endpoint"]
    odbc_driver = g_config["odbc_driver"]

    cluster_endpoint_split = cluster_endpoint.split(".")
    cluster_id = cluster_endpoint_split[0]

    # Keeping NLB just for Serverless for now
    if g_config['nlb_nat_dns'] is not None and g_is_serverless:
        cluster_host = g_config['nlb_nat_dns']
    else:
        cluster_host = cluster_endpoint.split(":")[0]

    cluster_port = cluster_endpoint_split[5].split("/")[0][4:]
    database = cluster_endpoint_split[5].split("/")[1] if database is None else database

    additional_args = {}
    if os.environ.get('ENDPOINT_URL'):
        import urllib3
        # disable insecure warnings when testing endpoint is used
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        additional_args = {'endpoint_url': os.environ.get('ENDPOINT_URL'),
                           'verify': False}

    response = None
    db_user = None
    db_password = None
    secret_keys = ['admin_username', 'admin_password']

    if g_is_serverless:
        if g_config['secret_name'] is not None:
            logger.info(f"Fetching secrets from: {g_config['secret_name']}")
            secret_name = get_secret(g_config["secret_name"], g_config["target_cluster_region"])
            if len(set(secret_keys) - set(secret_name.keys())) == 0:
                response = {'DbUser': secret_name["admin_username"], 'DbPassword': secret_name["admin_password"]}
            else:
                logger.error(f"Required secrets not found: {secret_keys}")
                exit(-1)
        else:
            # Using backward compatibility method to fetch user credentials for serverless workgroup
            # rs_client = client('redshift-serverless', region_name=g_config.get("target_cluster_region", None))
            rs_client = client('redshift', region_name=g_config.get("target_cluster_region", None))
            for attempt in range(1, max_attempts + 1):
                try:
                    # response = rs_client.get_credentials(dbName=database, durationSeconds = 3600, workgroupName=cluster_id)
                    serverless_cluster_id = f"redshift-serverless-{cluster_id}"
                    logger.debug(f"Serverless cluster id {serverless_cluster_id} passed to get_cluster_credentials")
                    response = rs_client.get_cluster_credentials(
                        DbUser=username, ClusterIdentifier=serverless_cluster_id, AutoCreate=False,
                        DurationSeconds=credentials_timeout_sec
                    )
                except rs_client.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'ExpiredToken':
                        logger.error(f"Error retrieving credentials for {cluster_id}: IAM credentials have expired.")
                        exit(-1)
                    elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                        logger.error(f"Serverless endpoint could not be found "
                                    f"RedshiftServerless:GetCredentials. {e}")
                        exit(-1)
                    else:
                        logger.error(f"Got exception retrieving credentials ({e.response['Error']['Code']})")
                        raise e

                if response is None or response.get('DbPassword') is None:
                    logger.warning(f"Failed to retrieve credentials for db {database} (attempt {attempt}/{max_attempts})")
                    logger.debug(response)
                    response = None
                    if attempt < max_attempts:
                        time.sleep(retry_delay_sec)
                else:
                    break
            db_user = response['DbUser']
            db_password = response['DbPassword']
    else:
        rs_client = client("redshift", region_name=g_config.get("target_cluster_region", None), **additional_args)
        for attempt in range(1, max_attempts + 1):
            try:
                response = rs_client.get_cluster_credentials(
                    DbUser=username, ClusterIdentifier=cluster_id, AutoCreate=False,
                    DurationSeconds=credentials_timeout_sec
                )
            except NoCredentialsError:
                raise CredentialsException("No credentials found")
            except rs_client.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'ExpiredToken':
                    logger.error(f"Error retrieving credentials for {cluster_id}: IAM credentials have expired.")
                    exit(-1)
                else:
                    logger.error(f"Got exception retrieving credentials ({e.response['Error']['Code']})")
                    raise e
            except rs_client.exceptions.ClusterNotFoundFault:
                logger.error(f"Cluster {cluster_id} not found. Please confirm cluster endpoint, account, and region.")
                exit(-1)

            if response is None or response.get('DbPassword') is None:
                logger.warning(f"Failed to retrieve credentials for user {username} (attempt {attempt}/{max_attempts})")
                logger.debug(response)
                response = None
                if attempt < max_attempts:
                    time.sleep(retry_delay_sec)
            else:
                break
        db_user = response["DbUser"]
        db_password = response["DbPassword"]

    if response is None:
        msg = f"Failed to retrieve credentials for {username}"
        raise CredentialsException(msg)

    cluster_odbc_url = (
        "Driver={}; Server={}; Database={}; IAM=1; DbUser={}; DbPassword={}; Port={}".format(
            odbc_driver,
            cluster_host,
            database,
            db_user.split(":")[1] if ":" in db_user else db_user,
            db_password,
            cluster_port,
        )
    )

    cluster_psql = {
        "username": db_user,
        "password": db_password,
        "host": cluster_host,
        "port": cluster_port,
        "database": database,
    }

    credentials = {  # old params
        'odbc': cluster_odbc_url,
        'psql': cluster_psql,
        # new params
        'username': db_user,
        'password': db_password,
        'host': cluster_host,
        'port': cluster_port,
        'database': database,
        'odbc_driver': g_config["odbc_driver"]
    }
    logger.debug("Successfully retrieved database credentials for {}".format(username))
    g_credentials_cache[username] = {'last_update': datetime.datetime.now(tz=datetime.timezone.utc),
                                     'target_cluster_urls': credentials}
    return credentials


def unload_system_table(
        default_interface,
        unload_system_table_queries_file,
        unload_location,
        unload_iam_role,
):
    # TODO: wrap this in retries and proper logging
    credentials = get_connection_credentials(g_config["master_username"], max_attempts=3)
    conn = db_connect(default_interface,
                      host=credentials['host'],
                      port=int(credentials['port']),
                      username=credentials['username'],
                      password=credentials['password'],
                      database=credentials['database'],
                      odbc_driver=credentials['odbc_driver'])

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
            try:
                cursor.execute(unload_query)
            except Exception as e:
                logger.error(f"Failed to unload query. {e}")
            logger.debug(f"Executed unload query: {unload_query}")


def validate_config(config):
    if not bool(re.fullmatch(g_cluster_endpoint_pattern, config["target_cluster_endpoint"])):
        logger.error(
            'Config file value for "target_cluster_endpoint" is not a valid endpoint. Endpoints must be in the format '
            'of <cluster-hostname>:<port>/<database-name>.'
        )
        exit(-1)
    if not config["target_cluster_region"]:
        logger.error(
            'Config file value for "target_cluster_region" is required.'
        )
        exit(-1)
    if not config["odbc_driver"]:
        config["odbc_driver"] = None
        logger.debug(
            'Config file missing value for "odbc_driver" so replay will not use ODBC. Please provide a value for '
            '"odbc_driver" to replay using ODBC.'
        )
    if config["odbc_driver"] or config["default_interface"] == "odbc":
        try:
            import pyodbc
        except ValueError:
            logger.error(
                'Import of pyodbc failed. Please install pyodbc library to use ODBC as default driver. Please refer '
                'to REAME.md'
            )
            exit(-1)
    if not (
            config["default_interface"] == "psql"
            or config["default_interface"] == "odbc"
    ):
        logger.error(
            'Config file value for "default_interface" must be either "psql" or "odbc". Please change the value for '
            '"default_interface" to either "psql" or "odbc".'
        )
        exit(-1)
    if not (
            config["time_interval_between_transactions"] == ""
            or config["time_interval_between_transactions"] == "all on"
            or config["time_interval_between_transactions"] == "all off"
    ):
        logger.error(
            'Config file value for "time_interval_between_transactions" must be either "", "all on", or "all off". '
            'Please change the value for "time_interval_between_transactions" to be "", "all on", or "all off".'
        )
        exit(-1)
    if not (
            config["time_interval_between_queries"] == ""
            or config["time_interval_between_queries"] == "all on"
            or config["time_interval_between_queries"] == "all off"
    ):
        logger.error(
            'Config file value for "time_interval_between_queries" must be either "", "all on", or "all off". Please '
            'change the value for "time_interval_between_queries" to be "", "all on", or "all off".'
        )
        exit(-1)
    if not (
            config["execute_copy_statements"] == "true"
            or config["execute_copy_statements"] == "false"
    ):
        logger.error(
            'Config file value for "execute_copy_statements" must be either "true" or "false". Please change the value '
            'for "execute_copy_statements" to either "true" or "false".'
        )
        exit(-1)
    if not (
            config["execute_unload_statements"] == "true"
            or config["execute_unload_statements"] == "false"
    ):
        logger.error(
            'Config file value for "execute_unload_statements" must be either "true" or "false". Please change the '
            'value for "execute_unload_statements" to either "true" or "false".'
        )
        exit(-1)
    if config["replay_output"] and not config["replay_output"].startswith(
            "s3://"
    ):
        logger.error(
            'Config file value for "replay_output" must be an S3 location (starts with "s3://"). Please remove this '
            'value or put in an S3 location.'
        )
        exit(-1)
    if not config["replay_output"] and config["execute_unload_statements"] == "true":
        logger.error(
            'Config file value for "replay_output" is not provided while "execute_unload_statements" is set to true. '
            'Please provide a valid S3 location for "replay_output".'
        )
        exit(-1)
    if (
            config["replay_output"]
            and config["target_cluster_system_table_unload_iam_role"]
            and not config["unload_system_table_queries"]
    ):
        logger.error(
            'Config file missing value for "unload_system_table_queries". Please provide a value for '
            '"unload_system_table_queries", or remove the value for "target_cluster_system_table_unload_iam_role".'
        )
        exit(-1)
    if (
            config["replay_output"]
            and config["target_cluster_system_table_unload_iam_role"]
            and config["unload_system_table_queries"]
            and not config["unload_system_table_queries"].endswith(".sql")
    ):
        logger.error(
            'Config file value for "unload_system_table_queries" does not end with ".sql". Please ensure the value for '
            '"unload_system_table_queries" ends in ".sql". See the provided "unload_system_tables.sql" as an example.'
        )
        exit(-1)
    if not config["workload_location"]:
        logger.error(
            'Config file missing value for "workload_location". Please provide a value for "workload_location".'
        )
        exit(-1)
    if not config["nlb_nat_dns"]:
        config["nlb_nat_dns"] = None
        logger.debug(
            'No NLB / NAT endpoint specified. Replay will use target_cluster_endpoint to connect.'
        )
    if not config["secret_name"]:
        config["secret_name"] = None
        logger.debug(
            'SECRET_NAME property not specified.'
        )

    config['filters'] = validate_and_normalize_filters(ConnectionLog, config.get('filters', {}))


def print_stats(stats):
    if 0 not in stats:
        logger.warning("No stats gathered.")
        return

    max_connection_diff = 0
    for process_idx in stats.keys():
        if abs(stats[process_idx].get('connection_diff_sec', 0)) > abs(max_connection_diff):
            max_connection_diff = stats[process_idx]['connection_diff_sec']
        logger.debug(
            f"[{process_idx}] Max connection offset: {stats[process_idx].get('connection_diff_sec', 0):+.3f} sec")
    logger.debug(f"Max connection offset: {max_connection_diff:+.3f} sec")


def main():
    global logger
    logger = init_logging(logging.INFO)

    global g_config
    global g_is_serverless

    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", type=str, help="Location of replay config file.",)
    args = parser.parse_args()

    g_config = load_config(args.config_file)
    if not g_config:
        logger.error("Failed to load config file")
        sys.exit(-1)
    validate_config(g_config)

    level = logging.getLevelName(g_config.get('log_level', 'INFO').upper())
    set_log_level(level)

    if g_config.get("logfile_level") != "none":
        level = logging.getLevelName(g_config.get('logfile_level', 'DEBUG').upper())
        log_file = 'replay.log'
        logging_dir = g_config.get("logging_dir", "simplereplay_logs")
        logger.info(f"Saving SimpleReplay logs to {logging_dir}")
        add_logfile(log_file, level=level, dir=logging_dir,
                    preamble=yaml.dump(g_config), backup_count=g_config.get("backup_count", 2))

    # print the version
    log_version()

    # populate the global is_serverless variable
    if bool(re.fullmatch(g_serverless_cluster_endpoint_pattern, g_config['target_cluster_endpoint'])):
        logger.info(f"Replaying on Redshift Serverless Cluster {g_config['target_cluster_endpoint']}")
        g_is_serverless = True
    else:
        g_is_serverless = False

    cluster = cluster_dict(g_config["target_cluster_endpoint"], g_is_serverless)

    # generate replay id/hash
    global g_replay_timestamp
    g_replay_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
    logger.info(f"Replay start time: {g_replay_timestamp}")

    id_hash = hashlib.sha1(g_replay_timestamp.isoformat().encode("UTF-8")).hexdigest()[:5]
    if g_config.get("tag", "") != "":
        replay_id = f'{g_replay_timestamp.isoformat()}_{cluster.get("id")}_{g_config["tag"]}_{id_hash}'
    else:
        replay_id = f'{g_replay_timestamp.isoformat()}_{cluster.get("id")}_{id_hash}'

    manager = SyncManager()

    def init_manager():
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    manager.start(init_manager)

    if not g_config["replay_output"]:
        g_config["replay_output"] = None

    (connection_logs, total_connections) = parse_connections(
        g_config["workload_location"],
        g_config["time_interval_between_transactions"],
        g_config["time_interval_between_queries"],
    )
    logger.info(
        f"Found {total_connections} total connections, {total_connections - len(connection_logs)} are excluded by filters. Replaying {len(connection_logs)}.")

    # Associate transactions with connections
    logger.info(
        f"Loading transactions from {g_config['workload_location']}, this might take some time."
    )

    # group all connections by connection key
    connection_idx_by_key = {}
    for idx, c in enumerate(connection_logs):
        connection_key = get_connection_key(c.database_name, c.username, c.pid)
        connection_idx_by_key.setdefault(connection_key, []).append(idx)

    all_transactions = parse_transactions(g_config["workload_location"])

    transaction_count = len(all_transactions)
    query_count = 0

    # assign the correct connection to each transaction by looking at the most
    # recent connection prior to the transaction start. This relies on connections
    # being sorted.
    for idx, t in enumerate(all_transactions):
        connection_key = get_connection_key(t.database_name, t.username, t.pid)
        possible_connections = connection_idx_by_key[connection_key]
        best_match_idx = None
        for c_idx in possible_connections:
            # truncate session start time, since query/transaction time is truncated to seconds
            if connection_logs[c_idx].session_initiation_time.replace(microsecond=0) > t.start_time():
                break
            best_match_idx = c_idx
        if best_match_idx is None:
            logger.warning(
                f"Couldn't find matching connection in {len(possible_connections)} connections for transaction {t}, skipping")
            continue
        connection_logs[best_match_idx].transactions.append(t)
        query_count += len(t.queries)

    logger.info(f"Found {transaction_count} transactions, {query_count} queries")
    connection_logs = [_ for _ in connection_logs if len(_.transactions) > 0]
    logger.info(f"{len(connection_logs)} connections contain transactions and will be replayed ")

    global g_total_connections
    g_total_connections = len(connection_logs)

    first_event_time = datetime.datetime.now(tz=datetime.timezone.utc)
    last_event_time = datetime.datetime.utcfromtimestamp(0).replace(
        tzinfo=datetime.timezone.utc
    )

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

        if connection.transactions[0].queries[0].start_time and connection.transactions[0].queries[
            0].start_time < first_event_time:
            first_event_time = connection.transactions[0].queries[0].start_time
        if connection.transactions[-1].queries[-1].end_time and connection.transactions[-1].queries[
            -1].end_time > last_event_time:
            last_event_time = connection.transactions[-1].queries[-1].end_time

    logger.info(
        "Estimated original workload execution time: "
        + str((last_event_time - first_event_time))
    )

    if g_config["execute_copy_statements"] == "true":
        logger.debug("Configuring COPY replacements")
        replacements = parse_copy_replacements(g_config["workload_location"])
        assign_copy_replacements(connection_logs, replacements)

    if g_config["execute_unload_statements"] == "true":
        if g_config["unload_iam_role"]:
            if g_config["replay_output"].startswith("s3://"):
                logger.debug("Configuring UNLOADs")
                assign_unloads(
                    connection_logs,
                    g_config["replay_output"],
                    replay_id,
                    g_config["unload_iam_role"],
                )
            else:
                logger.debug(
                    'UNLOADs not configured since "replay_output" is not an S3 location.'
                )

    logger.debug("Configuring time intervals")
    assign_time_intervals(connection_logs)

    logger.debug("Configuring CREATE USER PASSWORD random replacements")
    assign_create_user_password(connection_logs)

    # test connection
    try:
        # use the first user as a test
        get_connection_credentials(connection_logs[0].username, database=connection_logs[0].database_name, max_attempts=1)
    except CredentialsException as e:
        logger.error(f"Unable to retrieve credentials using GetClusterCredentials ({str(e)}).  "
                     f"Please verify that an IAM policy exists granting access.  See the README for more details.")
        sys.exit(-1)

    if len(connection_logs) == 0:
        logger.info("No logs to replay, nothing to do.")
        sys.exit()

    # Actual replay
    logger.debug("Starting replay")
    error_list = manager.list()
    per_process_stats = {}
    complete = False

    try:
        start_replay(connection_logs,
                     g_config["default_interface"],
                     g_config["odbc_driver"],
                     first_event_time,
                     last_event_time,
                     g_config.get("num_workers"),
                     manager,
                     error_list,
                     per_process_stats,
                     transaction_count,
                     query_count)
        complete = True
    except KeyboardInterrupt:
        replay_id += '_INCOMPLETE'
        logger.warning("Got CTRL-C, exiting...")
    except Exception as e:
        replay_id += '_INCOMPLETE'
        logger.error(f"Replay terminated. {e}")

    logger.debug("Aggregating stats")
    aggregated_stats = init_stats({})
    for idx, stat in per_process_stats.items():
        collect_stats(aggregated_stats, stat)

    replay_summary = []
    logger.info("Replay summary:")
    replay_summary.append(f"Attempted to replay {query_count} queries, {transaction_count} transactions, "
                          f"{len(connection_logs)} connections.")
    try:
        replay_summary.append(
            f"Successfully replayed {aggregated_stats.get('transaction_success', 0)} out of {transaction_count} "
            f"({round((aggregated_stats.get('transaction_success', 0) / transaction_count) * 100)}%) transactions."
        )
        replay_summary.append(
            f"Successfully replayed {aggregated_stats.get('query_success', 0)} out of {query_count} "
            f"({round((aggregated_stats.get('query_success', 0) / query_count) * 100)}%) queries."
        )
    except ZeroDivisionError:
        pass

    error_location = g_config.get("error_location", g_config["workload_location"])

    replay_summary.append(f"Encountered {len(aggregated_stats['connection_error_log'])} "
                          f"connection errors and {len(aggregated_stats['transaction_error_log'])} transaction errors")

    # and save them
    export_errors(
        aggregated_stats['connection_error_log'],
        aggregated_stats['transaction_error_log'],
        error_location,
        replay_id,
    )

    summary_stats = {"query_count": query_count,
                     "connection_count": len(connection_logs),
                     "transaction_count": transaction_count,
                     "query_success": aggregated_stats.get('query_success', 0),
                     "connection_errors": len(aggregated_stats['connection_error_log']),
                     "transaction_errors": len(aggregated_stats['transaction_error_log']),
                     }

    if len(error_list) != 0:
        bucket = bucket_dict(g_config["analysis_output"])
        with open('replayerrors000', 'w', newline='') as output_file:
            try:
                dict_writer = csv.DictWriter(output_file, fieldnames=error_list[0].keys())
                dict_writer.writeheader()
                dict_writer.writerows(error_list)
            except Exception as e:
                logger.debug(f"Failed to write replay errors to CSV. {e}")

        try:
            boto3.resource('s3').Bucket(bucket.get('bucket_name')) \
                .upload_file(output_file.name, f"{bucket['prefix']}analysis/{replay_id}/raw_data/{output_file.name}")
        except Exception as e:
            logger.debug(f"Error upload to S3 {bucket['bucket_name']} failed. {e}")

    replay_end_time = datetime.datetime.now(tz=datetime.timezone.utc)
    replay_summary.append(f"Replay finished in {replay_end_time - g_replay_timestamp}.")
    for line in replay_summary:
        logger.info(line)

    logger.info(f"Replay finished in {datetime.datetime.now(tz=datetime.timezone.utc) - g_replay_timestamp}.")

    if g_config.get("analysis_iam_role") and g_config.get("analysis_output"):
        try:
            run_replay_analysis(replay=replay_id,
                                cluster_endpoint=g_config["target_cluster_endpoint"],
                                start_time=g_replay_timestamp,
                                end_time=replay_end_time,
                                bucket_url=g_config["analysis_output"],
                                iam_role=g_config["analysis_iam_role"],
                                user=g_config["master_username"],
                                tag=g_config["tag"],
                                workload=g_config["workload_location"],
                                is_serverless=g_is_serverless,
                                secret_name=g_config["secret_name"],
                                nlb_nat_dns=g_config["nlb_nat_dns"],
                                complete=complete,
                                stats=summary_stats,
                                summary=replay_summary)
        except Exception as e:
            logger.error(f"Could not complete replay analysis. {e}")
    else:
        logger.info("Analysis not enabled for this replay.")

    if (
            g_config["replay_output"]
            and g_config["unload_system_table_queries"]
            and g_config["target_cluster_system_table_unload_iam_role"]
    ):
        logger.info(f'Exporting system tables to {g_config["replay_output"]}')

        unload_system_table(
            g_config["default_interface"],
            g_config["unload_system_table_queries"],
            g_config["replay_output"] + "/" + replay_id,
            g_config["target_cluster_system_table_unload_iam_role"],
        )

        logger.info(f'Exported system tables to {g_config["replay_output"]}')

    print_stats(per_process_stats)
    manager.shutdown()


if __name__ == "__main__":
    main()
