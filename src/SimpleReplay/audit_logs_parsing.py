"""
auditlogs_parsing.py

This module parses various auditlogs
"""

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
