from datetime import datetime, timedelta
import sys
import logging
import pg
import re
from util.sql.sql_text_helpers import GET_SAFE_LOG_STRING
import pytz


import boto3
from pg import connect

options = "keepalives=1 keepalives_idle=200 keepalives_interval=200 keepalives_count=6 connect_timeout=10"

set_timeout_stmt = "set statement_timeout = 0"


class RedshiftClusterFactory:
    def __init__(self):
        pass

    @staticmethod
    def from_pg_details(pg_details):
        c = RedshiftCluster(cluster_endpoint=pg_details.host)
        c.set_db(pg_details.database)
        c.set_user(pg_details.user)
        c.set_port(pg_details.port)
        c.set_password(pg_details.password)
        return c

    @staticmethod
    def from_cluster(cluster):
        c = RedshiftCluster(cluster_endpoint=cluster.get_host())
        c.set_db(cluster.get_db())
        c.set_user(cluster.get_user())
        c.set_port(cluster.get_port())
        c.set_password(cluster.get_password())
        return c


class RedshiftCluster:
    def __init__(self, cluster_endpoint):
        self._password = None
        self._user = None
        self._db = None
        self._port = None
        self.database_connections = {}
        self.database_timeouts = {}
        self.cluster_endpoint = cluster_endpoint
        self._user_auto_create = False
        self._user_creds_expiration = datetime.now(pytz.utc)
        self._user_db_groups = []
        self._configured_timeout = None
        self.has_temporary_password = False

    def __eq__(self, other):
        return type(self) == type(other) and \
               self.get_user() == other.get_user() and \
               self.get_db() == other.get_db() and \
               self.get_host() == other.get_host() and \
               self.get_port() == other.get_port()

    def get_user(self):
        return self._user

    def set_user(self, user):
        self._user = user

    def get_password(self):
        if self._password is None or self.is_temporary_credential_expired():
            self.refresh_temporary_credentials()
        # noinspection PyBroadException
        try:
            self._password = self._password.decode('utf-8')
        except:
            pass  # If we cannot decode it it could be a valid byte string already
        return self._password

    def set_password(self, password):
        self._password = password

    def get_host(self):
        return self.cluster_endpoint

    def set_host(self, cluster_endpoint):
        self.cluster_endpoint = cluster_endpoint

    def get_port(self):
        return self._port

    def set_port(self, port):
        self._port = port

    def get_db(self):
        return self._db

    def set_db(self, db):
        self._db = db

    def get_user_auto_create(self):
        return self._user_auto_create

    def set_user_auto_create(self, user_auto_create):
        self._user_auto_create = user_auto_create

    def get_user_db_groups(self):
        return self._user_db_groups

    def set_user_db_groups(self, user_db_groups):
        self._user_db_groups = user_db_groups

    def get_user_creds_expiration(self):
        return self._user_creds_expiration

    def set_user_creds_expiration(self, user_creds_expiration):
        self._user_creds_expiration = user_creds_expiration

    def is_temporary_credential_expired(self):
        if not self.has_temporary_password:
            return False
        one_minute_from_now = datetime.now(pytz.utc) + timedelta(minutes=1)
        if self.get_user_creds_expiration() is None:
            return True

        expiration_time = self.get_user_creds_expiration()
        if one_minute_from_now > expiration_time:
            return True
        return False

    def refresh_temporary_credentials(self):
        logging.debug("Try getting DB credentials for {u}@{c}".format(u=self.get_user(), c=self.get_host()))
        redshift_client = boto3.client('redshift', region_name=self.get_region_name())
        get_creds_params = {
            'DbUser': self.get_user(),
            'DbName': self.get_db(),
            'ClusterIdentifier': self.get_cluster_identifier()
        }
        if self.get_user_auto_create():
            get_creds_params['AutoCreate'] = True
        if len(self.get_user_db_groups()) > 0:
            get_creds_params['DbGroups'] = self.get_user_db_groups()
        # Change botocore.parsers to avoid logging of boto3 response since it contains the credentials
        log_level = logging.getLogger('botocore.parsers').getEffectiveLevel()
        logging.getLogger('botocore.parsers').setLevel(logging.INFO)
        response = redshift_client.get_cluster_credentials(**get_creds_params)
        logging.getLogger('botocore.parsers').setLevel(log_level)

        self.set_user(response['DbUser'])
        self.set_password(response['DbPassword'])
        self.set_user_creds_expiration(response['Expiration'])

    @staticmethod
    def get_cluster_endpoint_regex():
        """
        A cluster endpoint is comprised of letters, digits, or hyphens

        From http://docs.aws.amazon.com/redshift/latest/mgmt/managing-clusters-console.html
            They must contain from 1 to 63 alphanumeric characters or hyphens.
            Alphabetic characters must be lowercase.
            The first character must be a letter.
            They cannot end with a hyphen or contain two consecutive hyphens.
            They must be unique for all clusters within an AWS account.
        :return:
        """
        cluster_endpoint_regex_parts = [
            {
                'name': 'cluster_identifier',
                'pattern': '[a-z][a-z0-9-]*'
            },
            {
                'pattern': r'\.'
            },
            {
                'name': 'customer_hash',
                'pattern': r'[0-9a-z]+'
            },
            {
                'pattern': r'\.'
            },
            {
                'name': 'region',
                'pattern': '[a-z]+-[a-z]+-[0-9]+'
            },
            {
                'pattern': r'\.redshift\.amazonaws\.com$'
            }
        ]
        pattern = ''
        for element in cluster_endpoint_regex_parts:
            if 'name' in element.keys():
                pattern += '(?P<' + element['name'] + '>'
            pattern += element['pattern']
            if 'name' in element.keys():
                pattern += ')'
        return re.compile(pattern)

    def get_element_from_cluster_endpoint(self, element):
        match_result = RedshiftCluster.get_cluster_endpoint_regex().match(self.cluster_endpoint.lower())
        if match_result is not None:
            return match_result.groupdict()[element]
        else:
            logging.fatal('Could not extract region from cluster endpoint {cluster_endpoint}'.format(
                cluster_endpoint=self.cluster_endpoint.lower()))

    def get_region_name(self):
        return self.get_element_from_cluster_endpoint('region')

    def get_cluster_identifier(self):
        return self.get_element_from_cluster_endpoint('cluster_identifier')

    def _conn_to_rs(self, opt=options, timeout=set_timeout_stmt, database=None):
        rs_conn_string = "host={host} port={port} dbname={db} user={user} password={password} {opt}".format(
            host=self.get_host(),
            port=self.get_port(),
            db=self.get_db(),
            password=self.get_password(),  # First fetch the password because temporary password updates user!
            user=self.get_user(),
            opt=opt)
        logging.debug(GET_SAFE_LOG_STRING(rs_conn_string))
        try:
            # noinspection PyArgumentList
            rs_conn = connect(rs_conn_string)
        except pg.InternalError as ie:
            if hasattr(ie, 'args') and len(ie.args) > 0 \
                    and ('Operation timed out' in ie.args[0] or 'timeout expired' in ie.args[0]):
                msg = 'Connection timeout when connecting to {h}:{p}.\n'
                msg += 'Make sure that firewalls and security groups allow connections.'
                logging.fatal(msg.format(h=self.get_host(), p=self.get_port()))
            else:
                logging.fatal('Internal error encountered when trying to connect: {ie}'.format(ie=ie))
            raise sys.exc_info()[0](sys.exc_info()[1]).with_traceback(sys.exc_info()[2])
        if self._configured_timeout is not None and not self._configured_timeout == timeout:
            rs_conn.query(timeout)
            self.database_timeouts[database][opt] = timeout
        return rs_conn

    def get_conn_to_rs(self, opt=options, timeout=set_timeout_stmt, database=None):
        database = database or self.get_db()
        if database in self.database_connections:
            if opt in self.database_connections[database]:
                if not (opt in self.database_timeouts[database] and self.database_timeouts[database][opt] == timeout):
                    logging.debug('Timeout is different from last configured timeout.')
                    self.database_connections[database][opt].query(timeout)
                return self.database_connections[database][opt]
        else:
            self.database_connections[database] = {}
            self.database_timeouts[database] = {}
        self.database_connections[database][opt] = self._conn_to_rs(opt=opt, timeout=timeout, database=database)
        return self.database_connections[database][opt]

    def execute_update(self, command, opt=options, timeout=set_timeout_stmt, database=None):
        conn_rs = self.get_conn_to_rs(opt=opt, timeout=timeout, database=database)
        logging.debug('Executing update:' + GET_SAFE_LOG_STRING(command))
        conn_rs.query(command)

    def get_query_full_result_as_list_of_dict(self, sql, opt=options, timeout=set_timeout_stmt, database=None):
        """
        Inefficient way to store data but nice and easy for queries with small result sets.
        :return:
        """
        conn_rs = self.get_conn_to_rs(opt=opt, timeout=timeout, database=database)
        logging.debug('Executing query:' + GET_SAFE_LOG_STRING(sql))
        result = conn_rs.query(sql)
        dict_result = result.dictresult()
        return dict_result

    def __del__(self):
        for database in self.database_connections:
            for option in self.database_connections[database]:
                if self.database_connections[database][option] is not None:
                    # noinspection PyBroadException
                    try:
                        self.database_connections[database][option].close()
                    except:
                        logging.warning('Could not correctly close self.database_connections{db}{opt}'.format(
                            db=database,
                            opt=option
                        ))
                    self.database_connections[database][option] = None
