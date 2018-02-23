import os
HOME_DIR = os.environ['HOME']
DEFAULT_PGPASS_FILE_LOCATION = '{home}/.pgpass'.format(home=HOME_DIR)


class PGPassDetails:
    def __init__(self, line):
        self.host, self.port, self.database, self.user, self.password = line.split(':')
        self.password = self.password.rstrip('\n')

    def __str__(self):
        connect_string = 'host={h} port={p} user={u} dbname={db}'.format(
            h=self.host,
            p=self.port,
            u=self.user,
            db=self.database
        )
        return connect_string


class PGPassReader:
    HOSTNAME = 0
    PORT = 1
    DATABASE = 2
    USERNAME = 3
    PASSWORD = 4

    def __init__(self, pgpass_file_location=DEFAULT_PGPASS_FILE_LOCATION):
        self.pgpass_file_location = pgpass_file_location

    def get_first_match(self, hostname=None, port=None, database=None, user=None, password=None):
        pg_pass_filter = PGPassReader.PGPassFilter().has_hostname(hostname)\
                                            .has_port(port)\
                                            .has_database(database)\
                                            .has_user(user)\
                                            .has_password(password)

        with open(self.pgpass_file_location, 'r') as password_file:
            for line in password_file:
                if pg_pass_filter.matches(line):
                    return PGPassDetails(line)

    class PGPassFilter:

        def __init__(self):
            self.filters = []

        def matches(self, line):
            result = True
            for f in self.filters:
                result = result and f(line)
            return result

        @staticmethod
        def get_element_filter(element_position, element_value):
            if element_value is not None:
                return lambda x: x.split(':')[element_position] == element_value

        def add_filter_if_not_none(self, pgpass_filter):
            if pgpass_filter is not None:
                self.filters.append(pgpass_filter)

        def has_hostname(self, hostname):
            self.add_filter_if_not_none(PGPassReader.PGPassFilter.get_element_filter(PGPassReader.HOSTNAME, hostname))
            return self

        def has_port(self, port):
            """

            :param port: Could be int or str
            :return:
            """
            if port is not None:
                port = str(port)
            self.add_filter_if_not_none(PGPassReader.PGPassFilter.get_element_filter(PGPassReader.PORT, port))
            return self

        def has_database(self, database):
            self.add_filter_if_not_none(PGPassReader.PGPassFilter.get_element_filter(PGPassReader.DATABASE, database))
            return self

        def has_user(self, username):
            self.add_filter_if_not_none(PGPassReader.PGPassFilter.get_element_filter(PGPassReader.USERNAME, username))
            return self

        def has_password(self, password):
            self.add_filter_if_not_none(PGPassReader.PGPassFilter.get_element_filter(PGPassReader.PASSWORD, password))
            return self
