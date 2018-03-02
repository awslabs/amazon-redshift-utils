from unittest import TestCase
from util.redshift_cluster import RedshiftClusterFactory
from util.resources import DBResource, SchemaResource, TableResource
from util.sql_queries import CREATE_SCHEMA
from util.pgpass import PGPassReader
from random import choice
from global_config import GlobalConfigParametersReader
import string
import os


class RedshiftUnloadCopyClusterTests(TestCase):
    def setUp(self):
        global_config_reader = GlobalConfigParametersReader()
        arguments = ['app-name', 'config-file', 'eu-west-1', '--destination-schema-auto-create',
                     '--destination-table-auto-create']
        global_config_reader.get_config_key_values_updated_with_cli_args(arguments)
        self.configure_test_cluster()
        self.create_test_table()

    def configure_test_cluster(self):
        if 'TEST_CLUSTER' not in os.environ:
            self.fail('TEST_CLUSTER environment variable should be cluster FQN hostname')
        else:
            cluster_details = PGPassReader().get_first_match(hostname=os.environ['TEST_CLUSTER'])
        self.cluster = RedshiftClusterFactory.from_pg_details(cluster_details)

    def create_test_table(self):
        self.test_table_name = self.get_random_identifier()
        self.test_schema_name = self.get_random_identifier()
        self.test_schema = SchemaResource(self.cluster, self.test_schema_name)
        schema_ddl = self.test_schema.get_query_sql_text_with_parameters_replaced(CREATE_SCHEMA)
        self.test_schema.create(schema_ddl)
        self.test_table = TableResource(self.cluster, self.test_schema_name, self.test_table_name)
        ddl = 'CREATE TABLE {s}.{t}(id int);'
        self.test_table.create(ddl.format(s=self.test_schema_name, t=self.test_table_name))

    def tearDown(self):
        self.test_table.drop()
        self.test_schema.drop()

    # noinspection PyUnusedLocal
    @staticmethod
    def get_random_identifier(*args, **kwargs):
        return ''.join([choice(string.ascii_lowercase) for i in range(0, 20)])

    def test_check_if_dev_database_exists(self):
        cluster = RedshiftClusterFactory.from_cluster(self.cluster)
        cluster.set_db('dev')
        self.assertTrue(DBResource(cluster).is_present(force_update=True))

    def test_test_schema_and_table_exists(self):
        self.assertTrue(self.test_schema.is_present())
        self.assertTrue(self.test_table.is_present())

    def test_retrieve_tbl_ddl(self):
        table = TableResource(self.cluster, self.test_schema_name, self.test_table_name)
        ddl_text = None
        if table.is_present():
            ddl_text = table.get_create_sql(generate=True)
        start = r'CREATE TABLE IF NOT EXISTS ["]*{s}["]*.["]*{t}["]*'.format(s=self.test_schema_name,
                                                                             t=self.test_table_name)
        self.assertRegexpMatches(ddl_text, start, 'Create table is not present')
        self.assertRegexpMatches(ddl_text, r'["]*id["]* INTEGER', 'Column definition is not present')
