import re
from abc import abstractmethod
import logging

from util.child_object import ChildObject
from util.kms_helper import KMSHelper
from util.redshift_cluster import RedshiftCluster
from util.sql.ddl_generators import DatabaseDDLHelper, SchemaDDLHelper, TableDDLHelper, DDLTransformer
from util.sql.sql_text_helpers import SQLTextHelper, GET_SAFE_LOG_STRING
from global_config import config_parameters
from util.sql_queries import GET_DATABASE_NAME_OWNER_ACL, GET_SCHEMA_NAME_OWNER_ACL, GET_TABLE_NAME_OWNER_ACL


global resources
if 'resources' not in globals():
    resources = {}


class Resource(object):
    def __init__(self):
        self.create_sql = None
        self.created = False

    @abstractmethod
    def get_statement_to_retrieve_ddl_create_statement_text(self):
        pass

    def get_create_sql(self, generate=False):
        logging.debug('get_create_sql for {self}'.format(self=self))
        if generate:
            ddl_dict = self.get_cluster().get_query_full_result_as_list_of_dict(
                self.get_statement_to_retrieve_ddl_create_statement_text()
            )
            ddl = SQLTextHelper.get_sql_without_commands_newlines_and_whitespace(
                '\n'.join([r['ddl'] for r in ddl_dict])
            )
            self.create_sql = ddl
            return ddl
        else:
            if self.create_sql is not None:
                return self.create_sql
            else:
                raise Resource.CreateSQLNotSet('No create sql configured for resource {r}'.format(r=str(self)))

    def set_create_sql(self, create_sql):
        self.create_sql = create_sql

    @abstractmethod
    def get_cluster(self):
        pass

    def create(self, sql_text=None):
        """
        config_parameters has global_config_parameters.  If that is the case then they will be checked prior to creation
        :param sql_text:
        :return:
        """
        if self.created:
            return
        if hasattr(self, 'parent'):
            logging.debug('Object {self} has a parent that needs to be present.'.format(self=self))
            if not self.parent.is_present():
                self.parent.create(sql_text=sql_text)
            else:
                logging.debug('Parent of {self} is present.'.format(self=self))
        if isinstance(self, TableResource):
            if 'destinationTableAutoCreate' in config_parameters \
                    and not config_parameters['destinationTableAutoCreate']:
                raise Resource.AutoCreateRequiresConfigurationException(self, 'destinationTableAutoCreate')
        elif isinstance(self, SchemaResource):
            if 'destinationSchemaAutoCreate' in config_parameters \
                    and not config_parameters['destinationSchemaAutoCreate']:
                raise Resource.AutoCreateRequiresConfigurationException(self, 'destinationSchemaAutoCreate')
        elif isinstance(self, DBResource):
            if 'destinationDatabaseAutoCreate' in config_parameters \
                    and not config_parameters['destinationDatabaseAutoCreate']:
                raise Resource.AutoCreateRequiresConfigurationException(self, 'destinationDatabaseAutoCreate')

        logging.debug('Getting sql_text to create {self}.'.format(self=self))
        if sql_text is None:
            sql_text = self.get_create_sql()
        logging.info('Creating {self} with: '.format(self=self))
        logging.info('{sql_text}'.format(sql_text=sql_text))

        self.get_cluster().execute_update(sql_text)
        self.created = True

    @abstractmethod
    def drop(self):
        pass

    @abstractmethod
    def is_present(self, force_update=False):
        pass

    @abstractmethod
    def clone_structure_from(self, other):
        """
        Change DDL of self such that it has the same structure as other
        :param other: Resource implementation of same type a self
        :return:
        """
        pass

    class NotFound(Exception):
        def __init__(self, msg):
            self.msg = msg

        def __str__(self):
            s = super(Resource.NotFound, self).__str__() + '\n\t' + self.msg
            return s

    class CreateSQLNotSet(NotFound):
        def __init__(self, msg):
            super(Resource.CreateSQLNotSet, self).__init__(msg)

    class AutoCreateRequiresConfigurationException(Exception):
        def __init__(self, resource, configuration):
            super(Resource.AutoCreateRequiresConfigurationException, self).__init__()
            self.resource = resource
            self.configuration = configuration

        def __str__(self):
            return 'AutoCreateRequiresConfigurationException: Creating resource {r} requires {c}'.format(
                r=self.resource,
                c=self.configuration
            )


class DBResource(Resource):
    def clone_structure_from(self, other):
        ddl = other.get_create_sql(generate=True)
        if self.get_db() != other.get_db():
            ddl = DDLTransformer.get_ddl_for_different_database(
                ddl,
                new_database_name=self.get_db()
            )
        self.set_create_sql(ddl)

    def __init__(self, rs_cluster):
        """

        :param rs_cluster:
        members:
         - is_present_query: sql query that returns a single row if present otherwise <> 1 row
            this query can use parameters but they should be retrievable from the object as
            get_<parameter_name>()
        """
        Resource.__init__(self)
        self.commands = {}
        self._cluster = rs_cluster
        self.name = None
        self.owner = None
        self.acl = None
        self.get_name_owner_acl_sql = GET_DATABASE_NAME_OWNER_ACL

    def get_db(self):
        return self._cluster.get_db()

    def get_cluster(self):
        return self._cluster

    def set_cluster(self, cluster):
        self._cluster = cluster

    def __eq__(self, other):
        return type(self) == type(other) and \
               self.get_db() == other.get_db() and \
               self.get_cluster() == other.get_cluster()

    def __str__(self):
        return self.get_cluster().get_host() + ':' + str(self.get_db())

    def get_query_sql_text_with_parameters_replaced(self, sql_text):
        param_dict = {}
        for match_group in re.finditer(r'({[^}{]*})', sql_text):
            parameter_name = match_group.group().lstrip('{').rstrip('}')
            method = getattr(self, 'get_' + parameter_name)
            param_dict[parameter_name] = method()

        return sql_text.format(**param_dict)

    def retrieve_name_owner_acl_and_store_in_resource(self, force_update=False):
        if self.name is None or force_update:
            self.name = self.owner = self.acl = None
            get_details_sql = self.get_query_sql_text_with_parameters_replaced(self.get_name_owner_acl_sql)
            result = self.get_cluster().get_query_full_result_as_list_of_dict(get_details_sql)
            if len(result) == 0:
                raise Resource.NotFound('Resource {r} not found!'.format(r=str(self)))
            if len(result) > 1:
                raise Resource.NotFound('Multiple rows when retrieving Resource {r}'.format(r=str(self)))
            self.name = result[0]['name']
            self.owner = result[0]['owner']
            self.acl = result[0]['acl']

    def is_present(self, force_update=False):
        try:
            self.retrieve_name_owner_acl_and_store_in_resource(force_update=force_update)
        except Resource.NotFound:
            return False
        return self.name is not None

    def get_statement_to_retrieve_ddl_create_statement_text(self):
        return DatabaseDDLHelper().get_database_ddl_SQL(database_name=self.get_db())

    def drop(self):
        pass

    def run_command_against_resource(self, command, command_parameters=None):
        command_parameters = command_parameters or dict()
        command_parameters['cluster'] = self.get_cluster()
        command_to_execute = self.commands[command]
        if 'region' in command_parameters and command == 'copy_table' and command_parameters['region'] is not None:
            command_to_execute += " REGION '{region}' "
        update_sql_command = command_to_execute.format(**command_parameters)
        logging.info('Executing {command} against {resource}:'.format(command=command, resource=self))
        logging.info(GET_SAFE_LOG_STRING(update_sql_command))
        self.get_cluster().execute_update(update_sql_command)


class SchemaResource(DBResource, ChildObject):

    drop_schema_stmt = """DROP SCHEMA {schema_name}"""

    def __init__(self, rs_cluster, schema):
        DBResource.__init__(self, rs_cluster)
        self.commands['drop_schema'] = SchemaResource.drop_schema_stmt
        self.parent = DBResource(rs_cluster)
        self._schema = schema
        self.get_name_owner_acl_sql = GET_SCHEMA_NAME_OWNER_ACL

    def get_schema(self):
        return self._schema

    def set_schema(self, schema):
        self._schema = schema

    def __eq__(self, other):
        return type(self) == type(other) and \
               self.get_schema() == other.get_schema() and \
               DBResource.__eq__(self, other)

    def __str__(self):
        return super(SchemaResource, self).__str__() + '.' + str(self.get_schema())

    def get_statement_to_retrieve_ddl_create_statement_text(self):
        return SchemaDDLHelper().get_schema_ddl_SQL(schema_name=self.get_schema())

    def clone_structure_from(self, other):
        ddl = other.get_create_sql(generate=True)
        if self.get_schema() != other.get_schema():
            ddl = DDLTransformer.get_ddl_for_different_relation(
                ddl,
                new_schema_name=self.get_schema()
            )
        self.set_create_sql(ddl)

    def run_command_against_resource(self, command, command_parameters=None):
        command_parameters = command_parameters or dict()
        command_parameters['schema_name'] = self.get_schema()
        super(SchemaResource, self).run_command_against_resource(command, command_parameters)

    def drop(self):
        self.run_command_against_resource('drop_schema', {})


class TableResource(SchemaResource):
    unload_table_stmt = """unload ('SELECT {columns} FROM {schema_name}.{table_name}')
                     to '{dataStagingPath}.' credentials 
                     '{s3_access_credentials};master_symmetric_key={master_symmetric_key}'
                     manifest
                     encrypted
                     gzip
                     null as 'NULL_STRING__'
                     delimiter '^' addquotes escape allowoverwrite"""

    copy_table_stmt = """copy {schema_name}.{table_name} {columns}
                   from '{dataStagingPath}.manifest' credentials 
                   '{s3_access_credentials};master_symmetric_key={master_symmetric_key}'
                   manifest 
                   encrypted
                   gzip 
                   null as 'NULL_STRING__'
                   {explicit_ids}
                   dateformat 'auto'
                   timeformat 'auto'
                   delimiter '^' removequotes escape compupdate off """

    drop_table_stmt = """DROP TABLE {schema_name}.{table_name}"""

    def __init__(self, rs_cluster, schema, table):
        SchemaResource.__init__(self, rs_cluster, schema)
        self.parent = SchemaResource(rs_cluster, schema)
        self._table = table
        self.get_name_owner_acl_sql = GET_TABLE_NAME_OWNER_ACL
        self.commands['unload_table'] = TableResource.unload_table_stmt
        self.commands['copy_table'] = TableResource.copy_table_stmt
        self.commands['drop_table'] = TableResource.drop_table_stmt
        self.columns = None
        self.explicit_ids = False  # Only relevant to copy command

    def __eq__(self, other):
        return type(self) == type(other) and \
               self.get_table() == other.get_table() and \
               SchemaResource.__eq__(self, other)

    def __str__(self):
        return super(TableResource, self).__str__() + '.' + str(self.get_table())

    def get_statement_to_retrieve_ddl_create_statement_text(self):
        return TableDDLHelper().get_table_ddl_SQL(table_name=self.get_table(), schema_name=self.get_schema())

    def get_table(self):
        return self._table

    def set_table(self, table):
        self._table = table

    def run_command_against_resource(self, command, command_parameters=None):
        command_parameters = command_parameters or dict()
        command_parameters['table_name'] = self.get_table()
        super(TableResource, self).run_command_against_resource(command, command_parameters)

    def unload_data(self, s3_details):
        unload_parameters = {'s3_access_credentials': s3_details.access_credentials,
                             'master_symmetric_key': s3_details.symmetric_key,
                             'dataStagingPath': s3_details.dataStagingPath,
                             'region': s3_details.dataStagingRegion,
                             'columns': self.columns or '*'}
        self.run_command_against_resource('unload_table', unload_parameters)

    def copy_data(self, s3_details):
        copy_parameters = {'s3_access_credentials': s3_details.access_credentials,
                           'master_symmetric_key': s3_details.symmetric_key,
                           'dataStagingPath': s3_details.dataStagingPath,
                           'region': s3_details.dataStagingRegion,
                           'columns': self.columns or '',
                           'explicit_ids': 'explicit_ids' if self.explicit_ids else ''}

        self.run_command_against_resource('copy_table', copy_parameters)

    def clone_structure_from(self, other):
        ddl = other.get_create_sql(generate=True)
        if self.get_schema() != other.get_schema() or self.get_table() != other.get_table():
            ddl = DDLTransformer.get_ddl_for_different_relation(
                ddl,
                new_table_name=self.get_table(),
                new_schema_name=self.get_schema()
            )
        self.set_create_sql(ddl)
        self.parent.clone_structure_from(other.parent)

    def drop(self):
        self.run_command_against_resource('drop_table', {})

    def set_columns(self, columns):
        self.columns = columns

    def set_explicit_ids(self, explicit_ids):
        self.explicit_ids = explicit_ids

class ResourceFactory:
    def __init__(self):
        pass

    @staticmethod
    def get_source_resource_from_config_helper(config_helper, kms_region=None):
        cluster_dict = config_helper.config['unloadSource']
        return ResourceFactory.get_resource_from_dict(cluster_dict, kms_region)

    @staticmethod
    def get_table_resource_from_merging_2_resources(resource1, resource2):
        cluster = resource1.get_cluster()
        try:
            schema = resource1.get_schema()
        except AttributeError:
            logging.info('Destination did not have a schema declared fetching from resource2.')
            schema = resource2.get_schema()
            logging.info('Using resource2 schema {s}'.format(s=schema))
        try:
            table = resource1.get_table()
        except AttributeError:
            logging.info('Destination did not have a table declared fetching from resource2.')
            table = resource2.get_table()
            logging.info('Using resource2 table {t}'.format(t=table))
        return TableResource(cluster, schema, table)

    @staticmethod
    def get_target_resource_from_config_helper(config_helper, kms_region=None):
        cluster_dict = config_helper.config['copyTarget']
        return ResourceFactory.get_resource_from_dict(cluster_dict, kms_region)

    @staticmethod
    def get_cluster_from_cluster_dict(cluster_dict, kms_region):
        cluster = RedshiftCluster(cluster_dict['clusterEndpoint'])
        cluster.set_port(cluster_dict['clusterPort'])
        cluster.set_user(cluster_dict['connectUser'])
        cluster.set_host(cluster_dict['clusterEndpoint'])
        cluster.set_db(cluster_dict['db'])
        if 'connectPwd' in cluster_dict:
            if kms_region is None:
                kms_region = cluster.get_region_name()
            kms_helper = KMSHelper(kms_region)
            cluster.set_password(kms_helper.decrypt(cluster_dict['connectPwd']))

        cluster.set_user_auto_create(False)
        if 'userAutoCreate' in cluster_dict \
                and cluster_dict['userAutoCreate'].lower() == 'true':
            cluster.set_user_auto_create(True)

        cluster.user_db_groups = []
        if 'userDbGroups' in cluster_dict:
            cluster.set_user_db_groups(cluster_dict['userDbGroups'])
        return cluster

    @staticmethod
    def get_resource_from_dict(cluster_dict, kms_region):
        cluster = ResourceFactory.get_cluster_from_cluster_dict(cluster_dict, kms_region)
        if 'schemaName' not in cluster_dict:
            return DBResource(cluster)
        elif 'tableName' not in cluster_dict:
            return SchemaResource(cluster, cluster_dict['schemaName'])
        else:
            table_resource = TableResource(cluster, cluster_dict['schemaName'], cluster_dict['tableName'])
            if 'columns' in cluster_dict and cluster_dict['columns'].strip():
                table_resource.set_columns(cluster_dict['columns'].strip())
            if 'explicit_ids' in cluster_dict and cluster_dict['explicit_ids']:
                table_resource.set_explicit_ids(True)
            return table_resource
