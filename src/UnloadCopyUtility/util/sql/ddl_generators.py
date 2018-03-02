from util.sql.sql_text_helpers import SQLTextHelper
from global_config import config_parameters

import re
import os
import logging
from abc import abstractmethod


class DDLHelper:
    def __init__(self, path_to_v_generate, view_start):
        logging.debug('From {cwd} open v_generate {path}'.format(
            cwd=os.getcwd(),
            path=path_to_v_generate
        ))
        with open(path_to_v_generate, 'r') as v_generate:
            self.view_sql = v_generate.read()
        self.view_sql = SQLTextHelper.get_sql_without_commands_newlines_and_whitespace(self.view_sql)
        if self.view_sql.startswith(view_start):
            self.view_query_sql = self.view_sql[len(view_start):]
        self.filter_sql = ''

    def get_sql(self):
        sql_to_get_all_ddl = SQLTextHelper.remove_trailing_semicolon(self.view_query_sql)
        return 'SELECT * FROM (' + sql_to_get_all_ddl + ') ' + self.filter_sql + ';'

    def add_filters(self, filters):
        filter_list = []
        for filter_name in filters.keys():
            if type(filters[filter_name]) in (type(int),  type(float)):
                filter_list.append("{key}={value}".format(key=filter_name, value=filters[filter_name]))
            else:
                filter_list.append("{key}='{value}'".format(key=filter_name, value=filters[filter_name]))
        if len(filter_list) > 0:
            self.filter_sql = ' WHERE '
            self.filter_sql += ' AND '.join(filter_list)
        else:
            self.filter_sql = ''


class DatabaseDDLHelper(DDLHelper):
    def __init__(self):
        view_start = 'CREATE OR REPLACE VIEW admin.v_generate_database_ddl AS '
        DDLHelper.__init__(self, config_parameters['locationGenerateDatabaseDDLView'], view_start)

    # noinspection PyPep8Naming
    def get_database_ddl_SQL(self, database_name=None):
        filters = {}
        if database_name is not None:
            # noinspection SpellCheckingInspection
            filters['datname'] = database_name
        self.add_filters(filters)
        return self.get_sql()


class SchemaDDLHelper(DDLHelper):
    def __init__(self):
        view_start = 'CREATE OR REPLACE VIEW admin.v_generate_schema_ddl AS '
        DDLHelper.__init__(self, config_parameters['locationGenerateSchemaDDLView'], view_start)

    # noinspection PyPep8Naming
    def get_schema_ddl_SQL(self, schema_name=None):
        filters = {}
        if schema_name is not None:
            filters['schemaname'] = schema_name
        self.add_filters(filters)
        return self.get_sql()


class TableDDLHelper(DDLHelper):
    def __init__(self):
        view_start = 'CREATE OR REPLACE VIEW admin.v_generate_tbl_ddl AS '
        DDLHelper.__init__(self, config_parameters['locationGenerateTableDDLView'], view_start)

    # noinspection PyPep8Naming
    def get_table_ddl_SQL(self, table_name=None, schema_name=None):
        filters = {}
        if table_name is not None:
            filters['tablename'] = table_name
        if schema_name is not None:
            filters['schemaname'] = schema_name
        self.add_filters(filters)
        return self.get_sql()


class DDLTransformer:
    def __init__(self):
        pass

    @staticmethod
    def get_ddl_for_different_relation(ddl, new_table_name=None, new_schema_name=None):
        logging.debug('Transforming ddl: {ddl}'.format(ddl=ddl))
        clean_ddl = SQLTextHelper.get_sql_without_commands_newlines_and_whitespace(ddl)
        if clean_ddl.upper().startswith('CREATE TABLE IF NOT EXISTS '):
            return TableDDLTransformer.get_create_table_ddl_for_different_relation(
                clean_ddl,
                new_table_name=new_table_name,
                new_schema_name=new_schema_name
            )
        elif clean_ddl.upper().startswith('CREATE SCHEMA '):
            return SchemaDDLTransformer.get_create_schema_ddl_for_different_relation(
                clean_ddl,
                new_schema_name=new_schema_name
            )
        raise DDLTransformer.UnsupportedDDLForTransformationException(clean_ddl)

    @abstractmethod
    def has_table_in_ddl(self):
        pass

    @abstractmethod
    def has_schema_in_ddl(self):
        pass

    def get_relation_regex_string(self, quoted_schema=True, quoted_table=True):
        regex_string = r''
        if self.has_schema_in_ddl():
            if not quoted_schema:
                regex_schema = r'(?P<schema_name>.*)'
            else:
                regex_schema = r'(?P<schema_name>".*")'
            regex_string += regex_schema

        if self.has_table_in_ddl():
            if not quoted_table:
                regex_table = r'(?P<table_name>.*)'
            else:
                regex_table = r'(?P<table_name>".*")'
            regex_string += r'\.' + regex_table
        return regex_string

    @staticmethod
    def get_database_name_out_of_ddl(ddl):
        clean_ddl = SQLTextHelper.get_sql_without_commands_newlines_and_whitespace(ddl)
        if clean_ddl.upper().startswith('CREATE DATABASE "'):
            return SQLTextHelper.get_first_double_quoted_identifier(clean_ddl)
        elif clean_ddl.upper().startswith('CREATE DATABASE '):
            database_name = clean_ddl.split(' ')[2]
        else:
            raise DDLTransformer.UnsupportedDDLForTransformationException(clean_ddl)
        return database_name

    @staticmethod
    def get_ddl_for_different_database(ddl, new_database_name=None):
        old_database_name = DDLTransformer.get_database_name_out_of_ddl(ddl)
        return ddl.replace(old_database_name, new_database_name)

    def get_ddl_for_different_relation_where_relation_just_before_round_bracket(
            self,
            ddl,
            new_table_name=None,
            new_schema_name=None):
        """
        Get ddl but adapt it to create a relation with different name but same structure
        :param ddl:  ddl from admin.v_generate_tbl_ddl view
        :param new_table_name: if None don't replace table_name
        :param new_schema_name: if None don't replace schema_name
        :return:
        """
        relation_specification = 'Unknown'
        try:
            round_bracket_separated_parts = ddl.split('(')
            first_round_bracket_part = round_bracket_separated_parts[0].rstrip()
            space_separated_parts = first_round_bracket_part.split(' ')
            relation_specification = space_separated_parts[-1]
            relation_regex = self.get_relation_regex_string(
                    quoted_schema=relation_specification.startswith('"'),
                    quoted_table=relation_specification.endswith('"')
                )
            match_dict = re.match(relation_regex, relation_specification).groupdict()
            relation_specification = ''

            if self.has_schema_in_ddl():
                original_schema_name = SQLTextHelper.quote_unindent(match_dict['schema_name'])
                new_schema_name = new_schema_name or original_schema_name
                relation_specification += '{schema}'.format(schema=SQLTextHelper.quote_indent(new_schema_name))
            if self.has_table_in_ddl():
                original_table_name = SQLTextHelper.quote_unindent(match_dict['table_name'])
                new_table_name = new_table_name or original_table_name
                relation_specification += '.{table}'.format(table=SQLTextHelper.quote_indent(new_table_name))

            space_separated_parts[-1] = relation_specification
            round_bracket_separated_parts[0] = ' '.join(space_separated_parts)
            new_ddl = '('.join(round_bracket_separated_parts)
            return new_ddl
        except:
            logging.debug('Clean ddl: {ddl}\nRelation name: {rel_name}'.format(
                ddl=ddl,
                rel_name=relation_specification
            ))
            raise DDLTransformer.InvalidDDLSQLException(ddl)

    class UnsupportedDDLForTransformationException(Exception):
        def __init__(self, ddl):
            super(DDLTransformer.UnsupportedDDLForTransformationException, self).__init__()
            self.ddl = ddl

    class InvalidDDLSQLException(Exception):
        def __init__(self, ddl):
            super(TableDDLTransformer.InvalidDDLSQLException, self).__init__()
            self.ddl = ddl


class SchemaDDLTransformer(DDLTransformer):
    def __init__(self):
        DDLTransformer.__init__(self)

    @staticmethod
    def get_create_schema_ddl_for_different_relation(ddl, new_schema_name=None):
        return SchemaDDLTransformer().get_ddl_for_different_relation_where_relation_just_before_round_bracket(
            ddl,
            new_schema_name=new_schema_name
        )

    def has_schema_in_ddl(self):
        return False

    def has_table_in_ddl(self):
        return False


class TableDDLTransformer(DDLTransformer):
    def has_schema_in_ddl(self):
        return True

    def has_table_in_ddl(self):
        return True

    def __init__(self):
        DDLTransformer.__init__(self)

    @staticmethod
    def get_create_table_ddl_for_different_relation(ddl, new_table_name=None, new_schema_name=None):
        """
        Get ddl but adapt it to create a relation with different name but same structure
        :param ddl:  ddl from admin.v_generate_tbl_ddl view
        :param new_table_name: if None don't replace table_name
        :param new_schema_name: if None don't replace schema_name
        :return:
        """
        return TableDDLTransformer().get_ddl_for_different_relation_where_relation_just_before_round_bracket(
            ddl,
            new_table_name=new_table_name,
            new_schema_name=new_schema_name
        )
