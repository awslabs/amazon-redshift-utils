from unittest import TestCase

from util.sql.ddl_generators import SQLTextHelper, DDLTransformer


class TableDDLHelperTests(TestCase):
    def test_remove_simple_block(self):
        input_sql = """/*  co
               mm
                 ents
           can make -- things more difficult */ select 1;"""
        expected_sql = " select 1;"
        self.assertEquals(expected_sql, SQLTextHelper.get_sql_without_comments(input_sql_text=input_sql))

    def test_remove_simple_line_comment(self):
        input_sql = """-- things more difficult
--More SQL        
 select 1;"""
        expected_sql = " select 1;"
        self.assertEquals(expected_sql, SQLTextHelper.get_sql_without_comments(input_sql_text=input_sql))

    def test_remove_line_comment_must_not_influence_string_literals(self):
        input_sql = """select '--DROP TABLE "';"""
        expected_sql = input_sql
        self.assertEquals(expected_sql, SQLTextHelper.get_sql_without_comments(input_sql_text=input_sql))

    def test_remove_2_line_comments(self):
        input_sql = """-- things more difficult
--More SQL        
 select 1
union all
 select 2;"""
        expected_sql = """ select 1
union all
 select 2;"""
        self.assertEquals(expected_sql, SQLTextHelper.get_sql_without_comments(input_sql_text=input_sql))

    def test_remove_2_line_comments_and_white_spaces(self):
        input_sql = """-- things more difficult
--More SQL        
 select 1
union all
 select 2;"""
        expected_sql = """select 1 union all select 2;"""
        result_sql = SQLTextHelper.get_sql_without_commands_newlines_and_whitespace(input_sql_text=input_sql)
        self.assertEquals(expected_sql, result_sql)

    def test_remove_2_line_comments_and_white_spaces_string_literals(self):
        input_sql = """-- things more difficult
--More SQL        
 select '1   2'
union all
 select 2;"""
        expected_sql = """select '1   2' union all select 2;"""
        result_sql = SQLTextHelper.get_sql_without_commands_newlines_and_whitespace(input_sql_text=input_sql)
        self.assertEquals(expected_sql, result_sql)

    def test_transform_table_ddl(self):
        ddl = 'CREATE TABLE IF NOT EXISTS "public"."test_""_quote" ( "id""ea" INTEGER ENCODE lzo)DISTSTYLE EVEN;'
        transformed_ddl = DDLTransformer.get_ddl_for_different_relation(ddl, new_table_name='b')
        expected = 'CREATE TABLE IF NOT EXISTS public.b( "id""ea" INTEGER ENCODE lzo)DISTSTYLE EVEN;'
        self.assertEquals(transformed_ddl, expected)

    def test_transform_table_ddl_without_double_quote_around_table_name(self):
        ddl = 'CREATE TABLE IF NOT EXISTS public."test_""_quote" ( "id""ea" INTEGER ENCODE lzo)DISTSTYLE EVEN;'
        transformed_ddl = DDLTransformer.get_ddl_for_different_relation(ddl, new_table_name='b')
        expected = 'CREATE TABLE IF NOT EXISTS public.b( "id""ea" INTEGER ENCODE lzo)DISTSTYLE EVEN;'
        self.assertEquals(transformed_ddl, expected)

    def test_transform_table_ddl_without_double_quote_around_schema_name(self):
        ddl = 'CREATE TABLE IF NOT EXISTS "pu""blic".test ( "id""ea" INTEGER ENCODE lzo)DISTSTYLE EVEN;'
        transformed_ddl = DDLTransformer.get_ddl_for_different_relation(ddl, new_table_name='b')
        expected = 'CREATE TABLE IF NOT EXISTS "pu""blic".b( "id""ea" INTEGER ENCODE lzo)DISTSTYLE EVEN;'
        self.assertEquals(transformed_ddl, expected)

    def test_transform_table_ddl_without_double_quote_around_schema_nor_table_name(self):
        ddl = 'CREATE TABLE IF NOT EXISTS public.test ( "id""ea" INTEGER ENCODE lzo)DISTSTYLE EVEN;'
        transformed_ddl = DDLTransformer.get_ddl_for_different_relation(ddl, new_table_name='b')
        expected = 'CREATE TABLE IF NOT EXISTS public.b( "id""ea" INTEGER ENCODE lzo)DISTSTYLE EVEN;'
        self.assertEquals(transformed_ddl, expected)
