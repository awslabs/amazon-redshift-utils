#!/usr/bin/env python
"""
Unittests can only be ran in python3 due to dependencies

* Copyright 2017, Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Amazon Software License (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* http://aws.amazon.com/asl/
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
"""
from unittest import TestCase
from unittest.mock import MagicMock
from util.sql.sql_text_helpers import GET_SAFE_LOG_STRING, SQLTextHelper
from util.sql.ddl_generators import DDLTransformer
from util.redshift_cluster import RedshiftCluster
from util.sql_queries import GET_DATABASE_NAME_OWNER_ACL
from util.resources import DBResource
import redshift_unload_copy
import datetime
import time
import pytz


class TestRedshiftUnloadCopy(TestCase):
    bucket_name = 'pvb-cloud-storage'
    dir_key = 'Pictures'
    object_key = 'index.html'

    def setUp(self):
        connection_mock = MagicMock()
        redshift_unload_copy.conn_to_rs = MagicMock(return_value=connection_mock)

    def test_region_extract_example_url(self):
        example_url = 'my-cluster.a1bcdefghijk.eu-west-1.redshift.amazonaws.com'
        rs_cluster = RedshiftCluster(example_url)
        self.assertEqual('eu-west-1', rs_cluster.get_region_name())

    def test_region_extract_example_bad_cased_url(self):
        example_url = 'my-cluSter.a1bcdefghijk.eU-west-1.redshift.amazonaws.com'
        rs_cluster = RedshiftCluster(example_url)
        self.assertEqual('eu-west-1', rs_cluster.get_region_name())

    def test_identifier_extract_example_url(self):
        example_url = 'my-cluster.a1bcdefghijk.eu-west-1.redshift.amazonaws.com'
        rs_cluster = RedshiftCluster(example_url)
        self.assertEqual('my-cluster', rs_cluster.get_cluster_identifier())

    def test_identifier_extract_example_bad_cased_url(self):
        example_url = 'my-cluSter.a1bcdefghijk.eU-west-1.redshift.amazonaws.com'
        rs_cluster = RedshiftCluster(example_url)
        self.assertEqual('my-cluster', rs_cluster.get_cluster_identifier())

    def test_extract_first_double_quoted_identifier_from_string(self):
        test_string = 'CREATE DATABASE "test"'
        self.assertEquals('"test"', SQLTextHelper.get_first_double_quoted_identifier(test_string))

    def test_extract_first_double_quoted_identifier_from_string_with_escaped_double_quotes(self):
        test_string = 'CREATE DATABASE "te""st"'
        self.assertEquals('"te""st"', SQLTextHelper.get_first_double_quoted_identifier(test_string))

    def test_extract_first_double_quoted_identifier_from_string_with_escaped_double_quotes_at_end(self):
        test_string = 'CREATE DATABASE "test"""'
        self.assertEquals('"test"""', SQLTextHelper.get_first_double_quoted_identifier(test_string))

    def test_get_ddl_for_different_database_name(self):
        ddl_string = '  CREATE DATABASE {db}   WITH CONNECTION LIMIT UNLIMITED;'
        test_string = ddl_string.format(db='"my""data"')
        new_db_name = '"ab"""'
        expected_string = ddl_string.format(db=new_db_name)
        self.assertEquals(expected_string, DDLTransformer.get_ddl_for_different_database(test_string, new_db_name))

    def test_temporary_credential_expiration_predicate(self):
        example_url = 'my-cluSter.a1bcdefghijk.eU-west-1.redshift.amazonaws.com'
        rs_cluster = RedshiftCluster(example_url)
        rs_cluster.has_temporary_password = True
        expiration_time = (datetime.datetime.now(pytz.utc) + datetime.timedelta(minutes=1, milliseconds=300))
        rs_cluster._user_creds_expiration = expiration_time
        self.assertFalse(rs_cluster.is_temporary_credential_expired())
        time.sleep(0.4)
        self.assertTrue(rs_cluster.is_temporary_credential_expired())

    def test_construction_of_get_query_sql_text_with_parameters_replaced(self):
        cluster = RedshiftCluster(cluster_endpoint='test')
        db_name = 'testdb'
        cluster.set_db(db_name)
        sql = DBResource(rs_cluster=cluster).get_query_sql_text_with_parameters_replaced(GET_DATABASE_NAME_OWNER_ACL)
        expected_sql = GET_DATABASE_NAME_OWNER_ACL.format(db=db_name)
        self.assertEquals(sql, expected_sql)

    def test_redaction_of_sensitive_information_master_symmetric_key_with_single_quote(self):
        input_sql = """unload ('SELECT * FROM ssb.dwdate')
                     to 's3://unload-copy-t4f8cltb-s3copyunloadbucket-i6exicqitkr8/scenario004/2017-12-22_16:54:39/dev.ssb.dwdate.' credentials 
                     'aws_iam_role=arn:aws:iam::012345678910:role/unload-copy-t4f8cLtB-S3Role-S1R76KW2VMR;master_symmetric_key=fWyHGBEtRyYnjnzRpPDcXr2yuQbEfaWqMhFQuq11111='
                     manifest encrypted gzip delimiter '^' addquotes escape allowoverwrite"""
        expected_sql = """unload ('SELECT * FROM ssb.dwdate')
                     to 's3://unload-copy-t4f8cltb-s3copyunloadbucket-i6exicqitkr8/scenario004/2017-12-22_16:54:39/dev.ssb.dwdate.' credentials 
                     'aws_iam_role=arn:aws:iam::012345678910:role/unload-copy-t4f8cLtB-S3Role-S1R76KW2VMR;master_symmetric_key=REDACTED'
                     manifest encrypted gzip delimiter '^' addquotes escape allowoverwrite"""
        self.assertEquals(GET_SAFE_LOG_STRING(input_sql), expected_sql)

    def test_redaction_of_sensitive_information_master_symmetric_key_copy_with_semicolon(self):
        input_sql = """copy public.dwdate
                   from 's3://unload-copy-t4f8cltb-s3copyunloadbucket-i6exicqitkr8/scenario004/2017-12-22_16:54:39/dev.ssb.dwdate.manifest' credentials 
                   'master_symmetric_key=fWyHGBEtRyYnjnzRpPDcXr2yuQbEfaWqMhFQuq9GeNM=;aws_iam_role=arn:aws:iam::012345678910:role/unload-copy-t4f8cLtB-S3Role-S1R76KW2VMR'
                   manifest 
                   encrypted
                   gzip
                   delimiter '^' removequotes escape compupdate off REGION 'eu-west-1' """
        expected_sql = """copy public.dwdate
                   from 's3://unload-copy-t4f8cltb-s3copyunloadbucket-i6exicqitkr8/scenario004/2017-12-22_16:54:39/dev.ssb.dwdate.manifest' credentials 
                   'master_symmetric_key=REDACTED;aws_iam_role=arn:aws:iam::012345678910:role/unload-copy-t4f8cLtB-S3Role-S1R76KW2VMR'
                   manifest 
                   encrypted
                   gzip
                   delimiter '^' removequotes escape compupdate off REGION 'eu-west-1' """
        self.assertEquals(GET_SAFE_LOG_STRING(input_sql), expected_sql)

    def test_redaction_of_sensitive_information_secret_access_key_keyword(self):
        input_sql = """copy table-name
from 's3://objectpath'
access_key_id '<temporary-access-key-id>'
secret_access_key '<temporary-secret-access-key>'
token '<temporary-token>';"""
        expected_sql = """copy table-name
from 's3://objectpath'
access_key_id '<temporary-access-key-id>'
secret_access_key 'REDACTED'
token '<temporary-token>';"""
        self.assertEquals(GET_SAFE_LOG_STRING(input_sql), expected_sql)

    def test_redaction_of_sensitive_information_secret_access_key_keyword_UPPERCASE(self):
        """
        SQL is cae insensitive code needs to be as well case insensitive
        :return:
        """
        input_sql = """copy table-name
from 's3://objectpath'
access_key_id '<temporary-access-key-id>'
SECRET_ACCESS_KEY '<temporary-secret-access-key>'
token '<temporary-token>';"""
        expected_sql = """copy table-name
from 's3://objectpath'
access_key_id '<temporary-access-key-id>'
secret_access_key 'REDACTED'
token '<temporary-token>';"""
        self.assertEquals(GET_SAFE_LOG_STRING(input_sql), expected_sql)

    def test_redaction_password_in_connect_string(self):
        input_string = "host=localhost port=5439 dbname=dev user=master password=MyS3cr3tPass.word option1"
        expected_string = "host=localhost port=5439 dbname=dev user=master password=REDACTED option1"
        self.assertEquals(GET_SAFE_LOG_STRING(input_string), expected_string)
