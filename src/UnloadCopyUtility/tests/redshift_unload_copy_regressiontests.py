#!/usr/bin/env python
"""
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

Unittests can only be ran in python3 due to dependencies

"""
from unittest import TestCase
from unittest.mock import MagicMock
import redshift_unload_copy
import boto3

import util.redshift_cluster

import util.kms_helper
import util.s3_utils


class TestRedshiftUnloadCopy(TestCase):
    s3_test_config = 's3://support-peter-ie/config_test.json'
    test_local_config = 'example/config_test.json'
    bucket_name = 'support-peter-ie'
    s3_path_prefix = 'tests'

    def get_s3_key_for_object_name(self, object_name):
        return self.s3_path_prefix + '/' + object_name

    def setUp(self):
        redshift_unload_copy.conn_to_rs = MagicMock(return_value=MagicMock())
        redshift_unload_copy.copy_data = MagicMock(return_value=MagicMock())
        redshift_unload_copy.unload_data = MagicMock(return_value=MagicMock())

    def test_config_local_is_same_as_using_S3(self):
        """
        Mostly to test S3 boto to boto3 change
        :return:
        """
        s3_helper = util.s3_utils.S3Helper('eu-west-1')
        s3_config = redshift_unload_copy.ConfigHelper(self.s3_test_config, s3_helper).config
        local_config = redshift_unload_copy.ConfigHelper(self.test_local_config).config
        self.assertEqual(s3_config, local_config)

    def test_decoding_to_verify_kms_client(self):
        kms_helper = util.kms_helper.KMSHelper('us-east-1')
        encoded = "AQICAHjX2Xlvwj8LO0wam2pvdxf/icSW7G30w7SjtJA5higfdwG7KjYEDZ+jXA6QTjJY9PlDAAAAZTBjBgkqhkiG9w0BBwa" \
                  "gVjBUAgEAME8GCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMx+xGf9Ys58uvtfl5AgEQgCILmeoTmmo+Sh1cFgjyqNrySD" \
                  "fQgPYsEYjDTe6OHT5Z0eop"
        decoded_kms = kms_helper.decrypt(encoded)
        self.assertEqual("testing".encode('utf-8'), decoded_kms)

    class TableResourceMock:
        def __init__(self, rs_cluster, schema, table):
            self._cluster = rs_cluster
            self._schema = schema
            self._table = table
            self.dataStagingPath = None

        def get_db(self):
            return self._cluster.get_db()

        def get_schema(self):
            return self._schema

        def get_table(self):
            return self._table

        def unload_data(self, s3_details):
            s3_parts = util.s3_utils.S3Helper.tokenize_s3_path(s3_details.dataStagingPath)
            s3_client = boto3.client('s3', 'eu-west-1')
            s3_client.put_object(Body='content1'.encode('utf-8'),
                                 Bucket=s3_parts[0],
                                 Key=s3_parts[1] + 'test_file_1')
            s3_client.put_object(Body='content2'.encode('utf-8'),
                                 Bucket=s3_parts[0],
                                 Key=s3_parts[1] + 'test_file_2')
            self.dataStagingPath = s3_details.dataStagingPath

        def copy_data(self, s3_details):
            pass
