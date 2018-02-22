import json
from unittest import TestCase

from botocore.stub import Stubber

from tests.cloudformation.get_stack_parameters import StackParametersBuilder


class StackParametersBuilderUnittests(TestCase):
    def setUp(self):
        self.resource_path = 'tests/resources/cloudformation/scenario1'
        self.spb = StackParametersBuilder(self.resource_path)
        self.set_cloudformation_stubber_for_client(self.spb.get_redshift_client())

    def assertParameterSetWithValue(self, parameter_key, parameter_value):
        self.assertEquals(parameter_value, self.spb.get_parameters_dict()[parameter_key])

    def test_is_s3_copy_unload_bucket_arn_parameter_present(self):
        parameter_key = 'S3CopyUnloadBucketArn'
        parameter_value = 'arn:aws:s3:::rscopyunloadtest3-s3copyunloadbucket-t60htt2ludj1'
        self.assertParameterSetWithValue(parameter_key, parameter_value)

    def test_is_s3_copy_unload_bucket_parameter_present(self):
        parameter_key = 'CopyUnloadBucket'
        parameter_value = 'rscopyunloadtest3-s3copyunloadbucket-t60htt2ludj1'
        self.assertParameterSetWithValue(parameter_key, parameter_value)

    def test_is_password_parameter_present(self):
        parameter_key = 'KMSEncryptedPassword'
        parameter_value = 'FAKEAHg+xk08AT09LQPwWlubuZPK9qWqnleTQogT9t6BqUCoDQH8RE0SPOUPtzkccSRfqMQiAAAAcjBwBgkqhkiG9w0BBwagYzBhAgEAMFwGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMizQ+49C8jMM5hs4pAgEQgC/OtruhpIgbPfDn3i3CuMT/f/spg0oVZhFiCZhDphCFOWAsEplz8btht8Datvalng=='
        self.assertParameterSetWithValue(parameter_key, parameter_value)

    def test_is_source_cluster_name_present(self):
        parameter_key = 'SourceClusterName'
        parameter_value = 'rscopyunloadtest3-redshiftclustersource-1so4t2ip0ei3a'
        self.assertParameterSetWithValue(parameter_key, parameter_value)

    def test_is_source_cluster_fqn_present(self):
        parameter_key = 'SourceClusterEndpointAddress'
        parameter_value = 'rscopyunloadtest3-redshiftclustersource-1so4t2ip0ei3a.cmycczwsval6.eu-west-1.redshift.amazonaws.com'
        self.assertParameterSetWithValue(parameter_key, parameter_value)

    def test_is_source_cluster_port_present(self):
        parameter_key = 'SourceClusterEndpointPort'
        parameter_value = '5439'
        self.assertParameterSetWithValue(parameter_key, parameter_value)

    def test_is_target_cluster_db_present(self):
        parameter_key = 'TargetClusterDBName'
        parameter_value = 'dev'
        self.assertParameterSetWithValue(parameter_key, parameter_value)

    def test_is_copy_role_arn_present(self):
        parameter_key = 'S3CopyRole'
        parameter_value = 'arn:aws:iam::012345678910:role/rscopyunloadtest3-S3Role-WH7U48ALN2A3'
        self.assertParameterSetWithValue(parameter_key, parameter_value)

    def test_is_region_present(self):
        parameter_key = 'Region'
        parameter_value = 'eu-west-1'
        self.assertParameterSetWithValue(parameter_key, parameter_value)

    def set_cloudformation_stubber_for_client(self, redshift_client):
        stubber = Stubber(redshift_client)
        with open(self.resource_path+'/DescribeSourceClusterResponse.json') as describe_source_response:
            describe_source_cluster_response = json.load(describe_source_response)
        expected_source_params = {'ClusterIdentifier': 'rscopyunloadtest3-redshiftclustersource-1so4t2ip0ei3a'}
        stubber.add_response('describe_clusters', describe_source_cluster_response, expected_source_params)

        with open(self.resource_path+'/DescribeTargetClusterResponse.json') as describe_target_response:
            describe_target_cluster_response = json.load(describe_target_response)
        expected_target_params = {'ClusterIdentifier': 'rscopyunloadtest3-redshiftclustertarget-oaw35zvu02h'}
        stubber.add_response('describe_clusters', describe_target_cluster_response, expected_target_params)
        stubber.activate()

