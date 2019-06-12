import base64
import datetime
import json
import logging

import boto3

from util.kms_helper import KMSHelper


class S3Helper:
    def __init__(self, region_name):
        self.region_name = region_name
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.config = None

    def get_json_config_as_dict(self, s3_url):
        if s3_url.startswith("s3://"):
            # download the configuration from s3
            (config_bucket_name, config_key) = S3Helper.tokenize_s3_path(s3_url)

            response = self.s3_client.get_object(Bucket=config_bucket_name,
                                                 Key=config_key)  # Throws NoSuchKey exception if no config
            config_contents = response['Body'].read(1024 * 1024).decode('utf-8')  # Read maximum 1MB

            config = json.loads(config_contents)
        else:
            with open(s3_url) as f:
                config = json.load(f)

        self.config = config
        return config

    def delete_list_of_keys_from_bucket(self, keys_to_delete, bucket_name):
        """
        This is a wrapper around delete_objects for the boto3 S3 client.
        This call only allows a maximum of 1000 keys otherwise an Exception will be thrown
        :param keys_to_delete:
        :param bucket_name:
        :return:
        """
        if len(keys_to_delete) > 1000:
            raise Exception('Batch delete only supports a maximum of 1000 keys at a time')

        object_list = []
        for key in keys_to_delete:
            object_list.append({'Key': key})
        self.s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': object_list})

    def delete_s3_prefix(self, s3_details):
        print("Cleaning up S3 Data Staging Location %s" % s3_details.dataStagingPath)
        (stagingBucket, stagingPrefix) = S3Helper.tokenize_s3_path(s3_details.dataStagingRoot)

        objects = self.s3_client.list_objects_v2(Bucket=stagingBucket, Prefix=stagingPrefix)
        if objects['KeyCount'] > 0:
            keys_to_delete = []
            key_number = 1
            for s3_object in objects['Contents']:
                if (key_number % 1000) == 0:
                    self.delete_list_of_keys_from_bucket(keys_to_delete, stagingBucket)
                    keys_to_delete = []
                keys_to_delete.append(s3_object['Key'])
            self.delete_list_of_keys_from_bucket(keys_to_delete, stagingBucket)

    @staticmethod
    def tokenize_s3_path(path):
        path_elements = path.split('/')
        bucket_name = path_elements[2]
        prefix = "/".join(path_elements[3:])

        return bucket_name, prefix


class S3AccessCredentialsKey:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def __str__(self):
        return 'aws_access_key_id={key};aws_secret_access_key={secret}'.format(
            key=self.aws_access_key_id, secret=self.aws_secret_access_key
        )


class S3AccessCredentialsRole:
    def __init__(self, aws_iam_role):
        self.aws_iam_role = aws_iam_role

    def __str__(self):
        return 'aws_iam_role={role}'.format(role=self.aws_iam_role)


class S3Details:
    class NoS3CredentialsFoundException(Exception):
        def __init__(self, *args):
            super(S3Details.NoS3CredentialsFoundException, self).__init__(*args)

    class NoS3StagingInformationFoundException(Exception):
        def __init__(self, *args):
            super(S3Details.NoS3StagingInformationFoundException, self).__init__(*args)

    class S3StagingPathMustStartWithS3(Exception):
        def __init__(self, *args):
            msg = 's3Staging.path must be a path to S3, so start with s3://'
            super(S3Details.S3StagingPathMustStartWithS3, self).__init__(msg, *args)

    def __init__(self, config_helper, source_table, encryption_key_id=None):
        if 's3Staging' not in config_helper.config:
            raise S3Details.NoS3StagingInformationFoundException()
        else:
            s3_staging_conf = config_helper.config['s3Staging']
            if 'region' in s3_staging_conf:
                self.dataStagingRegion = s3_staging_conf['region']
            else:
                logging.warning('No region in s3_staging_conf')
                self.dataStagingRegion = None

            if 'deleteOnSuccess' in s3_staging_conf \
                    and s3_staging_conf['deleteOnSuccess'].lower() == 'true':
                self.deleteOnSuccess = True
            else:
                self.deleteOnSuccess = False

            if 'path' in s3_staging_conf:
                # datetime alias for operations
                self.nowString = "{:%Y-%m-%d_%H:%M:%S}".format(datetime.datetime.now())
                self.dataStagingRoot = "{s3_stage_path}/{timestamp}-{table_name}/".format(
                    s3_stage_path=s3_staging_conf['path'].rstrip("/"),
                    timestamp=self.nowString,
                    table_name=source_table.get_table()
                )
                self.dataStagingPath = "{root}{db_name}.{schema_name}.{table_name}".format(
                    root=self.dataStagingRoot,
                    db_name=source_table.get_db(),
                    schema_name=source_table.get_schema(),
                    table_name=source_table.get_table())

            if not self.dataStagingPath or not self.dataStagingPath.startswith("s3://"):
                raise S3Details.S3StagingPathMustStartWithS3

            if 'aws_iam_role' in s3_staging_conf:
                role = s3_staging_conf['aws_iam_role']
                self.access_credentials = S3AccessCredentialsRole(role)
            elif 'aws_access_key_id' in s3_staging_conf and 'aws_secret_access_key' in s3_staging_conf:
                kms_helper = KMSHelper(config_helper.s3_helper.region_name)
                key_id = kms_helper.decrypt(s3_staging_conf['aws_access_key_id']).decode('utf-8')
                secret_key = kms_helper.decrypt(s3_staging_conf['aws_secret_access_key']).decode('utf-8')
                self.access_credentials = S3AccessCredentialsKey(key_id, secret_key)
            else:
                raise(S3Details.NoS3CredentialsFoundException())

            use_kms = True
            if 'kmsGeneratedKey' in s3_staging_conf:
                if s3_staging_conf['kmsGeneratedKey'].lower() == 'false':
                    use_kms = False

            if use_kms:
                kms_helper = KMSHelper(config_helper.s3_helper.region_name)
                self.symmetric_key = kms_helper.generate_base64_encoded_data_key(encryption_key_id)
            else:
                self.symmetric_key = base64.b64encode(KMSHelper.generate_data_key_without_kms())
            # noinspection PyBroadException
            try:
                self.symmetric_key = self.symmetric_key.decode('utf-8')
            except:
                logging.debug('Exception converting string can be ignored, likely Python2 so already a string.')
