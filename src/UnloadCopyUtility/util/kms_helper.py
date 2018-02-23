import base64
import logging
import sys

import boto3


class KMSHelper:
    def __init__(self, region_name):
        self.kms_client = boto3.client('kms', region_name=region_name)

    def generate_base64_encoded_data_key(self, encryption_key_id, key_spec="AES_256"):
        data_key = self.kms_client.generate_data_key(KeyId=encryption_key_id, KeySpec=key_spec)
        return base64.b64encode(data_key['Plaintext']).decode('utf-8')

    def decrypt(self, b64_encoded_value):
        return self.kms_client.decrypt(CiphertextBlob=base64.b64decode(b64_encoded_value))['Plaintext']

    @staticmethod
    def generate_data_key_without_kms():
        if (sys.version_info[0] == 3 and sys.version_info[1] >= 6) or sys.version_info[0] > 3:
            # Use new secrets module https://docs.python.org/3/library/secrets.html
            import secrets
            return secrets.token_bytes(int(256 / 8))
        else:
            # Legacy code to generate random value
            try:
                # noinspection PyUnresolvedReferences
                from Crypto import Random
            except ImportError:
                pycrypto_explanation = """
                For generating a secure Random sequence without KMS, pycrypto is used in case you use Python <3.6 .
                This does not seem to be available on your system and therefore execution needs to be aborted.
                In order to not use KMS in case of a Python to setup you must install the pycrypto library.
                For example using `pip install pycrypto`

                It requires to compile code in C so there are some requirements you need to satisfy. For more info
                check out the installation section in the source documentation at https://pypi.python.org/pypi/pycrypto

                Alternatively you could use KMS by setting s3Staging -> kmsGeneratedKey to True in the config file
                In that case make sure to generate a key using ./createKmsKey.sh <region-short-name>
                """
                logging.fatal(pycrypto_explanation)
                sys.exit(-5)
            return Random.new().read(256 / 8)
