import boto3
import json
import sys
import os
import base64

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import config_constants

def get_password(kms_connection, config_detail, debug):
    # KMS crypto authorisation context
    auth_context = None
    if config_constants.KMS_AUTH_CONTEXT in config_detail:
        auth_context = config_detail[config_constants.KMS_AUTH_CONTEXT]
        # convert to json
        auth_context = json.loads(auth_context)

        if debug:
            print("Using Authorisation Context for decryption")
            print(auth_context)

    # resolve password
    if config_constants.ENCRYPTED_PASSWORD in config_detail:
        encrypted_password = base64.b64decode(config_detail[config_constants.ENCRYPTED_PASSWORD])

        if encrypted_password != "" and encrypted_password is not None:
            if auth_context is not None:
                # decrypt the password using KMS
                use_password = kms_connection.decrypt(CiphertextBlob=encrypted_password, EncryptionContext=auth_context)[
                    'Plaintext']
            else:
                # decrypt the password using KMS
                use_password = kms_connection.decrypt(CiphertextBlob=encrypted_password)[
                    'Plaintext']
        else:
            raise Exception("Unable to run Utilities without a configured Password")

        return use_password
    else:
        return None

def get_config(config_location, current_region, debug):
    if config_location.startswith("s3://"):
        print("Downloading configuration from %s" % config_location)
        # load the configuration file from S3
        s3_client = boto3.client('s3', region_name=current_region)

        bucket = config_location.replace('s3://', '').split("/")[0]
        key = config_location.replace('s3://' + bucket + "/", '')

        obj = s3_client.get_object(Bucket=bucket, Key=key)
        config_body = obj['Body'].read()
        config = json.loads(config_body)

        if debug:
            print("Raw Configuration downloaded from S3")
            print(config)
    elif config_location == config_constants.LOCAL_CONFIG:
        print("Using local configuration")
        # load from the local configuration
        if not os.path.isfile(config_constants.LOCAL_CONFIG):
            raise Exception("Unable to resolve local %s file" % config_constants.LOCAL_CONFIG)
        else:
            config_file = open("config.json", 'r')
            config = json.load(config_file)

            if config is None:
                raise Exception("No Configuration Found")
    else:
        raise Exception("Unsupported configuration location %s" % config_location)

    if "configuration" in config:
        config_detail = config["configuration"]

        # convert the provided configuration into something that the utilities we're calling will understand
        config_detail = config_constants.normalise_config(config_detail)

        return config_detail
    else:
        return config

