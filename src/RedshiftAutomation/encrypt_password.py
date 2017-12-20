#!/usr/bin/env python27

from __future__ import print_function
import boto3
import utils
import os
import sys
import base64
from boto.kms.exceptions import NotFoundException
from botocore.exceptions import *

OK = 0
ERROR = -1
INVALID_ARGS = -2
region_key = 'AWS_REGION'

def encrypt_password(args):
    if len(args) == 1:
        print("You must supply the password to be encrypted")
        sys.exit(-1)
        
    try:
        currentRegion = os.environ[region_key]
        
        if currentRegion == None or currentRegion == '':
            raise KeyError
    except KeyError:
        raise Exception("Unable to resolve environment variable %s" % region_key)
    
    # create a KMS connection
    kmsConnection = boto3.client('kms', region_name=currentRegion)
    
    # check to see if the application Customer Master Key exists
    cmkStatus = None
    try:
        cmkStatus = kmsConnection.describe_key(KeyId=utils.CMK)
    except ClientError as e:
        if 'NotFoundException' in str(e):
            pass
        else:
            raise e
    
    if cmkStatus == None or cmkStatus['KeyMetadata'] == None:
        # create the key and an alias
        new_cmk = kmsConnection.create_key(Description='AWSLambdaRedshiftUtilsPasswordEncryption',
                                           KeyUsage='ENCRYPT_DECRYPT')
        if new_cmk == None:
            print("Failed to create Customer Master Key")
            sys.exit(ERROR)
        alias = kmsConnection.create_alias(AliasName=utils.CMK,
                                           TargetKeyId=new_cmk['KeyMetadata']['KeyId'])
        
    # encrypt the provided password with this kms key
        # get the application authorisation context
    auth_context = utils.get_encryption_context(currentRegion)
    
    encrypted = kmsConnection.encrypt(KeyId=utils.CMK,
                                      Plaintext=args[1],
                                      EncryptionContext=auth_context)
    
    print("Encryption Complete in %s" % (currentRegion))
    print("Encrypted Password: %s" % base64.b64encode(encrypted['CiphertextBlob']))
        
if __name__ == "__main__":
    encrypt_password(sys.argv)
