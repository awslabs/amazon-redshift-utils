#!/usr/bin/env python

import os
import sys

# add the lib directory to the sys path
try:
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
except:
    pass

import json
import base64
import boto3
import utils
import analyze_schema_compression

config = None
region_key = 'AWS_REGION'

def event_handler(event, context):
    try:
        currentRegion = os.environ[region_key]
        
        if currentRegion == None or currentRegion == '':
            raise KeyError
    except KeyError:
        raise Exception("Unable to resolve environment variable %s" % region_key)
    
    kmsConnection = boto3.client('kms', region_name=currentRegion)
    
    # KMS crypto authorisation context
    authContext = utils.get_encryption_context(currentRegion)
    
    global config
    
    # lazy load the configuration file
    if config == None:
        try:
            config_file = open("config.json", 'r')
            config = json.load(config_file)
        
            if config == None:
                raise Exception("No Configuration Found")
        except:
            print sys.exc_info()[0]
            raise
        
    # resolve password
    configDetail = config["configuration"]
    encryptedPassword = configDetail["dbPassword"]
    encryptedPassword = base64.b64decode(encryptedPassword)
    
    if encryptedPassword != "" and encryptedPassword != None:
        # decrypt the password using KMS
        usePassword = kmsConnection.decrypt(CiphertextBlob=encryptedPassword, EncryptionContext=authContext)['Plaintext']
    else:
        raise Exception("Unable to run Utilities without a configured Password")
        
    # run the column encoding utility, if requested
    if "ColumnEncodingUtility" in config["utilities"]:
        analyze_schema_compression.configure(configDetail["outputFile"], configDetail["db"], configDetail["dbUser"], usePassword, configDetail["dbHost"], configDetail["dbPort"], configDetail["analyzeSchema"], configDetail["targetSchema"], configDetail["analyzeTable"], 1, True, configDetail["querySlotCount"], configDetail["ignoreErrors"], configDetail["force"], configDetail["dropOldData"], configDetail["comprows"], configDetail["queryGroup"], configDetail["debug"])
        encoding_result = analyze_schema_compression.run()
        
    print "Processing Complete"
    return encoding_result

if __name__ == "__main__":
    event_handler(sys.argv[0], None)
