#!/usr/bin/env python
from __future__ import print_function
import os
import sys

# add the lib directory to the sys path
try:
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib/ColumnEncodingUtility"))
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib/AnalyzeVacuumUtility"))
except:
    pass

import json
import base64
import boto3
import utils
import analyze_schema_compression
import analyze_vacuum

region_key = 'AWS_REGION'
COLUMN_ENCODING = "ColumnEncodingUtility"
ANALYZE_VACUUM = "AnalyzeVacuumUtility"
LOCAL_CONFIG = "config.json"

def event_handler(event, context):
    currentRegion = 'us-east-1'
    try:
        currentRegion = os.environ[region_key]
        
        if currentRegion is None or currentRegion == '':
            raise KeyError
    except KeyError:
        raise Exception("Unable to resolve environment variable %s" % region_key)
    
    kmsConnection = boto3.client('kms', region_name=currentRegion)
    
    # KMS crypto authorisation context
    authContext = utils.get_encryption_context(currentRegion)
    
    # load the configuration file
    config_location = LOCAL_CONFIG
    
    if 'ConfigLocation' in event:
        config_location = event['ConfigLocation']
        
    if config_location.startswith("s3://"): 
        s3client = boto3.client('s3',region_name=currentRegion)
        
        bucket = config_location.replace('s3://', '').split("/")[0]
        key = config_location.replace('s3://' + bucket + "/",'')
        
        obj = s3client.get_object(Bucket=bucket, Key=key)
        config = json.loads(obj['Body'].read())
    elif config_location == LOCAL_CONFIG:
        try:
            config_file = open("config.json", 'r')
            config = json.load(config_file)
        
            if config is None:
                raise Exception("No Configuration Found")
        except:
            print(sys.exc_info()[0])
            raise
    else:
        raise Exception("Unsupported configuration location %s" % config_location)
        
    # resolve password
    configDetail = config["configuration"]
    encryptedPassword = configDetail["dbPassword"]
    encryptedPassword = base64.b64decode(encryptedPassword)
    
    if encryptedPassword != "" and encryptedPassword is not None:
        # decrypt the password using KMS
        usePassword = kmsConnection.decrypt(CiphertextBlob=encryptedPassword, EncryptionContext=authContext)['Plaintext']
    else:
        raise Exception("Unable to run Utilities without a configured Password")
    
    run_utilities = []
    
    if "ExecuteUtility" in event:
        if event["ExecuteUtility"] == COLUMN_ENCODING:
            run_utilities.append(COLUMN_ENCODING)
        elif event["ExecuteUtility"] == ANALYZE_VACUUM:
            run_utilities.append(ANALYZE_VACUUM)
    elif 'utilities' in config:
        # run each utility, if requested
        if COLUMN_ENCODING in config["utilities"]:
            run_utilities.append(COLUMN_ENCODING)
            
        if COLUMN_ENCODING in config["utilities"]:
            run_utilities.append(ANALYZE_VACUUM)
    else:
        print("No Utilities configured to run. Exiting!")
        return
        
    results = []
    for util in run_utilities:
        if util == COLUMN_ENCODING:
            print("Running %s" % util)
            analyze_schema_compression.configure(configDetail["outputFile"], configDetail["db"], configDetail["dbUser"], usePassword, configDetail["dbHost"], configDetail["dbPort"], configDetail["analyzeSchema"], configDetail["targetSchema"], configDetail["analyzeTable"],configDetail["analyze_col_width"],configDetail["threads"], configDetail["do-execute"], configDetail["querySlotCount"], configDetail["ignoreErrors"], configDetail["force"], configDetail["dropOldData"], configDetail["comprows"], configDetail["queryGroup"], configDetail["debug"], configDetail["ssl-option"],None)        
            encoding_result = analyze_schema_compression.run()
            results.append(encoding_result)
        elif util == ANALYZE_VACUUM:
            print("Running %s" % util)
            analyze_result = analyze_vacuum.run_analyze_vacuum(configDetail["dbHost"], configDetail["dbPort"], configDetail["dbUser"], usePassword, configDetail["db"], configDetail["queryGroup"], configDetail["querySlotCount"], configDetail["doVacuum"], configDetail["doAnalyze"], configDetail["analyzeSchema"], configDetail["analyzeTable"], configDetail["tableBlacklist"], configDetail["ignoreErrors"], configDetail["ssl-option"], configDetail["debug"])
            if analyze_result == 0:
                results.append("OK")
        
    print("Processing Complete")
    return results

if __name__ == "__main__":
    event_handler(sys.argv[0], None)
