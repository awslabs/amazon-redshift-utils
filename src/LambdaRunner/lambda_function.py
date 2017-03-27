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
import analyze_vacuum_schema

config = None
# config2=None
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
    # global config2

    # lazy load the configuration file
    if config == None:
        try:
            config_file = open("config.json", 'r')
            config_e = json.load(config_file)

            if config== None:
                raise Exception("No Configuration Found")
        except:
            print sys.exc_info()[0]
            raise

    # resolve password
    configDetail_e = config["configuration"]
    configDetail_v = config["configuration2"]
    encryptedPassword_e = configDetail_e["dbPassword"]
    encryptedPassword_e = base64.b64decode(encryptedPassword_e)

    encryptedPassword_v = configDetail_v["dbPassword"]
    encryptedPassword_v= base64.b64decode(encryptedPassword_v)

    if encryptedPassword_e != "" and encryptedPassword_e != None:
        # decrypt the password using KMS
        usePassword_e = kmsConnection.decrypt(CiphertextBlob=encryptedPassword_e, EncryptionContext=authContext)['Plaintext']
    else:
        raise Exception("Unable to run Encoding Utilities without a configured Password")

     if encryptedPassword_v != "" and encryptedPassword_v != None:
        # decrypt the password using KMS
        usePassword_v = kmsConnection.decrypt(CiphertextBlob=encryptedPassword_v, EncryptionContext=authContext)['Plaintext']
    else:
        raise Exception("Unable to run Vacuum Utilities without a configured Password")
    encoding_result=[]
     # run the column encoding utility, if requested
    if "ColumnEncodingUtility" in config_e["utilities"]:
        analyze_schema_compression.configure(configDetail_e["outputFile"], configDetail_e["db"], configDetail_e["dbUser"], usePassword_e, configDetail_e["dbHost"], configDetail_e["dbPort"], configDetail_e["analyzeSchema"], configDetail_e["targetSchema"], configDetail_e["analyzeTable"], 1, True, configDetail_e["querySlotCount"], configDetail_e["ignoreErrors"], configDetail_e["force"], configDetail_e["dropOldData"], configDetail_e["comprows"], configDetail_e["queryGroup"], configDetail_e["debug"])
        encoding_result.append(analyze_schema_compression.run())

    print "Processing Complete for Encoding"

    if "AnalyzeVacuumUtility" in config_v["utilities"]:
        analyze_vacuum_schema.configure(configDetail_v["outputFile"], configDetail_v["db"], configDetail_e["dbUser"], usePassword_v, configDetail_v["dbHost"], configDetail_v["dbPort"], configDetail_v["schemaName"], configDetail_v["tableName"], configDetail_v["querySlotCount"], configDetail_v["ignoreErrors"], configDetail_v["analyzeFlag"],configDetail_v["vacuumFlag"], configDetail_v["vacuumParameter"],0.05,0.5,0.1, configDetail_v["queryGroup"], configDetail_v["debug"],0.1,configDetail_v["maxTableSize"])
        encoding_result.append(analyze_schema_compression.run())

    print "Processing Complete for Vacuum"
    return encoding_result

if __name__ == "__main__":
    event_handler(sys.argv[0], None)
