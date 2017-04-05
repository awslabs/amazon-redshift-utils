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
    configDetail_encoding = config["configuration"]
    encryptedPassword_encoding = configDetail_encoding["dbPassword"]
    encryptedPassword_encoding = base64.b64decode(encryptedPassword_encoding)

    configDetail_vacuum = config["configuration2"]
    encryptedPassword_vacuum = configDetail_vacuum["dbPassword"]
    encryptedPassword_vacuum = base64.b64decode(encryptedPassword_vacuum)

    if encryptedPassword_encoding != "" and encryptedPassword_encoding != None:
        # decrypt the password using KMS
        usePassword_encoding = \
            kmsConnection.decrypt(CiphertextBlob=encryptedPassword_encoding, EncryptionContext=authContext)[
                'Plaintext']
    else:
        raise Exception("Unable to run Encoding Utilities without a configured Password")

    if encryptedPassword_vacuum != "" and encryptedPassword_vacuum != None:
        # decrypt the password using KMS
        usePassword_vacuum = \
            kmsConnection.decrypt(CiphertextBlob=encryptedPassword_vacuum, EncryptionContext=authContext)[
                'Plaintext']
    else:
        raise Exception("Unable to run Vacuum Utilities without a configured Password")

    encoding_result = []
    #run the column encoding utility, if requested
    if "ColumnEncodingUtility" in config["utilities"]:
        analyze_schema_compression.configure(configDetail_encoding["outputFile"], configDetail_encoding["db"],
                                             configDetail_encoding["dbUser"], usePassword_encoding,
                                             configDetail_encoding["dbHost"], configDetail_encoding["dbPort"],
                                             configDetail_encoding["analyzeSchema"],
                                             configDetail_encoding["targetSchema"],
                                             configDetail_encoding["analyzeTable"],
                                             configDetail_encoding["analyze_col_width"],
                                             configDetail_encoding["threads"], configDetail_encoding["do-execute"],
                                             configDetail_encoding["querySlotCount"],
                                             configDetail_encoding["ignoreErrors"], configDetail_encoding["force"],
                                             configDetail_encoding["dropOldData"], configDetail_encoding["comprows"],
                                             configDetail_encoding["queryGroup"], configDetail_encoding["debug"],
                                             configDetail_encoding["ssl-option"]

                                             )
    encoding_result.append(analyze_schema_compression.run())

    print "Processing Complete for Encoding"
    #run the analyze vacuum utility, if requested
    if "AnalyzeVacuumUtility" in config["utilities"]:
        import analyze_vacuum_schema
        analyze_vacuum_schema.configure(configDetail_vacuum["outputFile"], configDetail_vacuum["db"],
                                        configDetail_vacuum["dbUser"],
                                        usePassword_vacuum, configDetail_vacuum["dbHost"],
                                        configDetail_vacuum["dbPort"],
                                        configDetail_vacuum["schemaName"], configDetail_vacuum["tableName"],
                                        configDetail_vacuum["querySlotCount"], configDetail_vacuum["ignoreErrors"],
                                        configDetail_vacuum["analyzeFlag"], configDetail_vacuum["vacuumFlag"],
                                        configDetail_vacuum["vacuumParameter"], 0.05, 0.5, 0.1,
                                        configDetail_vacuum["queryGroup"],
                                        configDetail_vacuum["debug"], 0.1, configDetail_vacuum["maxTableSize"])
        encoding_result.append(analyze_vacuum_schema.run())

    print "Processing Complete for Vacuum"
    return encoding_result


if __name__ == "__main__":
    event_handler(sys.argv[0], None)
