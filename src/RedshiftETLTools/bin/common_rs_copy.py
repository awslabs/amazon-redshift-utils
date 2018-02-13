#!/usr/bin/env python
import sys
import common
import argparse

s3Client = None
nowString = None
#config = None
region = None
bucket = None
key = None

def usage():
    print "Redshift Copy Utility"
    print "Exports data from a source Redshift database to S3."
    print ""
    print "Usage: This utility needs two arguments the path of configs and the sql file."
    print "python common_rs_copy.py <configuration> <sql_file>"
    print "    <configuration> Local Path to configs file in configs directory"
    print "    <target table> you want to load the data into."
    print "    <sql_file> Contains the select statement that needs to be used to download data. Can be in json or plain sql file. examples in sql folder."
    print "Additionally you can specify:"
    print " the copy parameter with a -u/--copyParams parameter. These parms are delimiter, removequotes, escape etc. passed as a string."
    print "Usage example with optional parameters"
    print "/git/amazon-redshift-utils/RedshiftETLTools/bin/common_rs_copy.py /git/amazon-redshift-utils/RedshiftETLTools/config/connect_configs public.events   s3://analytics-s3.company.com/events/2018-02-08_15-33-21/ removequotes  escape  delimiter ','"
    sys.exit(-1)


def prepCopy(tgtConn, tgtTableName, dataPath, s3AccessCredentials, copyParams):

    copyStmt = """copy %s
                 from '%s'
                 credentials '%s'
                 maxerror 10000
                 %s"""

    query = copyStmt % (tgtTableName, dataPath, s3AccessCredentials, copyParams)

    logger.info("Importing into %s data from %s" % (tgtTableName, dataPath))
    logger.info("Copy Query: %s" % (query))

    if common.execQuery(tgtConn, query,logger):
        logger.info("Loading %s Successful." %(tgtTableName))

def rsCopy(configFilePath,tgtTableName,dataPath, copyParams = None):

    # Logging
    global logger
    logger = common.setupLogging(tgtTableName, configFilePath)

    #get S3 connection credentials from config path
    s3AccessCredentials = common.getS3creds(configFilePath)

    if not copyParams:
        copyParams = common.getDefaultParms(configFilePath)
        copyParams = copyParams.replace("addquotes", "removequotes")
        copyParams = copyParams.replace("allowoverwrite", "")

    if not dataPath.startswith("s3://"):
        print "s3Staging.path must be a path to S3"
        sys.exit(-1)

    logger.info("Loading into table %s" %(tgtTableName))
    tgtConn = common.rsConn(configFilePath)
    prepCopy(tgtConn, tgtTableName, dataPath, s3AccessCredentials, copyParams)

    tgtConn.close()

def main(args):
    if len(args) < 4:
        usage()
    parser = argparse.ArgumentParser()
    parser.add_argument("configFilePath", help="Location for configuration file.")
    parser.add_argument("tgtTableName", help="Table to load to. Provide in the form schemaname.tablename")
    parser.add_argument("srcDataPath", help="S3 location to source the data.")
    parser.add_argument("--copyParams", "-u", help="User copy parameters to override settings from configuration file path.  -u 'delimiter '^' addquotes'")
    args = parser.parse_args()
    rsCopy(args.configFilePath,args.tgtTableName,args.srcDataPath,args.copyParams)

if __name__ == "__main__":
    main(sys.argv)
