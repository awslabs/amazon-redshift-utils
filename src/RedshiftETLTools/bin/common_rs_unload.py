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
    print "Redshift Unload Utility"
    print "Exports data from a source Redshift database to S3."
    print ""
    print "Usage: This utility needs two arguments the path of configs and the sql file."
    print "python common_rs_unload.py <configuration> <sql_file>"
    print "    <configuration> Local Path to configs file in configs directory"
    print "    <sql_file> Contains the select statement that needs to be used to download data."
    print "Additionally you can specify:"
    print " the S3 Data Stage location with a -l/--dataDownloadPath parameter."
    print " the unload parameter with a -u/--unloadParms parameter. These parms are delimiter, addquotes, escape etc. passed as a string."
    print "Usage example with optional parameters"
    print "python  /git/amazon-redshift-utils/RedshiftETLTools/bin/common_rs_unload.py /git/amazon-redshift-utils/RedshiftETLTools/config/connect_configs /git/amazon-redshift-utils/RedshiftETLTools/sql/sov_aggregates.sql -l s3://analytics.riffsy.com/tp_events/2018-02-07_13/"
    sys.exit(-1)


def prepUnload(conn, sqlText, dataDownloadPath, s3AccessCredentials, unloadParams):

    unloadStmt = """unload ('%s')
                 to '%s' credentials
                 '%s'
                 %s"""

    query = unloadStmt % (sqlText, dataDownloadPath, s3AccessCredentials, unloadParams)
    logger.info("Exporting %s data to %s" % (sqlText, dataDownloadPath))
    logger.info("Unload Query: %s" % (query))

    if common.execQuery(conn, query,logger):
        logger.info("Export Successful.")

def rsUnload(configFilePath,sqlFilePath,dataDownloadPath = None,unloadParams = None):
    # read SQL file
    queryName, sqlText = common.getQueryInfo(sqlFilePath)
    # Logging
    global logger
    logger = common.setupLogging(queryName, configFilePath)

    #get S3 connection credentials from config path
    s3AccessCredentials = common.getS3creds(configFilePath)

    if not unloadParams:
        unloadParams = common.getDefaultParms(configFilePath)

    # Unload Location Parms
    if not dataDownloadPath:
        dataDownloadPath = common.getDataDownloadPath(queryName,configFilePath)

    if not dataDownloadPath.startswith("s3://"):
        logger.info("s3Staging.path must be a path to S3")
        sys.exit(-1)

    logger.info("Exporting from Source...")
    srcConn = common.rsConn(configFilePath)
    prepUnload(srcConn, sqlText, dataDownloadPath, s3AccessCredentials, unloadParams)

    srcConn.close()

def main(args):
    if len(args) < 3:
        usage()
    parser = argparse.ArgumentParser()
    parser.add_argument("configFilePath", help="Location for configuration file.")
    parser.add_argument("sqlFilePath", help="Location for SQL file.")
    parser.add_argument("--dataDownloadPath", "-l", help="S3 location to unload data. if not provided takes the info from configFilePath.")
    parser.add_argument("--unloadParams", "-u", help="Unload parameters to override settings from configuration file path.  -u 'delimiter '^' addquotes'")
    args = parser.parse_args()
    rsUnload(args.configFilePath,args.sqlFilePath,args.dataDownloadPath,args.unloadParams)

if __name__ == "__main__":
    main(sys.argv)
