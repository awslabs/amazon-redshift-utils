#!/usr/bin/env python
import sys
import common
import argparse

def usage():
    print "Redshift Execute Query Utility"
    print "Executes a query in Redshift."
    print ""
    print "Usage: This utility needs one argument the path of the sql file."
    print "python common_rs_execquery.py <configuration> <sql_file_path>"
    print "    <configuration> Local Path to configs file in configs directory"
    print "    <sql_file> Contains the select statement that needs to be used to download data.  Can be in json or plain sql file. examples in sql folder."
    print "python  /git/amazon-redshift-utils/RedshiftETLTools/bin/common_rs_execquery.py /git/amazon-redshift-utils/RedshiftETLTools/config/connect_configs /git/amazon-redshift-utils/RedshiftETLTools/sql/events_hr_ul.sql"
    sys.exit(-1)

def rsExecQuery(configFilePath,sqlFilePath):
    # read SQL file
    queryName, query = common.getQueryInfo(sqlFilePath)
    # Logging
    global logger
    logger = common.setupLogging(queryName, configFilePath)

    logger.info("Executing query %s" %(query))
    dbConn = common.rsConn(configFilePath)
    if common.execQuery(dbConn, query,logger):
        logger.info("Query Successfully Executed.")
    dbConn.close()

def main(args):
    if len(args) < 3:
        usage()
    parser = argparse.ArgumentParser()
    parser.add_argument("configFilePath", help="Location for configuration file.")
    parser.add_argument("sqlFilePath", help="Location for SQL file.")
    args = parser.parse_args()
    rsExecQuery(args.configFilePath,args.sqlFilePath)

if __name__ == "__main__":
    main(sys.argv)
