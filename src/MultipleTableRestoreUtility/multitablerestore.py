# !/usr/bin/python

"""
multitablerestore.py
* Copyright 2016, Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Amazon Software License (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* http://aws.amazon.com/asl/
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.

Managed by adedotua

2017-12-23: PEP8 compliance
2017-12-23: Added cosmetic changes. Logs will now record timestamp for each step. 
            Number of successful, canceled or Failed restores will now be recorded once script completes
2017-12-23: Fixed issue where script would get stuck if table restore status is in state CANCELED.
"""

import json
import getopt
import datetime
from time import sleep
from boto3 import client
from botocore.exceptions import ClientError
from sys import exit, argv


class RsRestore:
    # Create restore table object
    def __init__(self, clusterIdentifier, snapshotIdentifier,
                 sourceDatabaseName, sourceSchemaName,
                 targetDatabaseName, targetSchemaName):
        self.rsclient = client('redshift')
        self.clusterId = clusterIdentifier
        self.snapshotId = snapshotIdentifier
        self.srcDatabase = sourceDatabaseName
        self.tgtDatabase = targetDatabaseName
        self.srcSchema = sourceSchemaName
        self.tgtSchema = targetSchemaName
        self.requestId = {}

    # Restore the table
    def restoretable(self, srcTable, tgtTable):
        try:
            self.requestId = self.rsclient.restore_table_from_cluster_snapshot(ClusterIdentifier=self.clusterId,
                                                                               SnapshotIdentifier=self.snapshotId,
                                                                               SourceDatabaseName=self.srcDatabase,
                                                                               SourceSchemaName=self.srcSchema,
                                                                               SourceTableName=srcTable,
                                                                               TargetDatabaseName=self.tgtDatabase,
                                                                               TargetSchemaName=self.tgtSchema,
                                                                               NewTableName=tgtTable)
        except ClientError as e:
            print e.response['Error']['Message']
            exit()

    # Get the status of the table restore
    def restorestatus(self, output):
        rstatus = self.rsclient.describe_table_restore_status(ClusterIdentifier=self.clusterId,
                                                              TableRestoreRequestId=self.requestId[
                                                                  'TableRestoreStatus']['TableRestoreRequestId'])
        try:
            return rstatus['TableRestoreStatusDetails'][0][output]
        except:
            return rstatus['TableRestoreStatusDetails'][0]['Message']

    def printmessage(self, status):
        datetime_str = str(datetime.datetime.now())
        if status == 'FAILED':
            print "[%s] %s " % (datetime_str, self.restorestatus('Message'))
        elif status != 'SUCCEEDED':
            print "[%s] STATUS: %s " % (datetime_str, self.restorestatus('Status'))
        else:
            print "[%s] DETAIL: Table %s.%s restored to database %s. Total size restored is %sMB." \
                  % (datetime_str, self.restorestatus('TargetSchemaName'), self.restorestatus('NewTableName'),
                     self.restorestatus('TargetDatabaseName'), self.restorestatus('TotalDataInMegaBytes'))


#  Usage of script
def errormsg(script_name):
    print("Usage: %s --target-database-name <target database> "
          "--source-database-name <source database> "
          "--snapshot-identifier <snapshot name> "
          "--cluster-identifier <cluster> --listfile <filename>") % script_name


#  Table restore function that can be called from within another module
def tablerestore(tgtdbname, srcdbname, snapshotid, clusterid, filename):
    previous_status = None
    try:
        with open(filename) as data_file:
            datac = json.load(data_file)
            #  Check json for valid key
            if 'TableRestoreList' not in datac:
                print 'ERROR: \'%s\' key in %s list is an invalid key. Valid key is \'TableRestoreList\'.' \
                      % (datac.keys()[0], filename)
                exit()
    #  Check restore list file exists
    except IOError:
        print 'Table restore list %s does not exist. Check file and try again.' % filename
        exit()
    except Exception as e:
        print e
        exit()

    count_succeeded = 0
    count_failed = 0
    count_canceled = 0
    count_unknown = 0
    total_restore_size = 0

    for i in datac['TableRestoreList']:
        try:
            srcschema = i['Schemaname']
            srctable = i['Tablename']
        except KeyError as e:
            print 'ERROR: Expected key %s is missing in %s.' % (e, filename)
            print 'DETAIL: %s' % i
            exit()
        tgtschema = srcschema
        trgttable = srctable
        tlr = RsRestore(clusterid, snapshotid, srcdbname, srcschema, tgtdbname, tgtschema)
        tlr.restoretable(srctable, trgttable)
        print "%s Starting Table Restores %s" % ('-' * 50, '-' * 50)
        print "[%s] Requestid: %s " % (str(datetime.datetime.now()), tlr.restorestatus('TableRestoreRequestId'))
        print "[%s] INFO: Starting restore of %s to schema %s in database %s" % (str(datetime.datetime.now()),
                                                                                 trgttable, tgtschema, tgtdbname)
        current_status = tlr.restorestatus('Status')
        while current_status != 'SUCCEEDED' and current_status != 'FAILED' and current_status != 'CANCELED':
            if current_status != previous_status:
                previous_status = current_status
                tlr.printmessage(current_status)
            sleep(2)
            current_status = tlr.restorestatus('Status')

        if current_status == 'SUCCEEDED':
            count_succeeded += 1
            total_restore_size += tlr.restorestatus('TotalDataInMegaBytes')
        elif current_status == 'FAILED':
            count_failed += 1
        elif current_status == 'CANCELED':
            count_canceled += 1
        else:
            count_unknown += 1
        tlr.printmessage(current_status)
    print "%s Table Restore Summary %s" % ('-' * 51, '-' * 51)
    print "[%s] Succeeded: %d Failed: %d Canceled: %d Unknown: %d Total Size: %dMB" \
          % (str(datetime.datetime.now()), count_succeeded, count_failed, count_canceled,
             count_unknown, total_restore_size)
    print "%s" % ('-' * 125)


def main(input_args):
    tgtdbname = None
    srcdbname = None
    snapshotid = None
    clusterid = None
    filename = None

    try:
        optlist,  remaining = getopt.getopt(input_args[1:], "", ['target-database-name=', 'source-database-name=',
                                                                 'snapshot-identifier=', 'cluster-identifier=',
                                                                 'listfile='])
    except getopt.GetoptError as err:
        print str(err)
        exit()

    for arg,  value in optlist:
        if arg == "--target-database-name":
            tgtdbname = value
        elif arg == "--source-database-name":
            srcdbname = value
        elif arg == "--snapshot-identifier":
            snapshotid = value
        elif arg == "--cluster-identifier":
            clusterid = value
        elif arg == "--listfile":
            filename = value
        else:
            print "Unknown argument %s" % arg
    if (tgtdbname is None) or (srcdbname is None) or (snapshotid is None) or (clusterid is None) or (filename is None):
        errormsg(input_args[0])
        exit()
    tablerestore(tgtdbname, srcdbname, snapshotid, clusterid, filename)


if __name__ == "__main__":
    main(argv)
