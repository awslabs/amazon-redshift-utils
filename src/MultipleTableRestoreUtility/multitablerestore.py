# !/usr/bin/python

'''
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
'''

import json
import getopt
from time import sleep
from boto3 import client
from botocore.exceptions import ClientError
from sys import exit, stdout, argv


class rsrestore:
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

    # In place printing of the restore status
    def monitorRestore(self):
        print "\rSTATUS: %s " % self.restorestatus('Status'),
        stdout.flush()

    # Print the final status of the table restore
    def printMessage(self):
        if self.restorestatus('Status') == 'SUCCEEDED':
            print "\nDETAIL: Table %s.%s restored to database %s. Total size restored is %sMB." % (self.restorestatus('TargetSchemaName'),
                                                                                                   self.restorestatus('NewTableName'),
                                                                                                   self.restorestatus('TargetDatabaseName'),
                                                                                                   self.restorestatus('TotalDataInMegaBytes'))
        else:
            print "\n%s " % (self.restorestatus('Message'))


#  Usage of script
def errormsg(script_name):
    print('Usage: %s --target-database-name <target database> --source-database-name <source database> --snapshot-identifier <snapshot name> --cluster-identifier <cluster> --listfile <filename>') % script_name


#  Table restore function that can be called from within another module
def tablerestore(tgtdbname, srcdbname, snapshotid, clusterid, filename):
    try:
        with open(filename) as data_file:
            datac = json.load(data_file)
            #  Check json for valid key
            if 'TableRestoreList' not in datac:
                print 'ERROR: \'%s\' key in %s list is an invalid key. Valid key is \'TableRestoreList\'.' % (datac.keys()[0],
                                                                                                              filename)
                exit()
    #  Check restore list file exists
    except IOError:
        print 'Table restore list %s does not exist. Check file and try again.' % filename
        exit()
    except Exception as e:
        print e
        exit()

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
        tlr = rsrestore(clusterid, snapshotid, srcdbname, srcschema, tgtdbname, tgtschema)

        tlr.restoretable(srctable, trgttable)
        print "INFO: Starting restore of %s to schema %s in database %s. Requestid: %s " % (trgttable, tgtschema, tgtdbname,
                                                                                            tlr.restorestatus(
                                                                                                'TableRestoreRequestId'))
        while tlr.restorestatus('Status') != 'SUCCEEDED' and tlr.restorestatus('Status') != 'FAILED':
            tlr.monitorRestore()
            sleep(2)
        tlr.monitorRestore()
        tlr.printMessage()


def main(input_args):
    tgtdbname = None
    srcdbname = None
    snapshotid = None
    clusterid = None
    filename = None

    try:
        optlist,  remaining = getopt.getopt(input_args[1:], "", ['target-database-name=', 'source-database-name=',
                                                                 'snapshot-identifier=', 'cluster-identifier=', 'listfile='])
    except getopt.GetoptError as err:
        print str(err)
        exit()

    for arg,  value in optlist:
        if arg in ("--target-database-name"):
            tgtdbname = value
        elif arg in ("--source-database-name"):
            srcdbname = value
        elif arg in ("--snapshot-identifier"):
            snapshotid = value
        elif arg in ("--cluster-identifier"):
            clusterid = value
        elif arg in ("--listfile"):
            filename = value
        else:
            print "Unknown argument %s" % (arg)
    if (tgtdbname is None) or (srcdbname is None) or (snapshotid is None) or (clusterid is None) or (filename is None):
        errormsg(input_args[0])
        exit()
    tablerestore(tgtdbname, srcdbname, snapshotid, clusterid, filename)

if __name__ == "__main__":
    main(argv)
