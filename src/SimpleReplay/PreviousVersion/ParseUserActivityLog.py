#!/usr/bin/python

'''
ParseUSerActivityLog.py
* Copyright 2018, Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Amazon Software License (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* http://aws.amazon.com/asl/
*
* This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.

10/26/2016 : Initial Release.
'''


import argparse
import os
import sys
import re
import gzip


def Parse_Auditlog(input_log):
    if '.gz' in input_log:
       logfile = gzip.open(input_log)
    else:
       logfile = open(input_log)

    loglines = logfile.readlines()
    logfile.close()
    
    preparsed_lines = PreParse_Auditlog_Datetime_Lines(loglines)
    #print preparsed_lines
    regular_user_lines = Parse_Regular_User_lines(preparsed_lines)
    return regular_user_lines

def Parse_Regular_User_lines(preparsed_lines):
    rdsdb_pat = re.compile(r'db=rdsdb')
    regular_user_lines = [s for s in preparsed_lines if not rdsdb_pat.search(s)]
    return regular_user_lines

def Remove_Problem_Keywords(reg_user_lines):
    #problem_keywords = ['context: SQL', 'copy', 'insert', 'delete', 'drop',
    #                    'update', 'begin', 'commit', 'vacuum', 'analyze','alter',
    #                    'create', 'pg_table_def', 'pg_catalog','pg_class', 'pg_namespace',
    #                    'pg_attribute', 'pg_tables', 'grant', 'set ',
    #                    'show ']
    problem_keywords = ['context: SQL', 'ERROR:', 'CONTEXT:  SQL', '$2', '$1',
                        'pg_table_def', 'pg_catalog','pg_class', 'pg_namespace',
                        'pg_attribute', 'pg_tables', 'show ', 'Undoing transaction', 'Undo on', 'pg_terminate_backend','pg_cancel_backend', 'volt_', 'pg_temp_' , 'BIND']
    sanitizedList = []
    for i in range(0, len(reg_user_lines), 1):
        if not ( any(word in reg_user_lines[i] for word in problem_keywords)):
            sanitizedList.append(reg_user_lines[i])
        #else:
        #    print "Else:" + reg_user_lines[i]
    return sanitizedList

def PreParse_Auditlog_Datetime_Lines(loglines_input):
    loglines = loglines_input[:]
    datetime_pat = re.compile(r'\d+-\d+-\d+T\d+:\d+:\d+Z UTC')
    matchdatetime = []
    for i in range(0,len(loglines), 1):
        if datetime_pat.search(loglines[i]) is not None:
            matchdatetime.append(i)
            #print matchdatetime
    linesremoved = 0

    for i in range(0,len(matchdatetime)-1,1):
        if (matchdatetime[i+1] != matchdatetime[i] + 1):
            #print str(matchdatetime[i] - linesremoved) + " and " + str(matchdatetime[i+1] - linesremoved)
            loglines[matchdatetime[i]-linesremoved:matchdatetime[i+1]-linesremoved] = [''.join(loglines[matchdatetime[i]-linesremoved:matchdatetime[i+1]-linesremoved])]
            linesremoved += (matchdatetime[i+1] - matchdatetime[i] -1)
            #print linesremoved
    return loglines

def BuildQueryObjList(sanitizedQueryObjList):
    queryObjList = []
    skipped = 0
    #print reg_user_lines[55]
    for i in range(0, len(sanitizedQueryObjList), 1):
        #print sanitizedQueryObjList[i]
        splittedline = sanitizedQueryObjList[i].split("LOG: ")
        # Skip Malformed Lines
        if len(splittedline) <> 2:
           #print sanitizedQueryObjList[i]
           skipped = skipped + 1
           continue
        querydata = splittedline[0].replace("'","")
        querydatasplitspace = querydata.split(" ")
        # Skip Malformed Lines
        if len(querydatasplitspace) <> 10:
           #print querydata
           skipped = skipped + 1
           continue
        curdatetime = str(querydatasplitspace[0].replace("T"," ").replace("Z","")) + " " + str(querydatasplitspace[1])
        databasename = querydatasplitspace[3].split("=")[1]
        username = querydatasplitspace[4].split("=")[1]
        pid = querydatasplitspace[5].split("=")[1]
        userid = querydatasplitspace[6].split("=")[1]
        xid = querydatasplitspace[7].split("=")[1]
        querytext = splittedline[1]
        queryobj = QueryObj(curdatetime, username, userid, databasename, pid, xid, querytext)
        #print str(queryobj)
        queryObjList.append(queryobj)
    if skipped > 0:
        print "Skipped Rows: " + str(skipped)
    return queryObjList


def Print_Human_Readable(queryObjList, creds, rewrites, readonly):
    creds_list = ['COPY ','UNLOAD ']
    rw_list = ['CREATE TABLE ','INSERT ','DELETE ','UPDATE ','DROP TABLE ','ALTER TABLE ','COPY ','UNLOAD ', 'VACUUM ','ANALYZE ', 'TRUNCATE ', 'GRANT ']
    rw_files = []
    skip_ends = [ 'BEGIN;', 'COMMIT;' ]
    skip_rewrites = [ 'SELECT ', 'UPDATE ', 'INSERT INTO ', 'DELETE FROM ', 'CREATE TEMP TABLE ','WITH ']
    avoid_skipping = [ 'SELECT *', ' BETWEEN ', 'LIKE ' ]
    dedupe_these = ['set ', 'select', 'create', 'delete', 'update', 'insert', 'SET ', 'SELECT', 'CREATE', 'DELETE', 'UPDATE', 'INSERT']
    for i in range(0, len(queryObjList), 1):
        if queryObjList[i].querystatement.endswith(tuple(skip_ends)):
           continue
        fname = queryObjList[i].databasename + '-' + queryObjList[i].username + '-' + queryObjList[i].pid + '.sql'
        rwname = 'rw-'+ fname
        if rewrites and ';' in queryObjList[i].querystatement and queryObjList[i].querystatement.count('\n') == 1 and queryObjList[i].querystatement.startswith(tuple(skip_rewrites)) and not any(word in queryObjList[i].querystatement for word in avoid_skipping):
           #print fname 
           #print queryObjList[i].querystatement 
           continue
        #
        if readonly and any(word.lower() in queryObjList[i].querystatement.lower() for word in rw_list):
           if fname not in rw_files:
              rw_files.append(fname)
           rwname = 'rw-'+ fname
           if os.path.isfile(fname):
             os.rename(fname,rwname)
        #
        if readonly and fname in rw_files:
          fname = rwname
        #
        firstTime = False
        if not os.path.isfile(fname):
          setUser = 'set session_authorization to ' + queryObjList[i].username + ';\n'
          firstTime = True 
        else:
          if  any(word in queryObjList[i].querystatement for word in dedupe_these):
             if queryObjList[i].querystatement.endswith(tuple(';')) or queryObjList[i].querystatement in open(fname).read():
                continue
        #
        f = open ( fname, 'a' )
        if firstTime:
          f.write ('--Starttime: ' + queryObjList[i].startdatetime + '\n')
          f.write (setUser)
        if  (creds and any(word in queryObjList[i].querystatement for word in creds_list)):
           f.write(queryObjList[i].querystatement.replace("CREDENTIALS ''","CREDENTIALS '"+ creds + "'") + ";\n" )
        else:
           f.write( queryObjList[i].querystatement + ";\n" )
        f.close()

def main(input_log, creds, rewrites, readonly):
    reg_user_lines = Parse_Auditlog(input_log)
    sanitizedQueryObjList = Remove_Problem_Keywords(reg_user_lines)
    queryObjList = BuildQueryObjList(sanitizedQueryObjList)
    Print_Human_Readable(queryObjList, creds, rewrites, readonly)


class QueryObj:
    def __init__(self, startdatetime, username, userid, databasename, pid, xid, querystatement):
        self.startdatetime = startdatetime
        self.username = username
        self.userid = userid
        self.databasename = databasename
        self.pid = pid
        self.xid = xid
        self.querystatement = querystatement

    def __str__(self):
        return "Startdate: %s, Username: %s, USerid: %s Database Name: %s, Pid: %s, Xid: %s, Query Text: %s" % (self.startdatetime, self.username, self.userid, self.databasename, self.pid, self.xid, self.querystatement)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--creds", type=str, default='', help="Provide credentials for COPY and UNLOAD commands")
    parser.add_argument("-r", "--rewrites", action="store_true", help="Try to mitigate rewrites by optimizer")
    parser.add_argument("-s", "--readonly", action="store_true", help="Flag writes into rw- prefixed files")
    parser.add_argument("auditfile", help="Logfile to be processed")
    args = parser.parse_args()

    main(args.auditfile, args.creds, args.rewrites, args.readonly)
