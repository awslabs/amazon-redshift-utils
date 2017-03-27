#!/usr/bin/env python

'''
analyze-vacuum-schema.py
* Copyright 2015, Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

The Redshift Analyze Vacuum Utility gives you the ability to automate VACUUM and ANALYZE operations.
When run, it will analyze or vacuum an entire schema or individual tables. This Utility Analyzes
and Vacuums table(s) in a Redshift Database schema, based on certain parameters like unsorted,
stats off and size of the table and system alerts from stl_explain & stl_alert_event_log.
By turning on/off '--analyze-flag' and  '--vacuum-flag' parameters, you can run it as  'vacuum-only'
or  'analyze-only' utility. This script can be scheduled to run VACUUM and ANALYZE as part of
regular maintenance/housekeeping activities, when there are less database activities (quiet period).

This script will:

   1) Analyze a single table or tables in a schema based on,
        a) Alerts from stl_explain & stl_alert_event_log.
        b) 'stats off' metrics from SVV_TABLE_INFO.

   2) Vacuum a single table or tables in a schema based on,
        a) The alerts from stl_alert_event_log.
        b) The 'unsorted' and 'size' metrics from SVV_TABLE_INFO.
        c) Vacuum reindex to analyze the interleaved sort keys

Srinikri Amazon Web Services (2015)

11/21/2015 : Added support for vacuum reindex to analyze the interleaved sort keys.

'''

import sys
import pg
import getopt
import os
import re
import getpass
import traceback
import datetime
from string import uppercase

__version__ = ".9.1.3.4"

ERROR = 1
INVALID_ARGS = 2
NO_WORK = 3
TERMINATED_BY_USER = 4
NO_CONNECTION = 5

# timeout for retries - 100ms
RETRY_TIMEOUT = 100/1000


master_conn = None
db_connections = {}
db = None
db_user = None
db_pwd = None
db_host = None
db_port = 5439
schema_name = 'public'
table_name = None
debug = False
output_file_handle = None
do_execute = False
query_slot_count = 1
ignore_errors = False
query_group = None

#set default values to vacuum, analyze variables

analyze_flag       = True
vacuum_flag        = True
vacuum_parameter   = 'FULL'
min_unsorted_pct   = 05
max_unsorted_pct   = 50
deleted_pct        = 05
stats_off_pct      = 10
max_table_size_mb  = (700*1024)
goback_no_of_days  = 1
query_rank         = 25

def execute_query(str):
    conn = get_pg_conn()
    result = None
    query_result = conn.query(str)

    if query_result is not None:
        result = query_result.getresult()
        query_count = len(result)

        if debug:
            comment('Query Execution returned %s Results' % (len(result)))

    return result

def commit():
    execute_query('commit')

def rollback():
    execute_query('rollback')

def close_conn(conn):
    try:
        conn.close()
    except Exception as e:
        if debug:
            print(e)

def cleanup():
    # close all connections and close the output file
    if master_conn != None:
        close_conn(master_conn)

    for key in db_connections:
        if db_connections[key] != None:
            close_conn(db_connections[key])

    if output_file_handle != None:
        output_file_handle.close()

def comment(string):
    datetime_str = str(datetime.datetime.now())
    if (string != None):
        if re.match('.*\\n.*',string) != None:
            write('/* [%s]\n%s\n*/\n' % (str(os.getpid()),string))
        else:
            write('-- %s [%s] %s' % (datetime_str,str(os.getpid()),string))

def print_statements(statements):
    if statements != None:
        for s in statements:
            if s != None:
                write(s)

def write(s):
    # write output to all the places we want it
    print(s)
    if output_file_handle != None:
        output_file_handle.write( str(s) + "\n")
        output_file_handle.flush()

def get_pg_conn():
    global db_connections
    pid = str(os.getpid())

    conn = None

    # get the database connection for this PID
    try:
        conn = db_connections[pid]
    except KeyError:
        pass

    if conn == None:
        # connect to the database
        if debug:
            comment('Connect [%s] %s:%s:%s:%s' % (pid,db_host,db_port,db,db_user))

        try:
            options = 'keepalives=1 keepalives_idle=200 keepalives_interval=200 keepalives_count=5'
            connection_string = "host=%s port=%s dbname=%s user=%s password=%s %s" % (db_host, db_port, db, db_user, db_pwd, options)

            conn = pg.connect(dbname=connection_string)
        except Exception as e:
            write(e)
            write('Unable to connect to Cluster Endpoint')
            cleanup()
            sys.exit(ERROR)

        # set default search path
        search_path = 'set search_path = \'$user\',public,%s' % (schema_name)

        if debug:
            comment(search_path)

        try:
            conn.query(search_path)
        except pg.ProgrammingError as e:
            if re.match('schema "%s" does not exist' % (schema_name,),e.message) != None:
                write('Schema %s does not exist' % (schema_name,))
            else:
                write(e.message)
            return None

        if query_group is not None:
            set_query_group = 'set query_group to %s' % (query_group)

            if debug:
                comment(set_query_group)

            conn.query(set_query_group)

        if query_slot_count != 1:
            set_slot_count = 'set wlm_query_slot_count = %s' % (query_slot_count)

            if debug:
                comment(set_slot_count)

            conn.query(set_slot_count)

        # set a long statement timeout
        set_timeout = "set statement_timeout = '36000000'"
        if debug:
            comment(set_timeout)

        conn.query(set_timeout)

        # cache the connection
        db_connections[pid] = conn

    return conn


def run_commands(conn, commands):
    for idx,c in enumerate(commands,start=1):
        if c != None:

            comment('[%s] Running %s out of %s commands: %s' % (str(os.getpid()),idx,len(commands),c))
            try:
                conn.query(c)
                comment('Success.')
            except Exception as e:
                # cowardly bail on errors
                rollback()
                write(traceback.format_exc())
                return False

    return True

def run_vacuum(conn):

    statements =[]

    if table_name != None:

        get_vacuum_statement = '''SELECT DISTINCT 'vacuum %s ' + "schema" + '."' + "table" + '" ; '
                                                   + '/* '+ ' Table Name : ' + "schema" + '."' + "table"
                                                   + '",  Size : ' + CAST("size" AS VARCHAR(10)) + ' MB,  Unsorted_pct : ' + CAST("unsorted" AS VARCHAR(10))
                                                   + ',  Deleted_pct : ' + CAST("empty" AS VARCHAR(10)) +' */ ;'

                                        FROM svv_table_info
                                        WHERE (unsorted > %s OR empty > %s)
                                            AND   size < %s
                                            AND  "schema" = '%s'
                                            AND  "table" = '%s';
                                        ''' % (vacuum_parameter,min_unsorted_pct,deleted_pct,max_table_size_mb,schema_name,table_name)
    else:

        # query for all tables in the schema ordered by size descending
        comment("Extracting Candidate Tables for vacuum based on the alerts...")

        get_vacuum_statement = '''
                SELECT DISTINCT 'vacuum %s ' + feedback_tbl.schema_name + '."' + feedback_tbl.table_name + '" ; '
                + '/* '+ ' Table Name : ' + info_tbl."schema" + '."' + info_tbl."table"
                                                   + '",  Size : ' + CAST(info_tbl."size" AS VARCHAR(10)) + ' MB'
                                                   + ',  Unsorted_pct : ' + COALESCE(CAST(info_tbl."unsorted" AS VARCHAR(10)), 'N/A')
                                                   + ',  Deleted_pct : ' + CAST(info_tbl."empty" AS VARCHAR(10)) +' */ ;'
                    FROM (SELECT schema_name,
                                 table_name
                          FROM (SELECT TRIM(n.nspname) schema_name,
                                       c.relname table_name,
                                       DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS qry_rnk,
                                       COUNT(*)
                                FROM stl_alert_event_log AS l
                                  JOIN (SELECT query,
                                               tbl,
                                               perm_table_name
                                        FROM stl_scan
                                        WHERE perm_table_name <> 'Internal Worktable'
                                        GROUP BY query,
                                                 tbl,
                                                 perm_table_name) AS s ON s.query = l.query
                                  JOIN pg_class c ON c.oid = s.tbl
                                  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                                WHERE l.userid > 1
                                AND   l.event_time >= dateadd (DAY,-%s,CURRENT_DATE)
                                AND   l.Solution LIKE '%%VACUUM command%%'
                                GROUP BY TRIM(n.nspname),
                                         c.relname) anlyz_tbl
                          WHERE anlyz_tbl.qry_rnk < %s) feedback_tbl
                      JOIN svv_table_info info_tbl
                        ON info_tbl.schema = feedback_tbl.schema_name
                       AND info_tbl.table = feedback_tbl.table_name
                    WHERE /*(info_tbl.unsorted > %s OR info_tbl.empty > %s) AND */
                        info_tbl.size < %s
                        AND   TRIM(info_tbl.schema) = '%s'
                        AND   (sortkey1 not ilike  'INTERLEAVED%%' OR sortkey1 IS NULL)
                    ORDER BY info_tbl.size ASC, info_tbl.skew_rows ASC;
                            ''' %(vacuum_parameter,goback_no_of_days,query_rank,min_unsorted_pct,deleted_pct,max_table_size_mb,schema_name,)

    if debug:
        comment(get_vacuum_statement)

    vacuum_statements = execute_query(get_vacuum_statement)

    for vs in vacuum_statements:
        statements.append(vs[0])

    if not run_commands(conn, statements):
                    if not ignore_errors:
                        if debug:
                            write("Error running statements: %s" % (str(statements),))
                        return ERROR

    statements =[]
    if table_name == None:

        # query for all tables in the schema ordered by size descending
        comment("Extracting Candidate Tables for vacuum ...")
        get_vacuum_statement = '''SELECT DISTINCT 'vacuum %s ' + "schema" + '."' + "table" + '" ; '
                                                   + '/* '+ ' Table Name : ' + "schema" + '."' + "table"
                                                   + '",  Size : ' + CAST("size" AS VARCHAR(10)) + ' MB'
                                                   + ',  Unsorted_pct : ' + COALESCE(CAST(info_tbl."unsorted" AS VARCHAR(10)), 'N/A')
                                                   + ',  Deleted_pct : ' + CAST("empty" AS VARCHAR(10)) +' */ ;'

                                        FROM svv_table_info info_tbl
                                        WHERE "schema" = '%s'
                                                AND
                                                 (
                                                --If the size of the table is less than the max_table_size_mb then , run vacuum based on condition: >min_unsorted_pct AND >deleted_pct
                                                    ((size < %s) AND (unsorted > %s OR empty > %s))
                                                    OR
                                                --If the size of the table is greater than the max_table_size_mb then , run vacuum based on condition:
                                                -- >min_unsorted_pct AND < max_unsorted_pct AND >deleted_pct
                                                --This is to avoid big table with large unsorted_pct
                                                     ((size > %s) AND (unsorted > %s AND unsorted < %s ))
                                                 )
                                                AND (sortkey1 not ilike  'INTERLEAVED%%' OR sortkey1 IS NULL)
                                        ORDER BY "size" ASC ,skew_rows ASC;

                                        ''' %(vacuum_parameter,schema_name,max_table_size_mb,min_unsorted_pct,
                                              deleted_pct,max_table_size_mb,min_unsorted_pct,max_unsorted_pct)

        if debug:
            comment(get_vacuum_statement)

        vacuum_statements = execute_query(get_vacuum_statement)

        for vs in vacuum_statements:
            statements.append(vs[0])

        if not run_commands(conn, statements):
            if not ignore_errors:
                if debug:
                    write("Error running statements: %s" % (str(statements),))
                return ERROR

    statements =[]
    if table_name == None:

        # query for all tables in the schema for vacuum reindex

        comment("Extracting Candidate Tables for vacuum reindex ...")
        get_vacuum_statement = ''' SELECT DISTINCT 'vacuum REINDEX ' + schema_name + '."' + table_name + '" ; ' + '/* ' + ' Table Name : '
                                    + schema_name + '."' + table_name + '",  Rows : ' + CAST("rows" AS VARCHAR(10))
                                    + ',  Interleaved_skew : ' + CAST("max_skew" AS VARCHAR(10))
                                    + ' ,  Reindex Flag : '  + CAST(reindex_flag AS VARCHAR(10)) + ' */ ;'

                                FROM (SELECT TRIM(n.nspname) schema_name, t.relname table_name,
                                                 MAX(v.interleaved_skew) max_skew, MAX(c.count) AS rows,
                                                 CASE
                                                   WHEN (max(c.max_bucket) = 0) OR (MAX(v.interleaved_skew) > 5 AND MAX(c.count) > 10240) THEN 'Yes'
                                                   ELSE 'No'
                                                 END AS reindex_flag
                                            FROM svv_interleaved_columns v
                                            JOIN (SELECT tbl,col, max(compressed_val) AS max_bucket,  SUM(count) AS count
                                                  FROM stv_interleaved_counts
                                                  GROUP BY tbl,col) c
                                            ON (v.tbl = c.tbl AND v.col = c.col)
                                            JOIN pg_class t ON t.oid = c.tbl
                                            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
                                            GROUP BY 1, 2)
                                WHERE reindex_flag = 'Yes'
                                    AND schema_name = '%s'
                                        ''' %(schema_name)

        if debug:
            comment(get_vacuum_statement)

        vacuum_statements = execute_query(get_vacuum_statement)

        for vs in vacuum_statements:
            statements.append(vs[0])

        if not run_commands(conn, statements):
            if not ignore_errors:
                if debug:
                    write("Error running statements: %s" % (str(statements),))
                return ERROR

    return True

def run_analyze(conn):

    statements =[]

    if table_name != None:

        # If it is one table , just check if this needs to be analyzed and prepare analyze statements

        get_analyze_statement_feedback = '''SELECT DISTINCT 'analyze ' + "schema" + '."' + "table" + '" ; '
                                                   + '/* '+ ' Table Name : ' + "schema" + '."' + "table"
                                                   + '",  stats_off : ' + CAST("stats_off" AS VARCHAR(10)) + ' */ ;'
                                                FROM svv_table_info
                                                WHERE   stats_off::DECIMAL (32,4) > %s ::DECIMAL (32,4)
                                                AND  trim("schema") = '%s'
                                                AND  trim("table") = '%s';
                                                ''' % (stats_off_pct,schema_name,table_name,)
    else:

        # query for all tables in the schema
        comment("Extracting Candidate Tables for analyze based on Query Optimizer Alerts(Feedbacks) ...")

        get_analyze_statement_feedback = '''
                                 --Get top N rank tables based on the missing statistics alerts

                                    SELECT DISTINCT 'analyze ' + feedback_tbl.schema_name + '."' + feedback_tbl.table_name + '" ; '
                                    + '/* '+ ' Table Name : ' + info_tbl."schema" + '."' + info_tbl."table"
                                        + '", Stats_Off : ' + CAST(info_tbl."stats_off" AS VARCHAR(10)) + ' */ ;'
                                    FROM ((SELECT TRIM(n.nspname) schema_name,
                                          c.relname table_name
                                   FROM (SELECT TRIM(SPLIT_PART(SPLIT_PART(a.plannode,':',2),' ',2)) AS Table_Name,
                                                COUNT(a.query),
                                                DENSE_RANK() OVER (ORDER BY COUNT(a.query) DESC) AS qry_rnk
                                         FROM stl_explain a,
                                              stl_query b
                                         WHERE a.query = b.query
                                         AND   CAST(b.starttime AS DATE) >= dateadd (DAY,-%s,CURRENT_DATE)
                                         AND   a.userid > 1
                                         AND   a.plannode LIKE '%%missing statistics%%'
                                         AND   a.plannode NOT LIKE '%%_bkp_%%'
                                         GROUP BY Table_Name) miss_tbl
                                     LEFT JOIN pg_class c ON c.relname = TRIM (miss_tbl.table_name)
                                     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                                   WHERE miss_tbl.qry_rnk <= %s)

                                   -- Get the top N rank tables based on the stl_alert_event_log alerts

                                   UNION
                                   SELECT schema_name,
                                          table_name
                                   FROM (SELECT TRIM(n.nspname) schema_name,
                                                c.relname table_name,
                                                DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS qry_rnk,
                                                COUNT(*)
                                         FROM stl_alert_event_log AS l
                                           JOIN (SELECT query,
                                                        tbl,
                                                        perm_table_name
                                                 FROM stl_scan
                                                 WHERE perm_table_name <> 'Internal Worktable'
                                                 GROUP BY query,
                                                          tbl,
                                                          perm_table_name) AS s ON s.query = l.query
                                           JOIN pg_class c ON c.oid = s.tbl
                                           JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                                         WHERE l.userid > 1
                                         AND   l.event_time >= dateadd (DAY,-%s,CURRENT_DATE)
                                         AND   l.Solution LIKE '%%ANALYZE command%%'
                                         GROUP BY TRIM(n.nspname),
                                                  c.relname) anlyz_tbl
                                   WHERE anlyz_tbl.qry_rnk < %s) feedback_tbl
                              JOIN svv_table_info info_tbl
                                ON info_tbl.schema = feedback_tbl.schema_name
                               AND info_tbl.table = feedback_tbl.table_name
                            WHERE info_tbl.stats_off::DECIMAL (32,4) > %s::DECIMAL (32,4)
                            AND   TRIM(info_tbl.schema) = '%s'
                            ORDER BY info_tbl.size ASC  ;
                            ''' % (goback_no_of_days,query_rank,goback_no_of_days,query_rank,stats_off_pct,schema_name)

        #print(get_analyze_statement_feedback)
    if debug:
        comment(get_analyze_statement_feedback)

    analyze_statements = execute_query(get_analyze_statement_feedback)

    for vs in analyze_statements:
        statements.append(vs[0])

    if not run_commands(conn, statements):
                    if not ignore_errors:
                        if debug:
                            write("Error running statements: %s" % (str(statements),))
                        return ERROR

    if table_name == None:

        comment("Extracting Candidate Tables for analyze based on stats off from system table info ...")

        get_analyze_statement = '''SELECT DISTINCT 'analyze ' + "schema" + '."' + "table" + '" ; '
                                        + '/* '+ ' Table Name : ' + "schema" + '."' + "table"
                                        + '", Stats_Off : ' + CAST("stats_off" AS VARCHAR(10)) + ' */ ;'
                                        FROM svv_table_info
                                        WHERE   stats_off::DECIMAL (32,4) > %s::DECIMAL (32,4)
                                        AND  trim("schema") = '%s'
                                        ORDER BY "size" ASC ;
                                        ''' % (stats_off_pct,schema_name)

        if debug:
            comment(get_analyze_statement)

        analyze_statements = execute_query(get_analyze_statement)

        statements =[]
        for vs in analyze_statements:
            statements.append(vs[0])

        if not run_commands(conn, statements):
                if not ignore_errors:
                    if debug:
                        write("Error running statements: %s" % (str(statements),))
                        return ERROR
    return True

def usage(with_message):
    write('Usage: analyze-vacuum-schema.py')
    write('       Runs vacuum AND/OR analyze on table(s) in a schema\n')

    if with_message != None:
        write(with_message + "\n")

    write('Arguments: --db                 - The Database to Use')
    write('           --db-user            - The Database User to connect to')
    write('           --db-pwd             - The Password for the Database User to connect to')
    write('           --db-host            - The Cluster endpoint')
    write('           --db-port            - The Cluster endpoint port : Default = 5439')
    write('           --schema-name        - The Schema to be Analyzed or Vacuumed : Default = public')
    write('           --table-name         - A specific table to be Analyzed or Vacuumed, if --analyze-schema is not desired')
    write('           --output-file        - The full path to the output file to be generated')
    write('           --debug              - Generate Debug Output including SQL Statements being run')
    write('           --slot-count         - Modify the wlm_query_slot_count : Default = 1')
    write('           --ignore-errors      - Ignore errors raised when running and continue processing')
    write('           --query_group        - Set the query_group for all queries')
    write('           --analyze-flag       - Flag to turn ON/OFF ANALYZE functionality (True or False) : Default = True ' )
    write('           --vacuum-flag        - Flag to turn ON/OFF VACUUM functionality (True or False) :  Default = True')
    write('           --vacuum-parameter   - Vacuum parameters [ FULL | SORT ONLY | DELETE ONLY | REINDEX ] Default = FULL')
    write('           --min-unsorted-pct   - Minimum unsorted percentage(%) to consider a table for vacuum : Default = 05%')
    write('           --max-unsorted-pct   - Maximum unsorted percentage(%) to consider a table for vacuum : Default = 50%')
    write('           --deleted-pct        - Minimum deleted percentage (%) to consider a table for vacuum: Default = 05%')
    write('           --stats-off-pct      - Minimum stats off percentage(%) to consider a table for analyze : Default = 10%')
    write('           --max-table-size-mb  - Maximum table size in MB : Default = 700*1024 MB')

    sys.exit(INVALID_ARGS)


def main(argv):
    supported_args = """db= db-user= db-pwd= db-host= db-port= schema-name= table-name= debug= output-file= slot-count= ignore-errors= query_group= analyze-flag= vacuum-flag= vacuum-parameter= min-unsorted-pct= max-unsorted-pct= deleted-pct= stats-off-pct= max-table-size-mb="""

    # extract the command line arguments
    try:
        optlist, remaining = getopt.getopt(argv[1:], "", supported_args.split())
    except getopt.GetoptError as err:
        print str(err)
        usage(None)

    # setup globals
    global master_conn
    global db
    global db_user
    global db_pwd
    global db_host
    global db_port
    global schema_name
    global table_name
    global debug
    global output_file_handle
    global query_slot_count
    global ignore_errors
    global query_group
    global analyze_flag
    global vacuum_flag
    global vacuum_parameter
    global min_unsorted_pct
    global max_unsorted_pct
    global deleted_pct
    global stats_off_pct
    global max_table_size_mb


    output_file = None

    # parse command line arguments
    for arg, value in optlist:
        if arg == "--db":
            if value == '' or value == None:
                usage()
            else:
                db = value
        elif arg == "--db-user":
            if value == '' or value == None:
                usage()
            else:
                db_user = value
        elif arg == "--db-pwd":
            if value == '' or value == None:
                usage()
            else:
                db_pwd = value
        elif arg == "--db-host":
            if value == '' or value == None:
                usage()
            else:
                db_host = value
        elif arg == "--db-port":
            if value != '' and value != None:
                db_port = value
        elif arg == "--schema-name":
            if value != '' and value != None:
                schema_name = value
        elif arg == "--table-name":
            if value != '' and value != None:
                table_name = value
        elif arg == "--debug":
            if value.upper() == 'TRUE':
                debug = True
            else:
                debug = False
        elif arg == "--output-file":
            if value == '' or value == None:
                usage()
            else:
                output_file = value
        elif arg == "--ignore-errors":
            if value.upper() == 'TRUE':
                ignore_errors = True
            else:
                ignore_errors = False
        elif arg == "--slot-count":
            query_slot_count = int(value)
        elif arg == "--query_group":
            if value != '' and value != None:
                query_group = value
        elif arg == "--vacuum-flag":
            if value.upper() == 'FALSE':
                vacuum_flag = False
        elif arg == "--analyze-flag":
            if value.upper()  == 'FALSE':
                analyze_flag = False
        elif arg == "--vacuum-parameter":
            if value.upper() == 'SORT ONLY' or value.upper() == 'DELETE ONLY' or value.upper() == 'REINDEX' :
                vacuum_parameter = value
            else:
                vacuum_parameter = 'FULL'
        elif arg == "--min-unsorted-pct":
            if value != '' and value != None:
                min_unsorted_pct = value
        elif arg == "--max-unsorted-pct":
            if value != '' and value != None:
                max_unsorted_pct = value
        elif arg == "--deleted-pct":
            if value != '' and value != None:
                deleted_pct = value
        elif arg == "--stats-off-pct":
            if value != '' and value != None:
                stats_off_pct = value
        elif arg == "--max-table-size-mb":
            if value != '' and value != None:
                max_table_size_mb = value
        else:
            assert False, "Unsupported Argument " + arg
            usage()

    # Validate that we've got all the args needed
    if db == None:
        usage("Missing Parameter 'db'")
    if db_user == None:
        usage("Missing Parameter 'db-user'")
    if db_pwd == None:
        usage("Missing Parameter 'db-pwd'")
    if db_host == None:
        usage("Missing Parameter 'db-host'")
    if db_port == None:
        usage("Missing Parameter 'db-port'")
    if output_file == None:
        usage("Missing Parameter 'output-file'")


    # get the database password
    #db_pwd = getpass.getpass("Password <%s>: " % db_user)

    # open the output file
    output_file_handle = open(output_file,'w')

    # get a connection for the controlling processes
    master_conn = get_pg_conn()

    if master_conn == None:
        sys.exit(NO_CONNECTION)

    comment("Connected to %s:%s:%s as %s" % (db_host, db_port, db, db_user))

    if vacuum_flag != False:
        # Run vacuum based on the Unsorted , Stats off and Size of the table
        run_vacuum(master_conn)
    else:
        comment("vacuum flag arg is set as %s.Vacuum is not performed." % (vacuum_flag))

    if analyze_flag != False:
        # Run Analyze based on the  Stats off Metrics table
        run_analyze(master_conn)
    else:
        comment("anlayze flag arg is set as %s.Analyze is not performed." % (analyze_flag))

    comment('Processing Complete')
    cleanup()

if __name__ == "__main__":
    main(sys.argv)
