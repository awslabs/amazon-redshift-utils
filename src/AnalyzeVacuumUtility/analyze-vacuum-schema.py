#!/usr/bin/env python
from __future__ import print_function

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
09/01/2017 : Fixed issues with interleaved sort key tables per https://github.com/awslabs/amazon-redshift-utils/issues/184
11/09/2017 : Refactored to support running in AWS Lambda
'''
import os
import sys

# add the lib directory to the sys path
try:
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
except:
    pass

import getopt
import analyze_vacuum

__version__ = ".9.1.4"

OK = 0
ERROR = 1
INVALID_ARGS = 2
NO_WORK = 3
TERMINATED_BY_USER = 4
NO_CONNECTION = 5


def get_env_var(name, defaultVal):
    return os.environ[name] if name in os.environ else defaultVal


def usage(with_message=None):
    print('Usage: analyze-vacuum-schema.py')
    print('       Runs vacuum AND/OR analyze on table(s) in a schema\n')

    if with_message is not None:
        print(with_message + "\n")

    print('Arguments: --db                 - The Database to Use')
    print('           --db-user            - The Database User to connect to')
    print('           --db-pwd             - The Password for the Database User to connect to')
    print('           --db-host            - The Cluster endpoint')
    print('           --db-port            - The Cluster endpoint port : Default = 5439')
    print('           --db-conn-opts       - Additional connection options. "name1=opt1[ name2=opt2].."')
    print('           --require-ssl        - Does the connection require SSL? (true | false)')
    print('           --schema-name        - The Schema to be Analyzed or Vacuumed : Default = public')
    print('           --table-name         - A specific table to be Analyzed or Vacuumed, if --analyze-schema is not desired')
    print('           --blacklisted-tables - The tables we do not want to Vacuum')
    print('           --output-file        - The full path to the output file to be generated')
    print('           --debug              - Generate Debug Output including SQL Statements being run')
    print('           --slot-count         - Modify the wlm_query_slot_count : Default = 1')
    print('           --ignore-errors      - Ignore errors raised when running and continue processing')
    print('           --query_group        - Set the query_group for all queries')
    print('           --analyze-flag       - Flag to turn ON/OFF ANALYZE functionality (True or False) : Default = True ' )
    print('           --vacuum-flag        - Flag to turn ON/OFF VACUUM functionality (True or False) :  Default = True')
    print('           --vacuum-parameter   - Vacuum parameters [ FULL | SORT ONLY | DELETE ONLY | REINDEX ] Default = FULL')
    print('           --min-unsorted-pct   - Minimum unsorted percentage(%) to consider a table for vacuum : Default = 05%')
    print('           --max-unsorted-pct   - Maximum unsorted percentage(%) to consider a table for vacuum : Default = 50%')
    print('           --deleted-pct        - Minimum deleted percentage (%) to consider a table for vacuum: Default = 05%')
    print('           --stats-off-pct      - Minimum stats off percentage(%) to consider a table for analyze : Default = 10%')
    print('           --predicate-cols     - Analyze predicate columns only')
    print('           --max-table-size-mb  - Maximum table size in MB : Default = 700*1024 MB')
    print('           --min-interleaved-skew   - Minimum index skew to consider a table for vacuum reindex: Default = 1.4')
    print('           --min-interleaved-cnt   - Minimum stv_interleaved_counts records to consider a table for vacuum reindex: Default = 0')

    sys.exit(INVALID_ARGS)


def main(argv):
    master_conn = None
    db_connections = {}
    db = get_env_var('PGDATABASE', None)
    db_user = get_env_var('PGUSER', None)
    db_pwd = get_env_var('PGPASSWORD', None)
    db_host = get_env_var('PGHOST', None)
    db_port = get_env_var('PGPORT', 5439)
    schema_name = 'public'
    table_name = None
    blacklisted_tables = None
    debug = False
    do_execute = False
    query_slot_count = 1
    ignore_errors = False
    query_group = None
    analyze_flag = True
    vacuum_flag = True
    require_ssl = False

    supported_args = """db= db-user= db-pwd= db-host= db-port= schema-name= table-name= blacklisted-tables= require-ssl= debug= output-file= slot-count= ignore-errors= query_group= analyze-flag= vacuum-flag= vacuum-parameter= min-unsorted-pct= max-unsorted-pct= deleted-pct= stats-off-pct= predicate-cols= max-table-size-mb= min-interleaved-skew= min-interleaved-cnt="""

    # extract the command line arguments
    try:
        optlist, remaining = getopt.getopt(argv[1:], "", supported_args.split())
    except getopt.GetoptError as err:
        print(str(err))
        usage()

    output_file = None

    # parse command line arguments
    for arg, value in optlist:
        if arg == "--db":
            if value == '' or value is None:
                usage()
            else:
                db = value
        elif arg == "--db-user":
            if value == '' or value is None:
                usage()
            else:
                db_user = value
        elif arg == "--db-pwd":
            if value == '' or value is None:
                usage()
            else:
                db_pwd = value
        elif arg == "--db-host":
            if value == '' or value is None:
                usage()
            else:
                db_host = value
        elif arg == "--db-port":
            if value != '' and value is not None:
                db_port = value
        elif arg == "--require-ssl":
            if value != '' and value != None:
                if value.upper() == 'TRUE' or value == '1':
                    require_ssl = True
        elif arg == "--schema-name":
            if value != '' and value is not None:
                schema_name = value
        elif arg == "--table-name":
            if value != '' and value is not None:
                table_name = value
        elif arg == "--blacklisted-tables":
            if value != '' and value is not None:
                blacklisted_tables = value
        elif arg == "--debug":
            if value.upper() == 'TRUE':
                debug = True
            else:
                debug = False
        elif arg == "--output-file":
            output_file = value
        elif arg == "--ignore-errors":
            if value.upper() == 'TRUE':
                ignore_errors = True
            else:
                ignore_errors = False
        elif arg == "--slot-count":
            query_slot_count = int(value)
        elif arg == "--query_group":
            if value != '' and value is not None:
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
            if value != '' and value is not None:
                min_unsorted_pct = value
        elif arg == "--max-unsorted-pct":
            if value != '' and value is not None:
                max_unsorted_pct = value
        elif arg == "--deleted-pct":
            if value != '' and value is not None:
                deleted_pct = value
        elif arg == "--stats-off-pct":
            if value != '' and value is not None:
                stats_off_pct = value
        elif arg == "--predicate-cols":
            if value.upper() == 'TRUE':
                predicate_cols = True
            else:
                predicate_cols = False
        elif arg == "--max-table-size-mb":
            if value != '' and value is not None:
                max_table_size_mb = value
        elif arg == "--min-interleaved-skew":
            if value != '' and value is not None:
                min_interleaved_skew = value
        elif arg == "--min-interleaved-cnt":
            if value != '' and value is not None:
                min_interleaved_cnt = value
        else:
            assert False, "Unsupported Argument " + arg
            usage()

    # Validate that we've got all the args needed
    if db is None:
        usage("Missing Parameter 'db'")
    if db_user is None:
        usage("Missing Parameter 'db-user'")
    if db_pwd is None:
        usage("Missing Parameter 'db-pwd'")
    if db_host is None:
        usage("Missing Parameter 'db-host'")
    if db_port is None:
        usage("Missing Parameter 'db-port'")

    if (output_file is not None):
        sys.stdout = open(output_file,'w')

    # invoke the main method of the utility
    result = analyze_vacuum.run_analyze_vacuum(db_host, db_port, db_user, db_pwd, db, query_group, query_slot_count, vacuum_flag, analyze_flag, schema_name, table_name, blacklisted_tables, ignore_errors, require_ssl)

    if (result is not None):
        sys.exit(result)
    else:
        sys.exit(0)
    
if __name__ == "__main__":
    main(sys.argv)
