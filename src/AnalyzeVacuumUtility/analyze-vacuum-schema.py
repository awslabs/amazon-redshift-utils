#!/usr/bin/env python
from __future__ import print_function

'''
analyze-vacuum-schema.py
* Copyright 2015, Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0

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
14/12/2017 : Refactored to support a more sensible interface style with kwargs
'''
import os
import sys
import argparse

# add the lib directory to the sys path
try:
    sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
except Exception:
    pass

import getopt
import analyze_vacuum
import config_constants

__version__ = ".10"

OK = 0
ERROR = 1
INVALID_ARGS = 2
NO_WORK = 3
TERMINATED_BY_USER = 4
NO_CONNECTION = 5

# setup cli args
parser = argparse.ArgumentParser()
parser.add_argument("--analyze-flag", dest="analyze_flag", required=True, default='False',
                    help="Flag to turn ON/OFF ANALYZE functionality (True or False): Default = False ")
parser.add_argument("--max-unsorted-pct", dest="max_unsorted_pct",
                    help="Maximum unsorted percentage( to consider a table for vacuum : Default = 50")
parser.add_argument("--min-interleaved-cnt", dest="min_interleaved_cnt", type=int,
                    help="Minimum stv_interleaved_counts records to consider a table for vacuum reindex: Default = 0")
parser.add_argument("--min-interleaved-skew", dest="min_interleaved_skew",
                    help="Minimum index skew to consider a table for vacuum reindex: Default = 1.4")
parser.add_argument("--min-unsorted-pct", dest="min_unsorted_pct",
                    help="Minimum unsorted percentage( to consider a table for vacuum : Default = 5")
parser.add_argument("--stats-off-pct ", dest="stats_off_pct",
                    help="Minimum stats off percentage( to consider a table for analyze : Default = 10")
parser.add_argument("--table-name", dest="table_name",
                    help="A specific table to be Analyzed or Vacuumed if analyze-schema is not desired")
parser.add_argument("--vacuum-flag", dest="vacuum_flag", required=True, default='False',
                    help="Flag to turn ON/OFF VACUUM functionality (True or False): Default = False")
parser.add_argument("--vacuum-parameter", dest="vacuum_parameter",
                    help="Vacuum parameters [ FULL | SORT ONLY | DELETE ONLY | REINDEX ] Default = FULL")
parser.add_argument("--blacklisted-tables", dest="blacklisted_tables", help="The tables we do not want to Vacuum")
parser.add_argument("--db-conn-opts", dest="db_conn_opts",
                    help="Additional connection options. name1=opt1[ name2=opt2]..")
parser.add_argument("--db-host", dest="db_host", required=True, help="The Cluster endpoint")
parser.add_argument("--db-port", dest="db_port", type=int, required=True,
                    help="The Cluster endpoint port : Default = 5439")
parser.add_argument("--db-pwd", dest="db_pwd", help="The Password for the Database User to connect to")
parser.add_argument("--db-user", dest="db_user", required=True, help="The Database User to connect to")
parser.add_argument("--debug ", dest="debug", default=False,
                    help="Generate Debug Output including SQL Statements being run")
parser.add_argument("--ignore-errors", dest="ignore_errors", default=True,
                    help="Ignore errors raised when running and continue processing")
parser.add_argument("--max-table-size-mb", dest="max_table_size_mb", type=int,
                    help="Maximum table size in MB : Default = 700*1024 MB")
parser.add_argument("--output-file", dest="output_file", help="The full path to the output file to be generated")
parser.add_argument("--predicate-cols", dest="predicate_cols", help="Analyze predicate columns only")
parser.add_argument("--query-group", dest="query_group", help="Set the query_group for all queries")
parser.add_argument("--schema-name", dest="schema_name",
                    help="The Schema to be Analyzed or Vacuumed (REGEX: Default = public")
parser.add_argument("--slot-count", dest="slot_count", help="Modify the wlm_query_slot_count : Default = 1")
parser.add_argument("--suppress-cloudwatch", dest="suppress_cw",
                    help="Don't emit CloudWatch metrics for analyze or vacuum when set to True")
parser.add_argument("--db", dest="db", help="The Database to Use")
full_args = parser.parse_args()

parse_args = {}
# remove args that end up as None
for k, v in vars(full_args).items():
    if v is not None:
        parse_args[k] = v


def main():
    args = {}

    # add argparse args
    args.update(parse_args)

    if args.get(config_constants.OUTPUT_FILE) is not None:
        sys.stdout = open(args.get(config_constants.OUTPUT_FILE), 'w')

    # invoke the main method of the utility
    result = analyze_vacuum.run_analyze_vacuum(**args)

    if result is not None:
        sys.exit(result)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
