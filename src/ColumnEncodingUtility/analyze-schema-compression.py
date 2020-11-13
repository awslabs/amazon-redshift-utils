#!/usr/bin/env python3

'''
analyze-schema-compression.py

* Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

Analyses all tables in a Redshift Cluster Schema, and outputs a SQL script to
migrate database tables with sub-optimal column encodings to optimal column
encodings as recommended by the database engine.

The processing model that the script will generate is:
    create new table XXX_$mig
    insert select * from old table into new table
    analyze new table
    rename old table to XXX_$old or drop table
    rename new table to old table

Use with caution on a running system


Ian Meyers
Amazon Web Services (2014)
'''
import getopt
import getpass
import os
import sys

try:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
except:
    pass

import config_constants
from column_encoder import ColumnEncoder

thismodule = sys.modules[__name__]


def usage(with_message):
    print('Usage: analyze-schema-compression.py')
    print('       Generates a script to optimise Redshift column encodings on all tables in a schema\n')

    if with_message is not None:
        print(with_message + "\n")

    print('Arguments: --db                  - The Database to Use')
    print('           --db-user             - The Database User to connect to')
    print('           --db-pwd              - The Password for the Database User to connect to')
    print('           --db-host             - The Cluster endpoint')
    print('           --db-port             - The Cluster endpoint port (default 5439)')
    print('           --analyze-schema      - The Schema to be Analyzed (default public)')
    print('           --analyze-table       - A specific table to be Analyzed, if --analyze-schema is not desired')
    print('           --analyze-cols        - Analyze column width and reduce the column width if needed')
    print('           --new-varchar-min     - Set minimum varchar length for new width (to be used with --analyze-cols)')
    print('           --new-dist-key        - Set a new Distribution Key (only used if --analyze-table is specified)')
    print('           --new-sort-keys       - Set a new Sort Key using these comma separated columns (Compound Sort key only , and only used if --analyze-table is specified)')
    print('           --target-schema       - Name of a Schema into which the newly optimised tables and data should be created, rather than in place')
    print('           --threads             - The number of concurrent connections to use during analysis (default 2)')
    print('           --output-file         - The full path to the output file to be generated')
    print('           --debug               - Generate Debug Output including SQL Statements being run')
    print('           --do-execute          - Run the compression encoding optimisation')
    print('           --slot-count          - Modify the wlm_query_slot_count from the default of 1')
    print('           --ignore-errors       - Ignore errors raised in threads when running and continue processing')
    print('           --force               - Force table migration even if the table already has Column Encoding applied')
    print('           --drop-old-data       - Drop the old version of the data table, rather than renaming')
    print('           --comprows            - Set the number of rows to use for Compression Encoding Analysis')
    print('           --query_group         - Set the query_group for all queries')
    print('           --ssl-option          - Set SSL to True or False (default False)')
    print('           --statement-timeout   - Set the runtime statement timeout in milliseconds (default 1200000)')
    print('           --suppress-cloudwatch - Set to True to suppress CloudWatch Metrics being created when --do-execute is True')
    sys.exit(config_constants.INVALID_ARGS)

def main(argv):
    supported_args = """db= db-user= db-pwd= db-host= db-port= target-schema= analyze-schema= analyze-table= new-dist-key= new-sort-keys= analyze-cols= new-varchar-min= threads= debug= output-file= do-execute= slot-count= ignore-errors= force= drop-old-data= comprows= query_group= ssl-option= suppress-cloudwatch= statement-timeout="""

    # extract the command line arguments
    try:
        optlist, remaining = getopt.getopt(argv[1:], "", supported_args.split())
    except getopt.GetoptError as err:
        print(str(err))
        usage(None)

    # parse command line arguments
    args = {}
    for arg, value in optlist:
        if arg == "--db":
            if value == '' or value is None:
                usage()
            else:
                args[config_constants.DB_NAME] = value
        elif arg == "--db-user":
            if value == '' or value is None:
                usage()
            else:
                args[config_constants.DB_USER] = value
        elif arg == "--db-host":
            if value == '' or value is None:
                usage()
            else:
                args[config_constants.DB_HOST] = value
        elif arg == "--db-port":
            if value != '' and value is not None:
                args[config_constants.DB_PORT] = int(value)
        elif arg == "--db-pwd":
            if value != '' and value is not None:
                args[config_constants.DB_PASSWORD] = value
        elif arg == "--analyze-schema":
            if value != '' and value is not None:
                args[config_constants.SCHEMA_NAME] = value
        elif arg == "--analyze-table":
            if value != '' and value is not None:
                args[config_constants.TABLE_NAME] = value
        elif arg == "--new-dist-key":
            if value != '' and value is not None:
                args['new_dist_key'] = value
        elif arg == "--new-sort-keys":
            if value != '' and value is not None:
                args['new_sort_keys'] = value
        elif arg == "--analyze-cols":
            if value != '' and value is not None:
                args['analyze_col_width'] = value
        elif arg == "--new-varchar-min":
            if value != '' and value is not None:
                args['new_varchar_min'] = int(value)
        elif arg == "--target-schema":
            if value != '' and value is not None:
                args[config_constants.TARGET_SCHEMA] = value
        elif arg == "--threads":
            if value != '' and value is not None:
                args[config_constants.THREADS] = int(value)
        elif arg == "--debug":
            if value == 'true' or value == 'True':
                args[config_constants.DEBUG] = True
            else:
                args[config_constants.DEBUG] = False
        elif arg == "--output-file":
            sys.stdout = open(value, 'w')
        elif arg == "--ignore-errors":
            if value == 'true' or value == 'True':
                args[config_constants.IGNORE_ERRORS] = True
            else:
                args[config_constants.IGNORE_ERRORS] = False
        elif arg == "--force":
            if value == 'true' or value == 'True':
                args[config_constants.FORCE] = True
            else:
                args[config_constants.FORCE] = False
        elif arg == "--drop-old-data":
            if value == 'true' or value == 'True':
                args[config_constants.DROP_OLD_DATA] = True
            else:
                args[config_constants.DROP_OLD_DATA] = False
        elif arg == "--do-execute":
            if value == 'true' or value == 'True':
                args[config_constants.DO_EXECUTE] = True
            else:
                args[config_constants.DO_EXECUTE] = False
        elif arg == "--slot-count":
            args[config_constants.QUERY_SLOT_COUNT] = int(value)
        elif arg == "--comprows":
            args[config_constants.COMPROWS] = int(value)
        elif arg == "--query_group":
            if value != '' and value is not None:
                args[config_constants.QUERY_GROUP] = value
        elif arg == "--ssl-option":
            if value == 'true' or value == 'True':
                args[config_constants.SSL] = True
            else:
                args[config_constants.SSL] = False
        elif arg == "--suppress-cloudwatch":
            if value == 'true' or value == 'True':
                args[config_constants.SUPPRESS_CLOUDWATCH] = True
            else:
                args[config_constants.SUPPRESS_CLOUDWATCH] = False
        elif arg == "--statement-timeout":
            if value != '' and value is not None:
                try:
                    args[config_constants.STATEMENT_TIMEOUT] = str(int(value))
                except ValueError:
                    pass
        else:
            print("Unsupported Argument " + arg)
            usage()

    # Validate that we've got all the args needed
    if config_constants.DB_NAME not in args:
        usage("Missing Parameter 'db'")
    if config_constants.DB_USER not in args:
        usage("Missing Parameter 'db-user'")
    if config_constants.DB_HOST not in args:
        usage("Missing Parameter 'db-host'")
    if config_constants.DB_PORT not in args:
        args[config_constants.DB_PORT] = 5439
    if config_constants.SCHEMA_NAME not in args:
        args[config_constants.SCHEMA_NAME] = 'public'

    # Reduce to 1 thread if we're analyzing a single table
    if config_constants.TABLE_NAME in args:
        args[config_constants.THREADS] = 1

    # get the database password
    if config_constants.DB_PASSWORD not in args:
        args[config_constants.DB_PASSWORD] = getpass.getpass("Password <%s>: " % args[config_constants.DB_USER])

    # setup the configuration
    encoder = ColumnEncoder(**args)

    # run the analyser
    result_code = encoder.run()

    # exit based on the provided return code
    return result_code


if __name__ == "__main__":
    main(sys.argv)
