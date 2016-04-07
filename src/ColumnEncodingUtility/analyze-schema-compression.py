#!/usr/bin/env python

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

import sys
from multiprocessing import Pool
import pg
import getopt
import os
import re
import getpass
import time
import traceback

__version__ = ".9.1.5"

OK = 0
ERROR = 1
INVALID_ARGS = 2
NO_WORK = 3
TERMINATED_BY_USER = 4
NO_CONNECTION = 5

# timeout for retries - 100ms
RETRY_TIMEOUT = 100. / 1000
    
# compiled regular expressions
IDENTITY_RE = re.compile(r'"identity"\((?P<current>.*), (?P<base>.*), \'(?P<seed>\d+),(?P<step>\d+)\'.*\)')

def get_env_var(name, defaultVal):
    return os.environ[name] if name in os.environ else defaultVal

master_conn = None
db_connections = {}
db = get_env_var('PGDATABASE', None)
db_user = get_env_var('PGUSER', None)
db_pwd = None
db_host = get_env_var('PGHOST', None)
db_port = get_env_var('PGPORT', 5439)
analyze_schema = 'public'
target_schema = None
analyze_table = None
debug = False
threads = 2
output_file_handle = None
do_execute = False
query_slot_count = 1
ignore_errors = False
force = False
drop_old_data = False
comprows = None
query_group = None

def execute_query(str):
    conn = get_pg_conn()
    result = None
    query_result = conn.query(str)
    
    if query_result is not None:
        result = query_result.getresult()
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
    if (string != None):
        if re.match('.*\\n.*', string) != None:
            write('/* [%s]\n%s\n*/\n' % (str(os.getpid()), string))
        else:
            write('-- [%s] %s' % (str(os.getpid()), string))

def print_statements(statements):
    if statements != None:
        for s in statements:
            if s != None:
                write(s)
        
def write(s):
    # write output to all the places we want it
    print(s)
    if output_file_handle != None:
        output_file_handle.write(str(s) + "\n")
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
            comment('Connect [%s] %s:%s:%s:%s' % (pid, db_host, db_port, db, db_user))
            
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
        search_path = 'set search_path = \'$user\',public,%s' % (analyze_schema)
        if target_schema != None and target_schema != analyze_schema:
            search_path = search_path + ', %s' % (target_schema)
            
        if debug:
            comment(search_path)
        
        try:
            conn.query(search_path)
        except pg.ProgrammingError as e:
            if re.match('schema "%s" does not exist' % (analyze_schema,), e.message) != None:
                write('Schema %s does not exist' % (analyze_schema,))
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
        set_timeout = "set statement_timeout = '1200000'"
        if debug:
            comment(set_timeout)
            
        conn.query(set_timeout)
        
        # cache the connection
        db_connections[pid] = conn
        
    return conn


def get_table_attribute(description_list, column_name, index):
    # get a specific value requested from the table description structure based on the index and column name
    # runs against output from get_table_desc()
    for item in description_list:
        if item[0] == column_name:
            return item[index]


def get_identity(adsrc):
    # checks if a column defined by adsrc (column from pg_attrdef) is
    # an identity, since both identities and defaults end up in this table
    # if is identity returns (seed, step); if not returns None
    # TODO there ought be a better way than using a regex
    m = IDENTITY_RE.match(adsrc)
    if m:
        return m.group('seed'), m.group('step')
    else:
        return None


def get_foreign_keys(analyze_schema, target_schema, table_name):
    has_fks = False
    
    fk_statement = '''SELECT conname,
  pg_catalog.pg_get_constraintdef(cons.oid, true) as condef
 FROM pg_catalog.pg_constraint cons,
 pg_namespace pgn,
 pg_class pgc
 WHERE cons.conrelid = pgc.oid
 and pgn.nspname = '%s'
 and pgc.relnamespace = pgn.oid
 and pgc.oid = '%s'::regclass
 AND cons.contype = 'f'
 ORDER BY 1
''' % (analyze_schema, table_name)

    if (debug):
        comment(fk_statement)
    
    foreign_keys = execute_query(fk_statement)
    fk_statements = []
    
    for fk in foreign_keys:
        has_fks = True
        references_clause = fk[1].replace('REFERENCES ', 'REFERENCES %s.' % (target_schema))      
        fk_statements.append('alter table %s."%s" add constraint %s %s;' % (target_schema, table_name, fk[0], references_clause))
    
    if has_fks:
        return fk_statements
    else:
        return None
            
def get_primary_key(table_schema, target_schema, original_table, new_table):
    pk_statement = 'alter table %s."%s" add primary key (' % (target_schema, new_table)
    has_pks = False
    
    # get the primary key columns
    statement = '''SELECT       
  att.attname
FROM pg_index ind, pg_class cl, pg_attribute att, pg_namespace pgn
WHERE 
  cl.oid = '%s'::regclass 
  AND ind.indrelid = cl.oid 
  AND att.attrelid = cl.oid
  and cl.relnamespace = pgn.oid
  and pgn.nspname = '%s'
  and (ind.indkey[0] = att.attnum or 
       ind.indkey[1] = att.attnum or
       ind.indkey[2] = att.attnum or
       ind.indkey[3] = att.attnum or
       ind.indkey[4] = att.attnum
      )
  and attnum > 0
  AND ind.indisprimary
order by att.attnum;
''' % (original_table, table_schema)

    if debug:
        comment(statement)
            
    pks = execute_query(statement)
    
    for pk in pks:
        has_pks = True
        pk_statement = pk_statement + pk[0] + ','
        
    pk_statement = pk_statement[:-1] + ');'
    
    if has_pks:
        return pk_statement
    else:
        return None
    
        
def get_table_desc(table_name):
    # get the table definition from the dictionary so that we can get relevant details for each column
    statement = '''select "column", type, encoding, distkey, sortkey, "notnull", ad.adsrc
 from pg_table_def de, pg_attribute at LEFT JOIN pg_attrdef ad ON (at.attrelid, at.attnum) = (ad.adrelid, ad.adnum)
 where de.schemaname = '%s'
 and de.tablename = '%s'
 and at.attrelid = '%s.%s'::regclass
 and de.column = at.attname
''' % (analyze_schema, table_name, analyze_schema, table_name)

    if debug:
        comment(statement)
        
    description = execute_query(statement)
    
    return description

def run_commands(conn, commands):
    for c in commands:
        if c != None:
            comment('[%s] Running %s' % (str(os.getpid()), c))
            try:
                conn.query(c)
                comment('Success.')
            except Exception as e:
                # cowardly bail on errors
                rollback()
                write(traceback.format_exc())
                return False
    
    return True
        
def analyze(table_info):     
    table_name = table_info[0]
    dist_style = table_info[3]
    
    statement = 'analyze compression %s."%s"' % (analyze_schema, table_name)
    
    if comprows != None:
        statement = statement + (" comprows %s" % (comprows,))
        
    try:
        if debug:
            comment(statement)
            
        comment("Analyzing Table '%s'" % (table_name,))
    
        output = None
        analyze_retry = 10
        attempt_count = 0
        last_exception = None
        while attempt_count < analyze_retry and output == None:
            try:
                output = execute_query(statement)
            except KeyboardInterrupt:
                # To handle Ctrl-C from user
                cleanup()
                sys.exit(TERMINATED_BY_USER)
            except Exception as e:
                write(e)
                attempt_count += 1
                last_exception = e
                rollback()
                
                # Exponential Backoff
                time.sleep(2 ** attempt_count * RETRY_TIMEOUT)

        if output == None:
            write("Unable to analyze %s due to Exception %s" % (table_name, last_exception.message))
            return ERROR
        
        if target_schema == analyze_schema:
            target_table = '%s_$mig' % (table_name,)
        else:
            target_table = table_name
        
        create_table = 'begin;\nlock table %s."%s";\ncreate table %s."%s" (' % (analyze_schema, table_name, target_schema, target_table,)
        
        # query the table column definition
        descr = get_table_desc(table_name)
        found_non_raw = False
        encode_columns = []
        statements = []
        sortkeys = {}
        has_zindex_sortkeys = False
        has_identity = False
        non_identity_columns = []
        
        # process each item given back by the analyze request
        for row in output:
            col = row[1]
            
            # only proceed with generating an output script if we found any non-raw column encodings
            if row[2] != 'raw':
                found_non_raw = True
            
            col_type = get_table_attribute(descr, col, 1)
            # fix datatypes
            col_type = col_type.replace('character varying', 'varchar').replace('without time zone', '')
            
            # is this the dist key?
            distkey = get_table_attribute(descr, col, 3)
            if str(distkey).upper() == 'T':
                distkey = 'DISTKEY'
            else:
                distkey = ''
                
            # is this the sort key?
            sortkey = get_table_attribute(descr, col, 4)
            if sortkey != 0:
                # add the absolute ordering of the sortkey to the list of all sortkeys
                sortkeys[abs(sortkey)] = col
                
                if (sortkey < 0):
                    has_zindex_sortkeys = True
                
            # don't compress first sort key
            if abs(sortkey) == 1:
                compression = 'RAW'
            else:
                compression = row[2]
                
            # extract null/not null setting
            col_null = get_table_attribute(descr, col, 5)
            
            if str(col_null).upper() == 'T':
                col_null = 'NOT NULL'
            else:
                col_null = ''

            # get default or identity syntax for this column
            default_or_identity = get_table_attribute(descr, col, 6)
            if default_or_identity:
                ident_data = get_identity(default_or_identity)
                if ident_data is None:
                    default_value = 'default %s' % default_or_identity
                    non_identity_columns.append(col)
                else:
                    default_value = 'identity (%s, %s)' % ident_data
                    has_identity = True
            else:
                default_value = ''
                non_identity_columns.append('"' + col + '"')

            # add the formatted column specification
            encode_columns.extend(['"%s" %s %s %s encode %s %s'
                                   % (col, col_type, default_value, col_null, compression, distkey)])

        fks = None 
        if found_non_raw or force:
            comment("Column Encoding will be modified for %s.%s" % (analyze_schema, table_name))
            
            # add all the column encoding statements on to the create table statement, suppressing the leading comma on the first one
            for i, s in enumerate(encode_columns):
                create_table += '\n%s%s' % ('' if i == 0 else ',', s)
    
            create_table = create_table + '\n)\n'
            
            # add diststyle all if needed
            if dist_style == 'ALL':
                create_table = create_table + 'diststyle all\n'
                
            # add sort key as a table block to accommodate multiple columns
            if len(sortkeys) > 0:
                sortkey = '%sSORTKEY(' % ('INTERLEAVED ' if has_zindex_sortkeys else '')    
                
                for i in range(1, len(sortkeys) + 1):
                    sortkey = sortkey + sortkeys[i]
                   
                    if i != len(sortkeys):
                       sortkey = sortkey + ','
                    else:
                       sortkey = sortkey + ')\n'
                create_table = create_table + (' %s ' % sortkey)                
            
            create_table = create_table + ';'
            
            # run the create table statement
            statements.extend([create_table])         
            
            # get the primary key statement
            statements.extend([get_primary_key(analyze_schema, target_schema, table_name, target_table)]);

            # insert the old data into the new table
            # if we have identity column(s), we can't insert data from them, so do selective insert
            if has_identity:
                source_columns = ', '.join(non_identity_columns)
                mig_columns = '(' + source_columns + ')'
            else:
                source_columns = '*'
                mig_columns = ''

            insert = 'insert into %s."%s" %s select %s from %s."%s";' % (target_schema,
                                                                     target_table,
                                                                     mig_columns,
                                                                     source_columns,
                                                                     analyze_schema,
                                                                     table_name)
            statements.extend([insert])
                    
            # analyze the new table
            analyze = 'analyze %s."%s";' % (target_schema, target_table)
            statements.extend([analyze])
                    
            if (target_schema == analyze_schema):
                # rename the old table to _$old or drop
                if drop_old_data:
                    drop = 'drop table %s."%s" cascade;' % (target_schema, table_name)
                else:
                    drop = 'alter table %s."%s" rename to "%s";' % (target_schema, table_name, table_name + "_$old")
                
                statements.extend([drop])
                        
                # rename the migrate table to the old table name
                rename = 'alter table %s."%s" rename to "%s";' % (target_schema, target_table, table_name)
                statements.extend([rename])
            
            # add foreign keys
            fks = get_foreign_keys(analyze_schema, target_schema, table_name)
            
            statements.extend(['commit;'])
            
            if do_execute:
                if not run_commands(get_pg_conn(), statements):
                    if not ignore_errors:
                        if debug:
                            write("Error running statements: %s" % (str(statements),))
                        return ERROR
        else:
            comment("No encoding modifications required for %s.%s" % (analyze_schema, table_name))    
    except Exception as e:
        write('Exception %s during analysis of %s' % (e.message, table_name))
        write(traceback.format_exc())
        return ERROR
    
    print_statements(statements)
    
    return (OK, fks)

def usage(with_message):
    write('Usage: analyze-schema-compression.py')
    write('       Generates a script to optimise Redshift column encodings on all tables in a schema\n')
    
    if with_message != None:
        write(with_message + "\n")
        
    write('Arguments: --db             - The Database to Use')
    write('           --db-user        - The Database User to connect to')
    write('           --db-host        - The Cluster endpoint')
    write('           --db-port        - The Cluster endpoint port (default 5439)')
    write('           --analyze-schema - The Schema to be Analyzed (default public)')
    write('           --analyze-table  - A specific table to be Analyzed, if --analyze-schema is not desired')
    write('           --target-schema  - Name of a Schema into which the newly optimised tables and data should be created, rather than in place')
    write('           --threads        - The number of concurrent connections to use during analysis (default 2)')
    write('           --output-file    - The full path to the output file to be generated')
    write('           --debug          - Generate Debug Output including SQL Statements being run')
    write('           --do-execute     - Run the compression encoding optimisation')
    write('           --slot-count     - Modify the wlm_query_slot_count from the default of 1')
    write('           --ignore-errors  - Ignore errors raised in threads when running and continue processing')
    write('           --force          - Force table migration even if the table already has Column Encoding applied')
    write('           --drop-old-data  - Drop the old version of the data table, rather than renaming')
    write('           --comprows       - Set the number of rows to use for Compression Encoding Analysis')
    write('           --query_group    - Set the query_group for all queries')
    sys.exit(INVALID_ARGS)
    

def main(argv):
    supported_args = """db= db-user= db-host= db-port= target-schema= analyze-schema= analyze-table= threads= debug= output-file= do-execute= slot-count= ignore-errors= force= drop-old-data= comprows= query_group="""
    
    # extract the command line arguments
    try:
        optlist, remaining = getopt.getopt(sys.argv[1:], "", supported_args.split())
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
    global threads
    global analyze_schema
    global analyze_table
    global target_schema
    global debug
    global output_file_handle
    global do_execute
    global query_slot_count
    global ignore_errors
    global force
    global drop_old_data
    global comprows
    global query_group
    
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
        elif arg == "--db-host":
            if value == '' or value == None:
                usage()
            else:
                db_host = value
        elif arg == "--db-port":
            if value != '' and value != None:
                db_port = value
        elif arg == "--analyze-schema":
            if value != '' and value != None:
                analyze_schema = value
        elif arg == "--analyze-table":
            if value != '' and value != None:
                analyze_table = value
        elif arg == "--target-schema":
            if value != '' and value != None:
                target_schema = value
        elif arg == "--threads":
            if value != '' and value != None:
                threads = int(value)
        elif arg == "--debug":
            if value == 'true' or value == 'True':
                debug = True
            else:
                debug = False
        elif arg == "--output-file":
            if value == '' or value == None:
                usage()
            else:
                output_file = value
        elif arg == "--ignore-errors":
            if value == 'true' or value == 'True':
                ignore_errors = True
            else:
                ignore_errors = False
        elif arg == "--force":
            if value == 'true' or value == 'True':
                force = True
            else:
                force = False
        elif arg == "--drop-old-data":
            if value == 'true' or value == 'True':
                drop_old_data = True
            else:
                drop_old_data = False
        elif arg == "--do-execute":
            if value == 'true' or value == 'True':
                do_execute = True
            else:
                do_execute = False
        elif arg == "--slot-count":
            query_slot_count = int(value)
        elif arg == "--comprows":
            comprows = int(value)
        elif arg == "--query_group":
            if value != '' and value != None:
                query_group = value
        else:
            assert False, "Unsupported Argument " + arg
            usage()
    
    # Validate that we've got all the args needed
    if db == None:
        usage("Missing Parameter 'db'")
    if db_user == None:
        usage("Missing Parameter 'db-user'")
    if db_host == None:        
        usage("Missing Parameter 'db-host'")
    if db_port == None:        
        usage("Missing Parameter 'db-port'")
    if output_file == None:
        usage("Missing Parameter 'output-file'")
    
    if target_schema == None:
        target_schema = analyze_schema
        
    # Reduce to 1 thread if we're analyzing a single table
    if analyze_table != None:
        threads = 1
        
    # get the database password
    db_pwd = getpass.getpass("Password <%s>: " % db_user)
    
    # open the output file
    output_file_handle = open(output_file, 'w')
    
    # get a connection for the controlling processes
    master_conn = get_pg_conn()
    
    if master_conn == None:
        sys.exit(NO_CONNECTION)
    
    comment("Connected to %s:%s:%s as %s" % (db_host, db_port, db, db_user))
    if analyze_table != None:
        snippet = "Table '%s'" % analyze_table        
    else:
        snippet = "Schema '%s'" % analyze_schema
        
    comment("Analyzing %s for Columnar Encoding Optimisations with %s Threads..." % (snippet, threads))
    
    if do_execute:
        if drop_old_data:
            really_go = getpass.getpass("This will make irreversible changes to your database, and cannot be undone. Type 'Yes' to continue: ")
            
            if not really_go == 'Yes':
                write("Terminating on User Request")
                sys.exit(TERMINATED_BY_USER)

        comment("Recommended encoding changes will be applied automatically...")
    else:
        pass
    
    if analyze_table != None:        
        statement = '''select trim(a.name) as table, b.mbytes, a.rows, decode(pgc.reldiststyle,0,'EVEN',1,'KEY',8,'ALL') dist_style
from (select db_id, id, name, sum(rows) as rows from stv_tbl_perm a group by db_id, id, name) as a
join pg_class as pgc on pgc.oid = a.id
join pg_namespace as pgn on pgn.oid = pgc.relnamespace
join (select tbl, count(*) as mbytes
from stv_blocklist group by tbl) b on a.id=b.tbl
and pgn.nspname = '%s' and pgc.relname = '%s'        
        ''' % (analyze_schema, analyze_table)        
    else:
        # query for all tables in the schema ordered by size descending
        comment("Extracting Candidate Table List...")
        
        statement = '''select trim(a.name) as table, b.mbytes, a.rows, decode(pgc.reldiststyle,0,'EVEN',1,'KEY',8,'ALL') dist_style
from (select db_id, id, name, sum(rows) as rows from stv_tbl_perm a group by db_id, id, name) as a
join pg_class as pgc on pgc.oid = a.id
join pg_namespace as pgn on pgn.oid = pgc.relnamespace
join (select tbl, count(*) as mbytes
from stv_blocklist group by tbl) b on a.id=b.tbl
where pgn.nspname = '%s'
  and trim(a.name) not like '%%_$old'
  and trim(a.name) not like '%%_$mig'
order by 2
        ''' % (analyze_schema,)
    
    if debug:
        comment(statement)
    
    analyze_tables = execute_query(statement)
    
    comment("Analyzing %s table(s)" % (len(analyze_tables)))

    # setup executor pool
    p = Pool(threads)
    
    if debug:
        comment(str(analyze_tables))
    
    if analyze_tables != None:
        try:
            # run all concurrent steps and block on completion
            result = p.map(analyze, analyze_tables)
        except KeyboardInterrupt:
            # To handle Ctrl-C from user
            p.close()
            p.terminate()
            cleanup()
            sys.exit(TERMINATED_BY_USER)
        except:
            write(traceback.format_exc())
            p.close()
            p.terminate()
            cleanup()
            sys.exit(ERROR)
    else:
        comment("No Tables Found to Analyze")
        
    # do a final vacuum if needed
    if drop_old_data:
        write("vacuum delete only;")

    p.terminate()
    
    # return any non-zero worker output statuses
    for ret in result:
        if isinstance(ret, (list, tuple)):
            return_code = ret[0]
            fk_commands = ret[1]
        else:
            return_code = ret
            fk_commands = None
        
        if fk_commands != None and len(fk_commands) > 0:
            print_statements(fk_commands)
            
            if do_execute:
                if not run_commands(master_conn, fk_commands):
                    if not ignore_errors:
                        write("Error running commands %s" % (fk_commands,))
                        sys.exit(ERROR)
            
        if return_code != OK:
            write("Error in worker thread: return code %d. Exiting." % (return_code,))
            sys.exit(return_code)
    
    if (do_execute):
        if not commit():
            sys.exit(ERROR)
    
    comment('Processing Complete')
    cleanup()    
    sys.exit(OK)

if __name__ == "__main__":
    main(sys.argv)
