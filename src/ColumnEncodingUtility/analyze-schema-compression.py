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
import os
from multiprocessing import Pool
import getopt
import re
import getpass
import time
import traceback
import pg8000
import shortuuid
import datetime
from _curses import OK

__version__ = ".9.2.0"

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
output_file = None
output_file_handle = None
do_execute = False
query_slot_count = 1
ignore_errors = False
force = False
drop_old_data = False
comprows = None
query_group = None
ssl_option = False


def execute_query(str):
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute(str)
    
    try:
        results = cursor.fetchall()
    except pg8000.ProgrammingError as e:
        if "no result set" in str(e):
            return None
        else:
            raise e
        
    return results
    
    
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
            conn = pg8000.connect(user=db_user, host=db_host, port=db_port, database=db, password=db_pwd, ssl=ssl_option, timeout=None, keepalives=1, keepalives_idle=200, keepalives_interval=200, keepalives_count=5)
        except Exception as e:
            write(e)
            write('Unable to connect to Cluster Endpoint')
            cleanup()
            return ERROR      
        
        # set default search path        
        search_path = 'set search_path = \'$user\',public,%s' % (analyze_schema)
        if target_schema != None and target_schema != analyze_schema:
            search_path = search_path + ', %s' % (target_schema)
            
        if debug:
            comment(search_path)
        
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(search_path)
        except pg8000.Error as e:
            if re.match('schema "%s" does not exist' % (analyze_schema,), e.message) != None:
                write('Schema %s does not exist' % (analyze_schema,))
            else:
                write(e.message)
            return None

        if query_group is not None:
            set_query_group = 'set query_group to %s' % (query_group)

            if debug:
                comment(set_query_group)

            cursor.execute(set_query_group)
        
        if query_slot_count != None and query_slot_count != 1:
            set_slot_count = 'set wlm_query_slot_count = %s' % (query_slot_count)
            
            if debug:
                comment(set_slot_count)
                
            cursor.execute(set_slot_count)

        # set a long statement timeout
        set_timeout = "set statement_timeout = '1200000'"
        if debug:
            comment(set_timeout)
            
        cursor.execute(set_timeout)
        
        # cache the connection
        db_connections[pid] = conn
        
    return conn


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
    
    fk_statement = '''SELECT /* fetching foreign key relations */ conname,
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
    statement = '''SELECT /* fetch primary key information */   
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
    statement = '''select /* fetching column descriptions for table */ "column", type, encoding, distkey, sortkey, "notnull", ad.adsrc
 from pg_table_def de, pg_attribute at LEFT JOIN pg_attrdef ad ON (at.attrelid, at.attnum) = (ad.adrelid, ad.adnum)
 where de.schemaname = '%s'
 and de.tablename = '%s'
 and at.attrelid = '%s.%s'::regclass
 and de.column = at.attname
''' % (analyze_schema, table_name, analyze_schema, table_name)

    if debug:
        comment(statement)
        
    description = execute_query(statement)
    
    descr = {}
    for row in description:
        if debug:
            comment("Table Description: %s" % str(row))
        descr[row[0]] = row
        
    return descr


def get_count_raw_columns(table_name):
    # count the number of raw encoded columns which are not the sortkey, from the dictionary
    statement = '''select /* getting count of raw columns in table */ count(9) count_raw_columns
      from pg_table_def 
      where schemaname = '%s'
        and lower(encoding) in ('raw','none') 
        and sortkey != 1        
        and tablename = '%s'
''' % (analyze_schema, table_name)

    if debug:
        comment(statement)
        
    description = execute_query(statement)
    
    return description


def run_commands(conn, commands):
    for c in commands:
        if c != None:
            cursor = conn.cursor()
            comment('[%s] Running %s' % (str(os.getpid()), c))
            try:
                if c.count(';') > 1:
                    subcommands = c.split(';')
                    
                    for s in subcommands:
                        if s != None and s != '':
                            cursor.execute(s.replace("\n", ""))
                else:
                    cursor.execute(c)
                comment('Success.')
            except Exception as e:
                # cowardly bail on errors
                conn.rollback()
                write(traceback.format_exc())
                return False
    
    return True
        
        
def analyze(table_info):     
    table_name = table_info[0]
    dist_style = table_info[3]
    
    # get the count of columns that have raw encoding applied
    table_unoptimised = False
    count_unoptimised = 0
    encodings_modified = False    
    output = get_count_raw_columns(table_name)
    
    if output == None:
        write("Unable to determine potential RAW column encoding for %s" % table_name)
        return ERROR
    else:
        for row in output:
            if row[0] > 0:
                table_unoptimised = True
                count_unoptimised += row[0]
                
    if not table_unoptimised and not force:
        comment("Table %s does not require encoding optimisation" % table_name)
        return OK
    else:
        comment("Table %s contains %s unoptimised columns" % (table_name, count_unoptimised))
        if force:
            comment("Using Force Override Option")
    
        statement = 'analyze compression %s.%s' % (analyze_schema, table_name)
        
        if comprows != None:
            statement = statement + (" comprows %s" % int(comprows))
            
        try:
            if debug:
                comment(statement)
                
            comment("Analyzing Table '%s'" % (table_name,))
        
            # run the analyze in a loop, because it could be locked by another process modifying rows and get a timeout
            analyze_compression_result = None
            analyze_retry = 10
            attempt_count = 0
            last_exception = None
            while attempt_count < analyze_retry and analyze_compression_result == None:
                try:
                    analyze_compression_result = execute_query(statement)
                except KeyboardInterrupt:
                    # To handle Ctrl-C from user
                    cleanup()
                    return TERMINATED_BY_USER
                except Exception as e:
                    write(e)
                    attempt_count += 1
                    last_exception = e
                    
                    # Exponential Backoff
                    time.sleep(2 ** attempt_count * RETRY_TIMEOUT)
    
            if analyze_compression_result == None:
                if last_exception != None:
                    write("Unable to analyze %s due to Exception %s" % (table_name, last_exception.message))
                else:
                    write("Unknown Error")
                return ERROR
            
            if target_schema == analyze_schema:
                target_table = '%s_$mig' % table_name
            else:
                target_table = table_name
            
            create_table = 'begin;\nlock table %s.%s;\ncreate table %s.%s(' % (analyze_schema, table_name, target_schema, target_table,)
            
            # query the table column definition
            descr = get_table_desc(table_name)
                
            encode_columns = []
            statements = []
            sortkeys = {}
            has_zindex_sortkeys = False
            has_identity = False
            non_identity_columns = []
            fks = []
            
            # process each item given back by the analyze request
            for row in analyze_compression_result:
                if debug:
                    comment("Analyzed Compression Row State: %s" % str(row))
                col = row[1]
                
                # compare the previous encoding to the new encoding
                new_encoding = row[2]
                old_encoding = descr[col][2]
                old_encoding = 'raw' if old_encoding == 'none' else old_encoding
                if new_encoding != old_encoding:
                    encodings_modified = True
                    if debug:
                        comment("Column %s will be modified from %s encoding to %s encoding" % (col, old_encoding, new_encoding))
                
                # fix datatypesj from the description type to the create type
                col_type = descr[col][1]                
                col_type = col_type.replace('character varying', 'varchar').replace('without time zone', '')
                
                # is this the dist key?
                distkey = descr[col][3]
                if str(distkey).upper() == 'T':
                    distkey = 'DISTKEY'
                else:
                    distkey = ''
                    
                # is this the sort key?
                sortkey = descr[col][4]
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
                col_null = descr[col][5]
                
                if str(col_null).upper() == 'TRUE':
                    col_null = 'NOT NULL'
                else:
                    col_null = ''
    
                # get default or identity syntax for this column
                default_or_identity = descr[col][6]
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
                    non_identity_columns.append(col)
    
                # add the formatted column specification
                encode_columns.extend(['"%s" %s %s %s encode %s %s'
                                       % (col, col_type, default_value, col_null, compression, distkey)])
            
            # if this table's encodings have not changed, then dont do a modification, including if the force options is set
            if not encodings_modified:
                comment("Column Encoding resulted in an identical table - no changes will be made")
            else:
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
    
                insert = 'insert into %s.%s %s select %s from %s.%s;' % (target_schema,
                                                                         target_table,
                                                                         mig_columns,
                                                                         source_columns,
                                                                         analyze_schema,
                                                                         table_name)
                statements.extend([insert])
                        
                # analyze the new table
                analyze = 'analyze %s.%s;' % (target_schema, target_table)
                statements.extend([analyze])
                        
                if (target_schema == analyze_schema):
                    # rename the old table to _$old or drop
                    if drop_old_data:
                        drop = 'drop table %s.%s cascade;' % (target_schema, table_name)
                    else:
                        # the alter table statement for the current data will use the first 104 characters of the original table name, the current datetime as YYYYMMDD and a 10 digit random string
                        drop = 'alter table %s.%s rename to %s_%s_%s_$old;' % (target_schema, table_name, table_name[0:104] , datetime.date.today().strftime("%Y%m%d") , shortuuid.ShortUUID().random(length=10))
                    
                    statements.extend([drop])
                            
                    # rename the migrate table to the old table name
                    rename = 'alter table %s.%s rename to %s;' % (target_schema, target_table, table_name)
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
        
        return (OK, fks, encodings_modified)


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
    write('           --ssl-option     - Set SSL to True or False (default False)')
    sys.exit(INVALID_ARGS)


# method used to configure global variables, so that we can call the run method
def configure(_output_file, _db, _db_user, _db_pwd, _db_host, _db_port, _analyze_schema, _target_schema, _analyze_table, _threads, _do_execute, _query_slot_count, _ignore_errors, _force, _drop_old_data, _comprows, _query_group, _debug, _ssl_option):
    # setup globals
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
    global do_execute
    global query_slot_count
    global ignore_errors
    global force
    global drop_old_data
    global comprows
    global query_group
    global output_file
    global ssl_option

    # set global variable values
    output_file = _output_file    
    db = None if _db == "" else _db
    db_user = _db_user
    db_pwd = _db_pwd
    db_host = _db_host
    db_port = _db_port
    analyze_schema = None if _analyze_schema == "" else _analyze_schema
    analyze_table = None if _analyze_table == "" else _analyze_table
    target_schema = _analyze_schema if _target_schema == "" or _target_schema == None else _target_schema
    debug = False if _debug == None else _debug    
    do_execute = False if _do_execute == None else _do_execute
    ignore_errors = False if _ignore_errors == None else _ignore_errors
    force = False if _force == None else _force
    drop_old_data = False if _drop_old_data == None else _drop_old_data
    query_group = None if _query_group == "" else _query_group
    threads = 1 if _threads == None else int(_threads)
    comprows = None if _comprows == -1 else int(_comprows)
    query_slot_count = int(_query_slot_count)
    ssl_option = False if _ssl_option == None else _ssl_option
    
    if (debug == True):
        comment("Redshift Column Encoding Utility Configuration")
        comment("output_file: %s " % (output_file))
        comment("db: %s " % (db))
        comment("db_user: %s " % (db_user))
        comment("db_host: %s " % (db_host))
        comment("db_port: %s " % (db_port))
        comment("threads: %s " % (threads))
        comment("analyze_schema: %s " % (analyze_schema))
        comment("analyze_table: %s " % (analyze_table))
        comment("target_schema: %s " % (target_schema))
        comment("debug: %s " % (debug))
        comment("do_execute: %s " % (do_execute))
        comment("query_slot_count: %s " % (query_slot_count))
        comment("ignore_errors: %s " % (ignore_errors))
        comment("force: %s " % (force))
        comment("drop_old_data: %s " % (drop_old_data))
        comment("comprows: %s " % (comprows))
        comment("query_group: %s " % (query_group))
        comment("ssl_option: %s " % (ssl_option))
    
    
def run():
    global master_conn
    global output_file_handle
    
    # open the output file
    output_file_handle = open(output_file, 'w')
    
    # get a connection for the controlling processes
    master_conn = get_pg_conn()
    
    if master_conn == None or master_conn == ERROR:
        return NO_CONNECTION
    
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
                return TERMINATED_BY_USER

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
  and substring(a.name,length(a.name)-3,length(a.name)) != '$old'
  and substring(a.name,length(a.name)-3,length(a.name)) != '$mig'
order by 2;
        ''' % (analyze_schema,)
    
    if debug:
        comment(statement)
    
    query_result = execute_query(statement)
    
    if query_result == None:
        comment("Unable to issue table query - aborting")
        return ERROR
    
    analyze_tables = []
    for row in query_result:
        analyze_tables.append(row)
    
    comment("Analyzing %s table(s) which contain allocated data blocks" % (len(analyze_tables)))

    if debug:
        [comment(str(x)) for x in analyze_tables]

    result = []
    
    if analyze_tables != None:   
        # we'll use a Pool to process all the tables with multiple threads, or just sequentially if 1 thread is requested         
        if threads > 1:
            # setup executor pool
            p = Pool(threads)
        
            try:
                # run all concurrent steps and block on completion
                result = p.map(analyze, analyze_tables)
            except KeyboardInterrupt:
                # To handle Ctrl-C from user
                p.close()
                p.terminate()
                cleanup()
                return TERMINATED_BY_USER
            except:
                write(traceback.format_exc())
                p.close()
                p.terminate()
                cleanup()
                return ERROR
                
            p.terminate()
        else:
            for t in analyze_tables:
                result.append(analyze(t))
    else:
        comment("No Tables Found to Analyze")
        
    # do a final vacuum if needed
    if drop_old_data:
        write("vacuum delete only;")
    
    # return any non-zero worker output statuses
    modified_tables = 0
    for ret in result:
        if isinstance(ret, (list, tuple)):
            return_code = ret[0]
            fk_commands = ret[1]
            modified_tables = modified_tables + 1 if ret[2] else modified_tables
        else:
            return_code = ret
            fk_commands = None
        
        if fk_commands != None and len(fk_commands) > 0:
            print_statements(fk_commands)
            
            if do_execute:
                if not run_commands(master_conn, fk_commands):
                    if not ignore_errors:
                        write("Error running commands %s" % (fk_commands,))
                        return ERROR
            
        if return_code != OK:
            write("Error in worker thread: return code %d. Exiting." % (return_code,))
            return return_code
    
    comment("Performed modification of %s tables" % modified_tables)
    
    if (do_execute):
        if not master_conn.commit():
            return ERROR
    
    comment('Processing Complete')
    cleanup()    
    
    return OK


def main(argv):
    output_file = None
    db = None
    db_user = None
    db_pwd = None
    db_host = None
    db_port = None
    threads = None
    analyze_schema = None
    analyze_table = None
    target_schema = None
    debug = None
    do_execute = None
    query_slot_count = None
    ignore_errors = None
    force = None
    drop_old_data = None
    comprows = None
    query_group = None
    ssl_option = None
    
    supported_args = """db= db-user= db-host= db-port= target-schema= analyze-schema= analyze-table= threads= debug= output-file= do-execute= slot-count= ignore-errors= force= drop-old-data= comprows= query_group= ssl-option="""
    
    # extract the command line arguments
    try:
        optlist, remaining = getopt.getopt(sys.argv[1:], "", supported_args.split())
    except getopt.GetoptError as err:
        print str(err)
        usage(None)

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
                db_port = int(value)
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
        elif arg == "--ssl-option":
            if value == 'true' or value == 'True':
                ssl_option = True
            else:
                ssl_option = False
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
    if analyze_schema == None and analyze_table == None:
        usage("You must supply analyze-schema or analyze-table")
    if target_schema == None:
        target_schema = analyze_schema
        
    # Reduce to 1 thread if we're analyzing a single table
    if analyze_table != None:
        threads = 1
        
    # get the database password
    db_pwd = getpass.getpass("Password <%s>: " % db_user)
    
    # setup the configuration
    configure(output_file, db, db_user, db_pwd, db_host, db_port, analyze_schema, target_schema, analyze_table, threads, do_execute, query_slot_count, ignore_errors, force, drop_old_data, comprows, query_group, debug, ssl_option)
    
    # run the analyser
    result_code = run()
    
    # exit based on the provided return code
    sys.exit(result_code)

if __name__ == "__main__":
    main(sys.argv)
