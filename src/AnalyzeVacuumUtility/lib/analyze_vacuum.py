from __future__ import print_function

import os
import sys
import re
import datetime
import pg8000
import traceback

# set default values to vacuum, analyze variables
goback_no_of_days = 1
query_rank = 25


# timeout for retries - 100ms
RETRY_TIMEOUT = 100/1000

OK = 0
ERROR = 1
INVALID_ARGS = 2
NO_WORK = 3
TERMINATED_BY_USER = 4
NO_CONNECTION = 5


def execute_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    try:
        results = cursor.fetchall()
        
        if debug:
            comment('Query Execution returned %s Results' % (len(results)))
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


def cleanup(conn):
    # close all connections and close the output file
    if conn is not None:
        close_conn(conn)


def comment(string):
    datetime_str = str(datetime.datetime.now())
    if string is not None:
        if re.match('.*\\n.*', string) is not None:
            print('/* [%s]\n%s\n*/\n' % (str(os.getpid()), string))
        else:
            print('-- %s [%s] %s' % (datetime_str, str(os.getpid()), string))


def print_statements(statements):
    if statements is not None:
        for s in statements:
            if s is not None:
                print(s)


def get_pg_conn(db_host, db_port, db, db_user, db_pwd, schema_name, query_group, query_slot_count, ssl_option):
    conn = None

    if debug:
        comment('Connect %s:%s:%s:%s' % (db_host, db_port, db, db_user))

    try:
        conn = pg8000.connect(user=db_user, 
                              host=db_host, 
                              port=int(db_port), 
                              database=db, 
                              password=db_pwd, 
                              ssl=ssl_option)
        conn.autocommit = True
    except Exception as e:
        print("Exception on Connect to Cluster: %s" % e)
        print('Unable to connect to Cluster Endpoint')
        cleanup(conn)
        
        return None

    # set default search path
    search_path = 'set search_path = \'$user\',public,%s' % schema_name

    if debug:
        comment(search_path)

    try:
        run_commands(conn, [search_path])
    except pg8000.ProgrammingError as e:
        if re.match('schema "%s" does not exist' % (schema_name,), e.message) is not None:
            print('Schema %s does not exist' % (schema_name,))
        else:
            print(e.message)
        return None

    if query_group is not None and query_group != '':
        set_query_group = 'set query_group to %s' % query_group

        if debug:
            comment(set_query_group)

        run_commands(conn, [set_query_group])

    set_slot_count = None
    if query_slot_count is not None and query_slot_count > 1:
        set_slot_count = 'set wlm_query_slot_count = %s' % query_slot_count

    if set_slot_count is not None:
        if debug:
            comment(set_slot_count)
        run_commands(conn, [set_slot_count])

    # set a long statement timeout
    set_timeout = "set statement_timeout = '36000000'"

    if debug:
        comment(set_timeout)

    run_commands(conn, [set_timeout])

    return conn


def run_commands(conn, commands):
    for idx, c in enumerate(commands, start=1):
        if c is not None:
            comment('[%s] Running %s out of %s commands: %s' % (str(os.getpid()), idx, len(commands), c))
            try:
                cursor = conn.cursor()
                cursor.execute(c)
                comment('Success.')
            except Exception:
                # cowardly bail on errors
                conn.rollback()
                print(traceback.format_exc())
                return False

    return True


def run_vacuum(conn,
               schema_name,
               table_name,
               blacklisted_tables,
               ignore_errors,
               vacuum_parameter,
               min_unsorted_pct,
               max_unsorted_pct,
               deleted_pct,
               stats_off_pct,
               max_table_size_mb,
               min_interleaved_skew,
               min_interleaved_cnt):
    statements = []

    if table_name is not None:
        get_vacuum_statement = '''SELECT 'vacuum %s ' + "schema" + '."' + "table" + '" ; '
                                                   + '/* '+ ' Table Name : ' + "schema" + '."' + "table"
                                                   + '",  Size : ' + CAST("size" AS VARCHAR(10)) + ' MB,  Unsorted_pct : ' + decode("unsorted", null,'null') + ', Stats Off : ' + stats_off :: varchar(10)
                                                   + ',  Deleted_pct : ' + CAST("empty" AS VARCHAR(10)) +' */ ;' as statement,
                                         "table" as table_name
                                        FROM svv_table_info
                                        WHERE (unsorted > %s OR empty > %s or stats_off > %s)
                                            AND   size < %s
                                            AND  "schema" = '%s'
                                            AND  "table" = '%s';
                                        ''' % (vacuum_parameter,min_unsorted_pct,deleted_pct,stats_off_pct,max_table_size_mb,schema_name,table_name)

    elif blacklisted_tables is not None:
        blacklisted_tables_array = blacklisted_tables.split(',')
        get_vacuum_statement = '''SELECT 'vacuum %s ' + "schema" + '."' + "table" + '" ; '
                                                   + '/* '+ ' Table Name : ' + "schema" + '."' + "table"
                                                   + '",  Size : ' + CAST("size" AS VARCHAR(10)) + ' MB,  Unsorted_pct : ' + decode("unsorted", null,'null')
                                                   + ',  Deleted_pct : ' + CAST("empty" AS VARCHAR(10)) +' */ ;' as statement,
                                         "table" as table_name
                                        FROM svv_table_info
                                        WHERE (unsorted > %s OR empty > %s or stats_off > %s)
                                            AND   size < %s
                                            AND  "schema" = '%s'
                                            AND  "table" NOT IN (%s);
                                        ''' % (vacuum_parameter,min_unsorted_pct,deleted_pct,stats_off_pct,max_table_size_mb,schema_name,str(blacklisted_tables_array)[1:-1])

    else:
        # query for all tables in the schema ordered by size descending
        comment("Extracting Candidate Tables for vacuum based on stl_alert_event_log...")

        get_vacuum_statement = '''
                SELECT 'vacuum %s ' + feedback_tbl.schema_name + '."' + feedback_tbl.table_name + '" ; ' + '/* ' + ' Table Name : ' + info_tbl."schema" + '."' + info_tbl."table" + '",  Size : ' + CAST(info_tbl."size" AS VARCHAR(10)) + ' MB' + ',  Unsorted_pct : ' + decode(info_tbl."unsorted", null,'null') + ',  Deleted_pct : ' + CAST(info_tbl."empty" AS VARCHAR(10)) + ' */ ;' as statement,
                       table_name
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
                            AND   l.event_time >= dateadd(DAY,-%s,CURRENT_DATE)
                            AND   l.Solution LIKE '%%VACUUM command%%'
                            GROUP BY TRIM(n.nspname),
                                     c.relname) anlyz_tbl
                      WHERE anlyz_tbl.qry_rnk <%s) feedback_tbl
                  JOIN svv_table_info info_tbl
                    ON info_tbl.schema = feedback_tbl.schema_name
                   AND info_tbl.table = feedback_tbl.table_name
                WHERE (info_tbl.unsorted > %s OR info_tbl.empty > %s OR info_tbl.stats_off > %s)
                AND   info_tbl.size < %s
                AND   TRIM(info_tbl.schema) = '%s'
                ORDER BY info_tbl.size,
                         info_tbl.skew_rows
                            ''' %(vacuum_parameter,goback_no_of_days,query_rank,min_unsorted_pct,deleted_pct,stats_off_pct,max_table_size_mb,schema_name,)

    if debug:
        comment(get_vacuum_statement)

    vacuum_statements = execute_query(conn, get_vacuum_statement)
    comment("Found %s Tables requiring Vacuum and flagged by alert" % len(vacuum_statements))

    for vs in vacuum_statements:
        statements.append(vs[0])
        statements.append("analyze %s.\"%s\"" % (schema_name, vs[1]))

    if not run_commands(conn, statements):
        if not ignore_errors:
            if debug:
                print("Error running statements: %s" % (str(statements),))
            return ERROR

    statements = []
    if table_name is None and blacklisted_tables is None:
        # query for all tables in the schema ordered by size descending
        comment("Extracting Candidate Tables for vacuum ...")
        get_vacuum_statement = '''SELECT 'vacuum %s ' + "schema" + '."' + "table" + '" ; '
                                                   + '/* '+ ' Table Name : ' + "schema" + '."' + "table"
                                                   + '",  Size : ' + CAST("size" AS VARCHAR(10)) + ' MB'
                                                   + ',  Unsorted_pct : ' + COALESCE(CAST(info_tbl."unsorted" AS VARCHAR(10)), 'N/A')
                                                   + ',  Deleted_pct : ' + CAST("empty" AS VARCHAR(10)) +' */ ;' as statement,
                                         info_tbl."table" as table_name
                                        FROM svv_table_info info_tbl
                                        WHERE "schema" = '%s'
                                                AND
                                                 (
                                                --If the size of the table is less than the max_table_size_mb then , run vacuum based on condition: >min_unsorted_pct AND >deleted_pct
                                                    ((size < %s) AND (unsorted > %s OR empty > %s or stats_off > %s))
                                                    OR
                                                --If the size of the table is greater than the max_table_size_mb then , run vacuum based on condition:
                                                -- >min_unsorted_pct AND < max_unsorted_pct AND >deleted_pct
                                                --This is to avoid big table with large unsorted_pct
                                                     ((size > %s) AND (unsorted > %s AND unsorted < %s ))
                                                 )
                                        ORDER BY "size" ASC ,skew_rows ASC;
                                        ''' % (vacuum_parameter,
                                               schema_name,
                                               max_table_size_mb,
                                               min_unsorted_pct,
                                               deleted_pct,
                                               stats_off_pct,
                                               max_table_size_mb,
                                               min_unsorted_pct,
                                               max_unsorted_pct)

        if debug:
            comment(get_vacuum_statement)

        vacuum_statements = execute_query(conn, get_vacuum_statement)
        comment("Found %s Tables requiring Vacuum due to stale statistics" % len(vacuum_statements))

        for vs in vacuum_statements:
            statements.append(vs[0])
            statements.append("analyze %s.\"%s\"" % (schema_name, vs[1]))

        if not run_commands(conn, statements):
            if not ignore_errors:
                if debug:
                    print("Error running statements: %s" % (str(statements),))
                return ERROR

    statements = []
    if table_name is None and blacklisted_tables is None:
        # query for all tables in the schema for vacuum reindex
        comment("Extracting Candidate Tables for vacuum reindex ...")
        get_vacuum_statement = ''' SELECT 'vacuum REINDEX ' + schema_name + '."' + table_name + '" ; ' + '/* ' + ' Table Name : '
                                    + schema_name + '."' + table_name + '",  Rows : ' + CAST("rows" AS VARCHAR(10))
                                    + ',  Interleaved_skew : ' + CAST("max_skew" AS VARCHAR(10))
                                    + ' ,  Reindex Flag : '  + CAST(reindex_flag AS VARCHAR(10)) + ' */ ;' AS statement, table_name
                                FROM (SELECT TRIM(n.nspname) schema_name, t.relname table_name,
                                                 MAX(v.interleaved_skew) max_skew, MAX(c.count) AS rows,
                                                 CASE
                                                   -- v.interleaved_skew can be null if the table has never been vacuumed so account for that
                                                   WHEN (max(c.max_bucket) = 0) OR (MAX(NVL(v.interleaved_skew,10)) > %s AND MAX(c.count) > %s) THEN 'Yes'
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
                                        ''' % (min_interleaved_skew, min_interleaved_cnt, schema_name)

        if debug:
            comment(get_vacuum_statement)

        vacuum_statements = execute_query(conn, get_vacuum_statement)
        comment("Found %s Tables with Interleaved Sort Keys requiring Vacuum" % len(vacuum_statements))

        for vs in vacuum_statements:
            statements.append(vs[0])
            statements.append("analyze %s.\"%s\"" % (schema_name, vs[1]))

        if not run_commands(conn, statements):
            if not ignore_errors:
                if debug:
                    print("Error running statements: %s" % (str(statements),))
                return ERROR

    return True


def run_analyze(conn, schema_name, table_name, ignore_errors, predicate_cols, stats_off_pct):
    statements = []

    if predicate_cols:
        predicate_cols_option = ' PREDICATE COLUMNS '
    else:
        predicate_cols_option = ' ALL COLUMNS '

    if table_name is not None:
        # If it is one table , just check if this needs to be analyzed and prepare analyze statements
        get_analyze_statement_feedback = '''SELECT DISTINCT 'analyze ' + "schema" + '."' + "table" + '"' + '%s ; '
                                                   + '/* '+ ' Table Name : ' + "schema" + '."' + "table"
                                                   + '",  stats_off : ' + CAST("stats_off" AS VARCHAR(10)) + ' */ ;'
                                                FROM svv_table_info
                                                WHERE   stats_off::DECIMAL (32,4) > %s ::DECIMAL (32,4)
                                                AND  trim("schema") = '%s'
                                                AND  trim("table") = '%s';
                                                ''' % (predicate_cols_option,stats_off_pct,schema_name,table_name,)
    else:
        # query for all tables in the schema
        comment("Extracting Candidate Tables for analyze based on Query Optimizer Alerts(Feedbacks) ...")

        get_analyze_statement_feedback = '''
                                    SELECT DISTINCT 'analyze ' + feedback_tbl.schema_name + '."' + feedback_tbl.table_name + '"' + '%s ; ' + '/* ' + ' Table Name : ' + info_tbl."schema" + '."' + info_tbl."table" + '", Stats_Off : ' + CAST(info_tbl."stats_off" AS VARCHAR(10)) + ' */ ;'
                                    FROM (/* Get top N rank tables based on the missing statistics alerts */  
                                         SELECT TRIM(n.nspname)::VARCHAR schema_name,
                                                c.relname::VARCHAR table_name 
                                         FROM (SELECT TRIM(SPLIT_PART(SPLIT_PART(a.plannode,':',2),' ',2)) AS Table_Name,
                                                      COUNT(a.query),
                                                      DENSE_RANK() OVER (ORDER BY COUNT(a.query) DESC) AS qry_rnk
                                               FROM stl_explain a,
                                                    stl_query b
                                               WHERE a.query = b.query
                                               AND   CAST(b.starttime AS DATE) >= dateadd(DAY,%s,CURRENT_DATE)
                                               AND   a.userid > 1
                                               AND   a.plannode LIKE '%%missing statistics%%'
                                               AND   a.plannode NOT LIKE '%%_bkp_%%'
                                               GROUP BY Table_Name) miss_tbl 
                                               LEFT JOIN pg_class c ON c.relname = TRIM(miss_tbl.table_name) 
                                               LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE miss_tbl.qry_rnk <= %s 
                                               /* Get the top N rank tables based on the stl_alert_event_log alerts */
                                               UNION
                                               SELECT schema_name,
                                                      table_name
                                               FROM (SELECT TRIM(n.nspname)::VARCHAR schema_name,
                                                            c.relname::VARCHAR table_name,
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
                                                     AND   l.event_time >= dateadd(DAY,%s,CURRENT_DATE)
                                                     AND   l.Solution LIKE '%%ANALYZE command%%'
                                                     GROUP BY TRIM(n.nspname),
                                                              c.relname) anlyz_tbl
                                               WHERE anlyz_tbl.qry_rnk < %s 
                                               UNION
                                               /* just a base dump of svv_table_info to check the stats_off metric */ 
                                               SELECT "schema"::VARCHAR schema_name,
                                                      "table"::VARCHAR table_name
                                               FROM svv_table_info) feedback_tbl
                                      JOIN svv_table_info info_tbl
                                        ON info_tbl.schema = feedback_tbl.schema_name
                                       AND info_tbl.table = feedback_tbl.table_name
                                    WHERE info_tbl.stats_off::DECIMAL(32,4) > %s::DECIMAL(32,4)
                                    AND   TRIM(info_tbl.schema) = '%s' 
                                    ORDER BY info_tbl.size ASC
                            ''' % (predicate_cols_option,
                                   goback_no_of_days,
                                   query_rank,
                                   goback_no_of_days,
                                   query_rank,
                                   stats_off_pct,
                                   schema_name)

    if debug:
        comment(get_analyze_statement_feedback)

    analyze_statements = execute_query(conn, get_analyze_statement_feedback)

    for vs in analyze_statements:
        statements.append(vs[0])

    comment("Found %s Tables requiring Analysis" % len(statements))

    if not run_commands(conn, statements):
        if not ignore_errors:
            if debug:
                print("Error running statements: %s" % (str(statements),))
            return ERROR

    if table_name is None:
        comment("Extracting Candidate Tables for analyze based on stats off from system table info ...")

        get_analyze_statement = '''SELECT DISTINCT 'analyze ' + "schema" + '."' + "table" + '" %s ; '
                                        + '/* '+ ' Table Name : ' + "schema" + '."' + "table"
                                        + '", Stats_Off : ' + CAST("stats_off" AS VARCHAR(10)) + ' */ ;'
                                        FROM svv_table_info
                                        WHERE   stats_off::DECIMAL (32,4) > %s::DECIMAL (32,4)
                                        AND  trim("schema") = '%s'
                                        ORDER BY "size" ASC ;
                                        ''' % (predicate_cols_option, stats_off_pct, schema_name)

        if debug:
            comment(get_analyze_statement)

        analyze_statements = execute_query(conn, get_analyze_statement)

        statements = []
        for vs in analyze_statements:
            statements.append(vs[0])

        if not run_commands(conn, statements):
                if not ignore_errors:
                    if debug:
                        print("Error running statements: %s" % (str(statements),))
                        return ERROR
    return True


def run_analyze_vacuum(db_host,
                       db_port,
                       db_user,
                       db_pwd,
                       db,
                       query_group,
                       query_slot_count,
                       vacuum_flag,
                       analyze_flag,
                       schema_name,
                       table_name,
                       blacklisted_tables,
                       ignore_errors,
                       ssl_option,
                       set_debug,
                       vacuum_parameter,
                       min_unsorted_pct,
                       max_unsorted_pct,
                       deleted_pct,
                       stats_off_pct,
                       predicate_cols,
                       max_table_size_mb,
                       min_interleaved_skew,
                       min_interleaved_cnt):
    global debug
    debug = set_debug

    if 'DEBUG' in os.environ:
        debug = os.environ['DEBUG']

    # get a connection for the controlling processes
    master_conn = get_pg_conn(db_host,
                              db_port,
                              db,
                              db_user,
                              db_pwd,
                              schema_name,
                              query_group,
                              query_slot_count,
                              ssl_option)

    if master_conn is None:
        sys.exit(NO_CONNECTION)

    comment("Connected to %s:%s:%s as %s" % (db_host, db_port, db, db_user))

    if blacklisted_tables is not None and len(blacklisted_tables) > 0:
        comment("The blacklisted tables are: %s" % (str(blacklisted_tables)))

    if vacuum_flag is True:
        # Run vacuum based on the Unsorted , Stats off and Size of the table
        run_vacuum(master_conn,
                   schema_name,
                   table_name,
                   blacklisted_tables,
                   ignore_errors,
                   vacuum_parameter,
                   min_unsorted_pct,
                   max_unsorted_pct,
                   deleted_pct,
                   stats_off_pct,
                   max_table_size_mb,
                   min_interleaved_skew,
                   min_interleaved_cnt)
    else:
        comment("Vacuum flag arg is set as %s. Vacuum is not performed." % vacuum_flag)

    if analyze_flag is True:
        if vacuum_flag is False:
            comment("Warning - Analyze without Vacuum may result in sub-optimal performance")

        # Run Analyze based on the  Stats off Metrics table
        run_analyze(master_conn, schema_name, table_name, ignore_errors, predicate_cols, stats_off_pct)
    else:
        comment("Analyze flag arg is set as %s. Analyze is not performed." % analyze_flag)

    comment('Processing Complete')
    
    cleanup(master_conn)
    
    return OK
