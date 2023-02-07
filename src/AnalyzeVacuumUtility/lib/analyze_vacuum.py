from __future__ import print_function

import datetime
import os
import re
import socket
import sys
import traceback
import socket
import boto3
import datetime
import redshift_connector

try:
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
except:
    pass

import redshift_utils_helper as aws_utils
import config_constants

__version__ = ".10"

# set default values to vacuum, analyze variables
goback_no_of_days = -1
query_rank = 25

# timeout for retries - 100ms
RETRY_TIMEOUT = 100 / 1000

MAX_PERCENT = 100

OK = 0
ERROR = 1
INVALID_ARGS = 2
NO_WORK = 3
TERMINATED_BY_USER = 4
NO_CONNECTION = 5

debug = False


def execute_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    try:
        results = cursor.fetchall()

        if debug:
            comment('Query Execution returned %s Results' % (len(results)))
    except Exception as e:
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


def get_rs_conn(db_host, db, db_user, db_pwd, schema_name, db_port=5439, query_group=None, query_slot_count=1,
                ssl=True, **kwargs):
    conn = None

    if debug:
        comment('Connect %s:%s:%s:%s' % (db_host, db_port, db, db_user))

    try:
        conn = redshift_connector.connect(user=db_user, host=db_host, port=int(db_port), database=db, password=db_pwd,
                              ssl=ssl, timeout=None)
        conn._usock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        conn.autocommit = True
    except Exception as e:
        print("Exception on Connect to Cluster: %s" % e)
        print('Unable to connect to Cluster Endpoint')
        cleanup(conn)

        return None

    # set search paths
    aws_utils.set_search_paths(conn, schema_name, exclude_external_schemas=True)

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

    # set application name
    set_name = "set application_name to 'AnalyzeVacuumUtility-v%s'" % __version__

    if debug:
        comment(set_name)

    run_commands(conn, [set_name])

    comment("Connected to %s:%s:%s as %s" % (db_host, db_port, db, db_user))

    return conn


def run_commands(conn, commands, cw=None, cluster_name=None, suppress_errors=False):
    for idx, c in enumerate(commands, start=1):
        if c is not None:
            comment('[%s] Running %s out of %s commands: %s' % (str(os.getpid()), idx, len(commands), c))
            try:
                cursor = conn.cursor()
                cursor.execute(c)
                comment('Success.')
            except:
                # cowardly bail on errors
                conn.rollback()
                print(traceback.format_exc())
                if not suppress_errors:
                    raise

            # emit a cloudwatch metric for the statement
            if cw is not None and cluster_name is not None:
                dimensions = [
                    {'Name': 'ClusterIdentifier', 'Value': cluster_name}
                ]
                if c.lower().startswith('analyze'):
                    metric_name = 'AnalyzeTable'
                elif c.lower().startswith('vacuum'):
                    metric_name = 'VacuumTable'
                else:
                    # skip to the next statement - not exporting anything about these statements to cloudwatch
                    continue

                aws_utils.put_metric(cw, 'Redshift', metric_name, dimensions, None, 1, 'Count')
                if debug:
                    comment("Emitted Cloudwatch Metric for Column Encoded table")

    return True


def run_vacuum(conn,
               cluster_name,
               cw,
               schema_name='public',
               table_name=None,
               blacklisted_tables=None,
               ignore_errors=False,
               vacuum_parameter='FULL',
               min_unsorted_pct=5,
               max_unsorted_pct=50,
               stats_off_pct=10,
               max_table_size_mb=(700 * 1024),
               min_interleaved_skew=1.4,
               min_interleaved_count=0,
               **kwargs):
    statements = []

    threshold = MAX_PERCENT - int(min_unsorted_pct) if min_unsorted_pct is not None else 5
    threshold_stanza = ""
    if vacuum_parameter is not None and vacuum_parameter.upper() != 'REINDEX':
        threshold_stanza = " to %d percent" % threshold

    if table_name is not None:
        get_vacuum_statement = '''SELECT 'vacuum %s ' + "schema" + '."' + "table" + '"%s ; '
                                         + '/* Size : ' + CAST("size" AS VARCHAR(10)) + ' MB'
                                         + ', Unsorted_pct : ' + coalesce(unsorted :: varchar(10),'null') 
                                         + ', Stats Off : ' + stats_off :: varchar(10)
                                         + ' */ ;' as statement,
                                         "table" as table_name,
                                         "schema" as schema_name
                                  FROM svv_table_info
                                  WHERE 
                                    ( NVL(unsorted,0) > %s 
                                      OR stats_off > %s
                                    )
                                    AND   size < %s
                                    AND  "schema" ~ '%s'
                                    AND  "table" = '%s';
                                        ''' % (
            vacuum_parameter, threshold_stanza, min_unsorted_pct, stats_off_pct, max_table_size_mb, schema_name,
            table_name)

    elif blacklisted_tables is not None:
        comment("Extracting Candidate Tables for Vacuum...")
        blacklisted_tables_array = blacklisted_tables.split(',')
        get_vacuum_statement = '''SELECT 'vacuum %s ' + "schema" + '."' + "table" + '"%s ; '
                                         + '/* Size : ' + CAST("size" AS VARCHAR(10)) + ' MB'
                                         + ', Unsorted_pct : ' + coalesce(unsorted :: varchar(10),'null')
                                         + ', Stats Off : ' + stats_off :: varchar(10)
                                         + ' */ ;' as statement,
                                         "table" as table_name,
                                         "schema" as schema_name
                                  FROM svv_table_info
                                  WHERE 
                                    (NVL(unsorted) > %s
                                     OR stats_off > %s)
                                    AND   size < %s
                                    AND  "schema" ~ '%s'
                                    AND  "table" NOT IN (%s);
                                        ''' % (
            vacuum_parameter, threshold_stanza, min_unsorted_pct, stats_off_pct, max_table_size_mb, schema_name,
            str(blacklisted_tables_array)[1:-1])

    else:
        # query for all tables in the schema ordered by size descending
        comment("Extracting Candidate Tables for Vacuum...")

        get_vacuum_statement = '''
                SELECT 'vacuum %s ' + feedback_tbl.schema_name + '."' + feedback_tbl.table_name + '"%s; '
                       + '/* Size : ' + CAST(info_tbl."size" AS VARCHAR(10)) + ' MB' 
                       + ', Unsorted_pct : ' + coalesce(unsorted :: varchar(10),'null') 
                       + ', Stats Off : ' + stats_off :: varchar(10)
                       + ' */ ;' as statement,
                       table_name,
                       schema_name
                FROM (SELECT schema_name,
                             table_name
                      FROM (SELECT TRIM(n.nspname) schema_name,
                                   TRIM(c.relname) table_name,
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
                            AND   regexp_instr(solution,'.*VACUUM.*reclaim deleted.') > 0
                            GROUP BY TRIM(n.nspname),
                                     TRIM(c.relname)) anlyz_tbl
                      WHERE anlyz_tbl.qry_rnk <%s) feedback_tbl
                  JOIN svv_table_info info_tbl
                    ON info_tbl.schema = feedback_tbl.schema_name
                   AND info_tbl.table = feedback_tbl.table_name
                WHERE (NVL(info_tbl.unsorted) > %s OR info_tbl.stats_off > %s)
                AND   info_tbl.size < %s
                AND   TRIM(info_tbl.schema) ~ '%s'
                ORDER BY info_tbl.size,
                         info_tbl.skew_rows
                            ''' % (vacuum_parameter,
                                   threshold_stanza,
                                   goback_no_of_days,
                                   query_rank,
                                   min_unsorted_pct,
                                   stats_off_pct,
                                   max_table_size_mb,
                                   schema_name)

    if debug:
        comment(get_vacuum_statement)

    vacuum_statements = execute_query(conn, get_vacuum_statement)
    comment("Found %s Tables requiring Vacuum and flagged by alert" % len(vacuum_statements))

    for vs in vacuum_statements:
        statements.append(vs[0])
        statements.append("analyze %s.\"%s\"" % (vs[2], vs[1]))

    if not run_commands(conn, statements, cw=cw, cluster_name=cluster_name, suppress_errors=ignore_errors):
        if not ignore_errors:
            if debug:
                print("Error running statements: %s" % (str(statements),))
            return ERROR

    statements = []
    if table_name is None and blacklisted_tables is None:
        # query for all tables in the schema ordered by size descending
        comment("Extracting Candidate Tables for Vacuum ...")
        get_vacuum_statement = '''SELECT 'vacuum %s ' + "schema" + '."' + "table" + '"%s; '
                                                   + '/* Size : ' + CAST("size" AS VARCHAR(10)) + ' MB'
                                                   + ',  Unsorted_pct : ' + coalesce(info_tbl.unsorted :: varchar(10),'N/A')
                                                   + ' */ ;' as statement,
                                         info_tbl."table" as table_name,
                                         info_tbl."schema" as schema_name
                                        FROM svv_table_info info_tbl
                                        WHERE "schema" ~ '%s'
                                                AND
                                                 (
                                                --If the size of the table is less than the max_table_size_mb then , run vacuum based on condition: >min_unsorted_pct
                                                    ((size < %s) AND (NVL(unsorted,0) > %s or stats_off > %s))
                                                    OR
                                                --If the size of the table is greater than the max_table_size_mb then , run vacuum based on condition:
                                                -- >min_unsorted_pct AND < max_unsorted_pct
                                                --This is to avoid big table with large unsorted_pct
                                                     ((size > %s) AND (NVL(unsorted) > %s AND unsorted < %s ))
                                                 )
                                        ORDER BY "size" ASC ,skew_rows ASC;
                                        ''' % (vacuum_parameter,
                                               threshold_stanza,
                                               schema_name,
                                               max_table_size_mb,
                                               min_unsorted_pct,
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
            statements.append("analyze %s.\"%s\"" % (vs[2], vs[1]))

        if not run_commands(conn, statements, cw=cw, cluster_name=cluster_name, suppress_errors=ignore_errors):
            if not ignore_errors:
                if debug:
                    print("Error running statements: %s" % (str(statements),))
                return ERROR

    statements = []
    if table_name is None and blacklisted_tables is None:
        # query for all tables in the schema for vacuum reindex
        comment("Extracting Candidate Tables for Vacuum reindex of Interleaved Sort Keys...")
        get_vacuum_statement = ''' SELECT 'vacuum REINDEX ' + schema_name + '."' + table_name + '" ; ' + '/* Rows : ' + CAST("rows" AS VARCHAR(10))
                                    + ', Interleaved_skew : ' + CAST("max_skew" AS VARCHAR(10))
                                    + ', Reindex Flag : '  + CAST(reindex_flag AS VARCHAR(10)) + ' */ ;' AS statement, table_name, schema_name
                                FROM (SELECT TRIM(n.nspname) schema_name, TRIM(t.relname) table_name,
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
                                    AND schema_name ~ '%s'
                                        ''' % (min_interleaved_skew, min_interleaved_count, schema_name)

        if debug:
            comment(get_vacuum_statement)

        vacuum_statements = execute_query(conn, get_vacuum_statement)
        comment("Found %s Tables with Interleaved Sort Keys requiring Vacuum" % len(vacuum_statements))

        for vs in vacuum_statements:
            statements.append(vs[0])
            statements.append("analyze %s.\"%s\"" % (vs[2], vs[1]))

        if not run_commands(conn, statements, cw=cw, cluster_name=cluster_name, suppress_errors=ignore_errors):
            if not ignore_errors:
                if debug:
                    print("Error running statements: %s" % (str(statements),))
                return ERROR

    return True


def run_analyze(conn,
                cluster_name,
                cw,
                schema_name='public',
                table_name=None,
                blacklisted_tables=None,
                ignore_errors=False,
                predicate_cols=False,
                stats_off_pct=10,
                **kwargs):
    statements = []

    if predicate_cols:
        predicate_cols_option = ' PREDICATE COLUMNS '
    else:
        predicate_cols_option = ' ALL COLUMNS '

    if table_name is not None:
        # If it is one table , just check if this needs to be analyzed and prepare analyze statements
        get_analyze_statement_feedback = '''SELECT DISTINCT 'analyze ' + "schema" + '."' + "table" + '"' + '%s ; '
                                                   + '/* Stats_Off : ' + CAST("stats_off" AS VARCHAR(10)) + ' */ ;'
                                                FROM svv_table_info
                                                WHERE   stats_off::DECIMAL (32,4) > %s ::DECIMAL (32,4)
                                                AND  trim("schema") ~ '%s'
                                                AND  trim("table") = '%s';
                                                ''' % (predicate_cols_option, stats_off_pct, schema_name, table_name,)

    elif blacklisted_tables is not None:
        comment("Extracting Candidate Tables for analyze based on Query Optimizer Alerts...")

        blacklisted_tables_array = blacklisted_tables.split(',')
        get_analyze_statement_feedback = '''
                                    SELECT DISTINCT 'analyze ' + feedback_tbl.schema_name + '."' + feedback_tbl.table_name + '"' + '%s ; ' + '/* Stats_Off : ' + CAST(info_tbl."stats_off" AS VARCHAR(10)) + ' */ ;'
                                    FROM (/* Get top N rank tables based on the missing statistics alerts */
                                         SELECT TRIM(n.nspname)::VARCHAR schema_name,
                                                TRIM(c.relname)::VARCHAR table_name
                                         FROM (SELECT TRIM(SPLIT_PART(SPLIT_PART(a.plannode,':',2),' ',2)) AS Table_Name,
                                                      COUNT(a.query),
                                                      DENSE_RANK() OVER (ORDER BY COUNT(a.query) DESC) AS qry_rnk
                                               FROM stl_explain a,
                                                    stl_query b
                                               WHERE a.query = b.query
                                               AND   CAST(b.starttime AS DATE) >= dateadd(DAY,%s,CURRENT_DATE)
                                               AND   a.userid > 1
                                               AND   regexp_instr(a.plannode,'.*missing statistics.*') > 0
                                               AND   regexp_instr(a.plannode,'.*_bkp_.*') = 0
                                               GROUP BY Table_Name) miss_tbl
                                               LEFT JOIN pg_class c ON c.relname = TRIM(miss_tbl.table_name)
                                               LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE miss_tbl.qry_rnk <= %s
                                               /* Get the top N rank tables based on the stl_alert_event_log alerts */
                                               UNION
                                               SELECT schema_name,
                                                      table_name
                                               FROM (SELECT TRIM(n.nspname)::VARCHAR schema_name,
                                                            TRIM(c.relname)::VARCHAR table_name,
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
                                                     AND   regexp_instr(l.Solution,'.*ANALYZE command.*') > 0
                                                     GROUP BY TRIM(n.nspname),
                                                              TRIM(c.relname)) anlyz_tbl
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
                                    AND   TRIM(info_tbl.schema) ~ '%s'
                                    AND   info_tbl.table NOT IN (%s)
                                    ORDER BY info_tbl.size ASC;
                            ''' % (predicate_cols_option,
                                   goback_no_of_days,
                                   query_rank,
                                   goback_no_of_days,
                                   query_rank,
                                   stats_off_pct,
                                   schema_name,
                                   str(blacklisted_tables_array)[1:-1],)


    else:
        # query for all tables in the schema
        comment("Extracting Candidate Tables for analyze based on Query Optimizer Alerts...")

        get_analyze_statement_feedback = '''
                                    SELECT DISTINCT 'analyze ' + feedback_tbl.schema_name + '."' + feedback_tbl.table_name + '"' + '%s ; ' + '/* Stats_Off : ' + CAST(info_tbl."stats_off" AS VARCHAR(10)) + ' */ ;'
                                    FROM (/* Get top N rank tables based on the missing statistics alerts */  
                                         SELECT TRIM(n.nspname)::VARCHAR schema_name,
                                                TRIM(c.relname)::VARCHAR table_name 
                                         FROM (SELECT TRIM(SPLIT_PART(SPLIT_PART(a.plannode,':',2),' ',2)) AS Table_Name,
                                                      COUNT(a.query),
                                                      DENSE_RANK() OVER (ORDER BY COUNT(a.query) DESC) AS qry_rnk
                                               FROM stl_explain a,
                                                    stl_query b
                                               WHERE a.query = b.query
                                               AND   CAST(b.starttime AS DATE) >= dateadd(DAY,%s,CURRENT_DATE)
                                               AND   a.userid > 1
                                               AND   regexp_instr(a.plannode,'.*missing statistics.*') > 0
                                               AND   regexp_instr(a.plannode,'.*_bkp_.*') = 0
                                               GROUP BY Table_Name) miss_tbl 
                                               LEFT JOIN pg_class c ON c.relname = TRIM(miss_tbl.table_name) 
                                               LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE miss_tbl.qry_rnk <= %s 
                                               /* Get the top N rank tables based on the stl_alert_event_log alerts */
                                               UNION
                                               SELECT schema_name,
                                                      table_name
                                               FROM (SELECT TRIM(n.nspname)::VARCHAR schema_name,
                                                            TRIM(c.relname)::VARCHAR table_name,
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
                                                     AND   regexp_instr(l.Solution,'.*ANALYZE command.*') > 0
                                                     GROUP BY TRIM(n.nspname),
                                                              TRIM(c.relname)) anlyz_tbl
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
                                    AND   TRIM(info_tbl.schema) ~ '%s' 
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

    if not run_commands(conn, statements, cw=cw, cluster_name=cluster_name, suppress_errors=ignore_errors):
        if not ignore_errors:
            if debug:
                print("Error running statements: %s" % (str(statements),))
            return ERROR

    if table_name is None:
        comment("Extracting Candidate Tables for analyze based on stats off from system table info ...")

        if blacklisted_tables is not None:
            blacklisted_tables_array = blacklisted_tables.split(',')
            get_analyze_statement = '''SELECT DISTINCT 'analyze ' + "schema" + '."' + "table" + '" %s ; '
                                            + '/* Stats_Off : ' + CAST("stats_off" AS VARCHAR(10)) + ' */ ;'
                                            FROM svv_table_info
                                            WHERE   stats_off::DECIMAL (32,4) > %s::DECIMAL (32,4)
                                            AND  trim("schema") ~ '%s'
                                            AND "table" NOT IN (%s)
                                            ORDER BY "size" ASC ;
                                            ''' % (predicate_cols_option,
                                                   stats_off_pct,
                                                   schema_name,
                                                   str(blacklisted_tables_array)[1:-1],
                                                   )
        else:
            get_analyze_statement = '''SELECT DISTINCT 'analyze ' + "schema" + '."' + "table" + '" %s ; '
                                            + '/* Stats_Off : ' + CAST("stats_off" AS VARCHAR(10)) + ' */ ;'
                                            FROM svv_table_info
                                            WHERE   stats_off::DECIMAL (32,4) > %s::DECIMAL (32,4)
                                            AND  trim("schema") ~ '%s'
                                            ORDER BY "size" ASC ;
                                            ''' % (predicate_cols_option, stats_off_pct, schema_name)

        if debug:
            comment(get_analyze_statement)

        analyze_statements = execute_query(conn, get_analyze_statement)

        statements = []
        for vs in analyze_statements:
            statements.append(vs[0])

        if not run_commands(conn, statements, cw=cw, cluster_name=cluster_name, suppress_errors=ignore_errors):
            if not ignore_errors:
                if debug:
                    print("Error running statements: %s" % (str(statements),))
                    return ERROR
    return True


def run_analyze_vacuum(**kwargs):
    global debug
    if config_constants.DEBUG in os.environ:
        debug = os.environ[config_constants.DEBUG]
    if config_constants.DEBUG in kwargs and kwargs[config_constants.DEBUG]:
        debug = True

    # connect to cloudwatch
    region_key = 'AWS_REGION'
    if region_key in os.environ:
        aws_region = os.environ[region_key]
    else:
        aws_region = 'us-east-1'

    print("Connecting to AWS_REGION : %s" % aws_region)

    cw = None
    if config_constants.SUPPRESS_CLOUDWATCH not in kwargs or not kwargs[config_constants.SUPPRESS_CLOUDWATCH]:
        try:
            cw = boto3.client('cloudwatch', region_name=aws_region)
            comment("Connected to CloudWatch in %s" % aws_region)
        except Exception as e:
            if debug:
                print(traceback.format_exc())
    else:
        if debug:
            comment("Suppressing CloudWatch connection and metrics export")

    # extract the cluster name
    if config_constants.CLUSTER_NAME in kwargs:
        cluster_name = kwargs[config_constants.CLUSTER_NAME]

        # remove the cluster name argument from kwargs as it's a positional arg
        del kwargs[config_constants.CLUSTER_NAME]
    else:
        cluster_name = kwargs[config_constants.DB_HOST].split('.')[0]

    if debug:
        comment("Using Cluster Name %s" % cluster_name)
        comment("Supplied Args:")
        print(kwargs)

    db_pwd = None

    if db_pwd is None:
        db_pwd = kwargs[config_constants.DB_PASSWORD]

    if config_constants.SCHEMA_NAME not in kwargs:
        kwargs[config_constants.SCHEMA_NAME] = 'public'

    # get a connection for the controlling processes
    master_conn = get_rs_conn(kwargs[config_constants.DB_HOST],
                              kwargs[config_constants.DB_NAME],
                              kwargs[config_constants.DB_USER],
                              db_pwd,
                              kwargs[config_constants.SCHEMA_NAME],
                              kwargs[config_constants.DB_PORT],
                              None if config_constants.QUERY_GROUP not in kwargs else kwargs[
                                  config_constants.QUERY_GROUP],
                              None if config_constants.QUERY_SLOT_COUNT not in kwargs else kwargs[
                                  config_constants.QUERY_SLOT_COUNT],
                              None if config_constants.SSL not in kwargs else kwargs[config_constants.SSL])

    if master_conn is None:
        raise Exception("No Connection was established")

    # Retrieve the flags from the arguments:
    vacuum_flag = kwargs.get("vacuum_flag",'False')
    analyze_flag = kwargs.get("analyze_flag",'False')

    # Convert to Boolean
    if(vacuum_flag == 'True'):
        vacuum_flag_b = True
    else:
        vacuum_flag_b = False

    if( analyze_flag == 'True'):
        analyze_flag_b = True
    else:
        analyze_flag_b = False

    # Evaluate wether to run vacuum or analyze or both
    if vacuum_flag_b is True:
        # Run vacuum based on the Unsorted , Stats off and Size of the table
        run_vacuum(master_conn, cluster_name, cw, **kwargs)
    else:
        comment("Vacuum flag arg is set as '%s'. Vacuum not performed." % vacuum_flag )

    if analyze_flag_b is True:
        if not vacuum_flag_b:
            comment("Warning - Analyze without Vacuum may result in sub-optimal performance")

        # Run Analyze based on the  Stats off Metrics table
        run_analyze(master_conn, cluster_name, cw, **kwargs)
    else:
        comment("Analyze flag arg is set as '%s'. Analyze is not performed." % analyze_flag )

    comment('Processing Complete')

    cleanup(master_conn)

    return OK
