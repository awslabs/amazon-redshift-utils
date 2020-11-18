/**********************************************************************************************
Purpose: Sync all changed rows from a source table and merge them into a target table

Notes: * Useful for keeping a up-to-date Redshift copy of a remote Federated table
       * Changed rows are found by referring to a `last_modified` timestamp column
         * Rows in source that do not exist in target are inserted in target 
         * Rows in source that match a row in target are updated in target
         * Be aware that deletions from the source will not sync to target
       * Source and target tables must have same table structure (column order and data types)

Parameters:
        batch_time  : Timestamp for this batch. Used to timestamp the log
        source_tbl  : Schema qualified name of external source table
        target_tbl  : Schema qualified name of local target table
        last_mod_col: Name of "last modified" column used to find changed rows
        log_table   : Schema qualified name of table where actions are logged
                       CREATE TABLE $(log_table) (
                           batch_time   TIMESTAMP
                         , source_table VARCHAR
                         , target_table VARCHAR
                         , sync_column  VARCHAR
                         , sync_status  VARCHAR
                         , sync_queries VARCHAR --Query ID of the INSERT
                         , row_count    INT);
        max_rows    : Maximum number of rows allowed to sync automatically

History:
2020-04-01 - joeharris76 - Created
**********************************************************************************************/

-- DROP PROCEDURE sp_sync_merge_changes(TIMESTAMP,VARCHAR,VARCHAR,VARCHAR,VARCHAR,BIGINT);
CREATE OR REPLACE PROCEDURE sp_sync_merge_changes (
      batch_time   IN TIMESTAMP
    , source_tbl   IN VARCHAR(256) --Schema qualified with external schema
    , target_tbl   IN VARCHAR(256) --Schema qualified or `public` is assumed
    , last_mod_col IN VARCHAR(128)
    , log_table    IN VARCHAR(256)
    , max_rows     IN BIGINT)
AS $$
DECLARE
    sql            VARCHAR(MAX) := '';
    rs_tbl         VARCHAR := '';
    rs_schm        VARCHAR := '';
    is_timestamp   BOOLEAN := FALSE;
    max_mod_ts     TIMESTAMP := '1970-01-01';
    pk_columns     VARCHAR(512) := '';
    insert_query   VARCHAR := '';
    delete_query   VARCHAR := '';
    sync_count     INTEGER := 0;
    delete_count   INTEGER := 0;
    insert_count   INTEGER := 0;
    change_count   INTEGER := 0;
BEGIN
    -- Check that required parameters are set
    IF source_tbl='' OR target_tbl='' OR last_mod_col='' OR log_table='' OR max_rows IS NULL THEN
        EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||last_mod_col||
                       ''',''ABORT - Empty parameters'','''||insert_query||''','||sync_count||';';
        COMMIT; --Persist the log entry
        RAISE EXCEPTION 'Aborted - required parameters are missing.';
    END IF;

    -- Notify user when max_rows is specified
    IF max_rows <> 0 THEN
         RAISE INFO 'A maximum of % rows will be inserted.', max_rows;
    END IF;

    -- Split `target_tbl` into `rs_schm` and `rs_tbl` as needed
    IF CHARINDEX('.',target_tbl) > 0 THEN
        SELECT INTO rs_schm SPLIT_PART(target_tbl,'.',1);
        SELECT INTO rs_tbl  SPLIT_PART(target_tbl,'.',2);
    ELSE
        rs_schm := 'public';
        rs_tbl := target_tbl;
    END IF;

    --Check that `last_mod_col` is a usable Date/Time value
    -- Note: types 1083, 1266, and 1186 are unsupported in Redshift
    sql := 'SELECT CASE WHEN a.atttypid IN (1114,1184,1082) THEN TRUE ELSE FALSE END '||
           '  FROM pg_attribute a '||
           '  JOIN pg_class     b     ON a.attrelid = b.relfilenode '||
           '  JOIN pg_namespace c     ON b.relnamespace = c.oid '||
           ' WHERE TRIM(a.attname) = '''||last_mod_col||''''||
           '   AND TRIM(c.nspname) = '''||rs_schm||''''||
           '   AND TRIM(b.relname) = '''||rs_tbl||''''||
           'LIMIT 1;';
    EXECUTE sql INTO is_timestamp;
    IF is_timestamp IS FALSE THEN
        EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||last_mod_col||
                       ''',''ABORT - `last_mod_col` is not a date/time'','''||insert_query||''','||sync_count||';';
        COMMIT; --Persist the log entry
        RAISE EXCEPTION 'Aborted - `%` is not a date/time column.', last_mod_col;
    END IF;

    --Get the max value of `last_mod_col` in `target_tbl`
    sql := 'SELECT MAX('||last_mod_col||') FROM '||target_tbl||' ;';
    EXECUTE sql INTO max_mod_ts;
    IF max_mod_ts = '1970-01-01' OR max_mod_ts IS NULL THEN
        EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||last_mod_col||
                       ''',''ABORT - Invalid `max_mod_ts`'','''||insert_query||''','||sync_count||';';
        COMMIT; --Persist the log entry
        RAISE EXCEPTION 'Aborted - `max_mod_ts` was not retrieved correctly.';
    END IF;

    --Retrieve the primary key column(s) for the table
    sql := 'SELECT REPLACE(REPLACE(pg_get_constraintdef(con.oid),''PRIMARY KEY ('',''''),'')'','''')'||
           ' FROM pg_constraint AS con '||
           ' JOIN pg_class      AS tbl ON tbl.relnamespace = con.connamespace AND tbl.oid = con.conrelid'||
           ' JOIN pg_namespace  AS sch ON sch.oid =tbl.relnamespace'||
           ' WHERE tbl.relkind = ''r'' AND con.contype = ''p''';
    EXECUTE sql||' ;' INTO pk_columns;
    IF pk_columns = '' THEN
        EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||last_mod_col||
                       ''',''ABORT - No PRIMARY KEY found'','''||insert_query||''','||sync_count||';';
        COMMIT; --Persist the log entry
        RAISE EXCEPTION 'Aborted - No PRIMARY KEY found for target.';
    END IF;
    
    -- Create a local temp table with the structure of the target table
        -- We can check `max_rows` against the temp table row count and roll back if needed
        -- This means we SELECT from the source table **just once**.
    EXECUTE 'DROP TABLE IF EXISTS tmp_sp_sync_merge;';
    EXECUTE 'CREATE TEMPORARY TABLE tmp_sp_sync_merge (LIKE '||target_tbl||' );';
    -- Insert all changed data from source into the temp table
    EXECUTE 'INSERT INTO tmp_sp_sync_merge SELECT * FROM '||source_tbl||' WHERE '||quote_ident(last_mod_col)||' > '''||max_mod_ts||''';';
    SELECT INTO sync_count COUNT(*) FROM tmp_sp_sync_merge;
    -- Roll back if `max_rows` is set and temp table row count exceeds it
    IF max_rows <> 0 AND sync_count > max_rows THEN
        ROLLBACK;
        EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||last_mod_col||
                       ''',''ABORT - Sync exceeds `max_rows`'','''||insert_query||''','||sync_count||';';
        COMMIT; --Persist the log entry
        RAISE EXCEPTION 'Aborted - Sync row count exceeds `max_rows` value.';
    END IF;

    IF sync_count > 0 THEN
        -- MERGE pt1 - DELETE any changed rows from the target table
        EXECUTE 'DELETE FROM '||target_tbl||' WHERE '||pk_columns||' IN (SELECT '||pk_columns||' FROM tmp_sp_sync_merge);';
        -- Get row count for the DELETE
        GET DIAGNOSTICS delete_count := ROW_COUNT;
        -- Get query id for the DELETE
        SELECT INTO delete_query pg_last_query_id();

        -- MERGE pt2 - INSERT all rows (new and changes) from temp into target
        EXECUTE 'INSERT INTO '||target_tbl||' SELECT * FROM tmp_sp_sync_merge;';
        -- Get row count for the INSERT
        GET DIAGNOSTICS insert_count := ROW_COUNT;
        -- Get query id for the INSERT
        SELECT INTO insert_query pg_last_query_id();

        -- Check modified row count in target before commit
        EXECUTE 'SELECT COUNT(*) FROM '||target_tbl||' WHERE '||quote_ident(last_mod_col)||' > '''||max_mod_ts||''';' INTO change_count;
        IF sync_count = change_count THEN
            -- Commit the merge and record success
            COMMIT;
            EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                    ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||last_mod_col||
                           ''',''SUCCESS'','''||delete_query||', '||insert_query||''','||sync_count||';';
            RAISE INFO 'SUCCESS - % rows synced.', sync_count;
        
        -- Otherwise roll back changes
        ELSE 
            ROLLBACK;
            EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                    ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||last_mod_col||
                           ''',''ABORT - Changed row count was not correct~'','''||delete_query||', '||insert_query||''','||sync_count||';';
            COMMIT; -- Persist the log entry
            RAISE EXCEPTION 'Aborted - Changed row count did not match expected row count.';
        END IF;
    ELSE
        EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||last_mod_col||
                       ''',''SKIPPED'','''||insert_query||''','||sync_count||';';
        RAISE INFO 'SKIPPED - No new or changed rows found.';
    END IF;
END
$$ LANGUAGE plpgsql;



/* Usage Example:

    -- Redshift: create logging table
    DROP TABLE IF EXISTS public.sp_logs;
    CREATE TABLE public.sp_logs (
        batch_time   TIMESTAMP 
      , source_table VARCHAR
      , target_table VARCHAR
      , sync_column  VARCHAR
      , sync_status  VARCHAR
      , sync_queries VARCHAR
      , row_count    INT);

    -- Redshift: create valid target table and partially populate
    DROP TABLE IF EXISTS public.rs_tbl;
    CREATE TABLE public.rs_tbl ( 
          pk_col   INTEGER PRIMARY KEY
        , data_col VARCHAR(20)
        , last_mod TIMESTAMP);
    INSERT INTO public.rs_tbl 
    VALUES (1,'aardvark', SYSDATE), (2,'aardvarks', SYSDATE);

    -- Redshift: create **invalid** target table to test the SP
    DROP TABLE IF EXISTS public.rs_invalid;
    CREATE TABLE public.rs_invalid ( 
          pk_col   VARCHAR(20) PRIMARY KEY
        , data_col VARCHAR(20)
        , last_mod VARCHAR);
    INSERT INTO public.rs_invalid 
    VALUES (1,'aardvark', SYSDATE), (2,'aardvarks', SYSDATE);
    
    -- Postgres: create source table and populate with data
    DROP TABLE IF EXISTS public.pg_tbl;
    CREATE TABLE public.pg_tbl ( 
          pk_col   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
        , data_col VARCHAR(20)
        , last_mod TIMESTAMP);
    INSERT INTO public.pg_tbl (data_col, last_mod)
    VALUES ('aardvark', NOW()),('aardvarks', NOW()),('aardwolf', NOW()),('aardwolves', NOW());

    -- Redshift: create external schema for federated database
    -- CREATE EXTERNAL SCHEMA IF NOT EXISTS pg_fed
    -- FROM POSTGRES DATABASE 'dev' SCHEMA 'public'
    -- URI 'aurora-postgres-ro.ch1pch0psh0p.us-west-2.rds.amazonaws.com' PORT 8192
    -- IAM_ROLE 'arn:aws:iam::123456789012:role/apg-federation-role'
    -- SECRET_ARN 'arn:aws:secretsmanager:us-west-2:123456789012:secret:apg-federation-secret-187Asd';
    CREATE EXTERNAL SCHEMA IF NOT EXISTS apg_100g
    FROM POSTGRES DATABASE 'tpch_100g' SCHEMA 'public'
    URI 'apgf-instance-1.cr7d8lhiupkf.us-west-2.rds.amazonaws.com' PORT 8192
    IAM_ROLE 'arn:aws:iam::953075369322:role/apg-fed-test'
    SECRET_ARN 'arn:aws:secretsmanager:us-west-2:953075369322:secret:apgf-secret-187Asd'
    ;

    -- Redshift: call the stored procedure using the invalid table
    -- CALL sp_sync_merge_changes(SYSDATE,'pg_fed.pg_tbl','public.rs_invalid','last_mod','public.sp_logs',0);
    CALL sp_sync_merge_changes(SYSDATE,'apg_100g.pg_tbl','public.rs_invalid','last_mod','public.sp_logs',0);
    -- ERROR:  Aborted - `pk_col` is not a date/time column.
    SELECT * FROM sp_logs ORDER BY batch_time DESC LIMIT 1;
    -- -[ RECORD 1 ]+------------------------------------------
    -- batch_time   | 2020-04-01 17:49:53.413601
    -- source_table | apg_100g.pg_tbl
    -- target_table | public.rs_invalid
    -- sync_column  | pk_col
    -- sync_status  | ABORT - `last_mod_col` is not a date/time
    -- sync_queries |
    -- row_count    | 0

    -- Redshift: call the stored procedure using NULL for `max_rows`
    -- CALL sp_sync_merge_changes(SYSDATE,'pg_fed.pg_tbl','public.rs_invalid','last_mod','public.sp_logs',NULL);
    CALL sp_sync_merge_changes(SYSDATE,'apg_100g.pg_tbl','public.rs_invalid','last_mod','public.sp_logs',NULL);
    -- ERROR:  Aborted - required parameters are missing.
    SELECT * FROM sp_logs ORDER BY batch_time DESC LIMIT 1;
    -- -[ RECORD 1 ]+---------------------------
    -- batch_time   | 2020-04-01 17:47:22.406301
    -- source_table | apg_100g.pg_tbl
    -- target_table | public.rs_invalid
    -- sync_column  | pk_col
    -- sync_status  | ABORT - Empty parameters
    -- sync_queries |
    -- row_count    | 0
    
    -- Redshift: call the stored procedure with empty `last_mod_col` 
    -- CALL sp_sync_merge_changes(SYSDATE,'pg_fed.pg_tbl','public.rs_tbl','','public.sp_logs',0);
    CALL sp_sync_merge_changes(SYSDATE,'apg_100g.pg_tbl','public.rs_tbl','','public.sp_logs',0);
    -- ERROR:  Aborted - required parameters are missing.
    SELECT * FROM sp_logs ORDER BY batch_time DESC LIMIT 1;
    -- -[ RECORD 1 ]+---------------------------
    -- batch_time   | 2020-04-01 17:53:55.863634
    -- source_table | apg_100g.pg_tbl
    -- target_table | public.rs_tbl
    -- sync_column  |
    -- sync_status  | ABORT - Empty parameters
    -- sync_queries |
    -- row_count    | 0
    
    -- Redshift: call the stored procedure with max_rows = 1
    -- CALL sp_sync_merge_changes(SYSDATE,'pg_fed.pg_tbl','public.rs_tbl','last_mod','public.sp_logs',1);
    CALL sp_sync_merge_changes(SYSDATE,'apg_100g.pg_tbl','public.rs_tbl','last_mod','public.sp_logs',1);
    -- INFO:  A maximum of 1 rows will be inserted.
    -- ERROR:  Aborted - Sync row count exceeds `max_rows` value.
    SELECT * FROM sp_logs ORDER BY batch_time DESC LIMIT 1;
    -- -[ RECORD 1 ]+--------------------------------
    -- batch_time   | 2020-04-01 18:00:15.649098
    -- source_table | apg_100g.pg_tbl
    -- target_table | public.rs_tbl
    -- sync_column  | last_mod
    -- sync_status  | ABORT - Sync exceeds `max_rows`
    -- sync_queries |
    -- row_count    | 4

    -- Redshift: call the stored procedure with max_rows unlimited
    CALL sp_sync_merge_changes(SYSDATE,'pg_fed.pg_tbl','public.rs_tbl','last_mod','public.sp_logs',0);
    -- INFO:  Max of `last_mod_col` is 2020-04-01 17:36:36.392182 .
    -- INFO:  SUCCESS - 4 rows synced.
    SELECT * FROM sp_logs ORDER BY batch_time DESC LIMIT 1;
    -- -[ RECORD 1 ]+---------------------------
    -- batch_time   | 2020-04-01 18:03:21.640183
    -- source_table | apg_100g.pg_tbl
    -- target_table | public.rs_tbl
    -- sync_column  | last_mod
    -- sync_status  | SUCCESS
    -- sync_queries | 1254325
    -- row_count    | 4
    
    -- Redshift: call the stored procedure again
    CALL sp_sync_merge_changes(SYSDATE,'pg_fed.pg_tbl','public.rs_tbl','last_mod','public.sp_logs',0);
    -- INFO:  SKIPPED - No new or changed rows found.
    SELECT * FROM sp_logs ORDER BY batch_time DESC LIMIT 1;
    -- -[ RECORD 1 ]+---------------------------
    -- batch_time   | 2020-04-01 18:04:59.511929
    -- source_table | apg_100g.pg_tbl
    -- target_table | public.rs_tbl
    -- sync_column  | last_mod
    -- sync_status  | SKIPPED
    -- sync_queries |
    -- row_count    | 0


*/

