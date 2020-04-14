/**********************************************************************************************
Purpose: Get new rows from source table and insert them into a target table

Notes: * Useful for keeping a up-to-date Redshift copy of a remote Federated table
       * Insert any rows from a source table with an ID higher than the max in the target table
       * Source and target tables must have same table structure (column order and data types)

Parameters:
        batch_time: Timestamp for this batch. Used to timestamp the log
        source_tbl: Schema qualified name of external source table
        target_tbl: Schema qualified name of local target table
        id_column : Name of auto-incrementing ID column used to find new rows
        log_table : Schema qualified name of table where actions are logged
                      CREATE TABLE $(log_table) (
                          batch_time   TIMESTAMP
                        , source_table VARCHAR
                        , target_table VARCHAR
                        , sync_column  VARCHAR
                        , sync_status  VARCHAR
                        , sync_queries VARCHAR --Query ID of the INSERT
                        , row_count    INT);
        max_rows  : Maximum number of rows allowed to sync automatically

History:
2020-04-01 - joeharris76 - Created
**********************************************************************************************/

-- DROP PROCEDURE sp_sync_get_new_rows(TIMESTAMP,VARCHAR,VARCHAR,VARCHAR,VARCHAR,BIGINT);
CREATE OR REPLACE PROCEDURE sp_sync_get_new_rows (
      batch_time IN TIMESTAMP
    , source_tbl IN VARCHAR(256) --Schema qualified with external schema
    , target_tbl IN VARCHAR(256) --Schema qualified or `public` is assumed
    , id_column  IN VARCHAR(128)
    , log_table  IN VARCHAR(256)
    , max_rows   IN BIGINT)
AS $$
DECLARE
    sql          VARCHAR(MAX) := '';
    rs_tbl       VARCHAR := '';
    rs_schm      VARCHAR := '';
    is_numeric   BOOLEAN := FALSE;
    max_id       BIGINT  := 0;
    insert_query VARCHAR := '';
    insert_count INTEGER := 0;
BEGIN
    -- Check that required parameters are set
    IF source_tbl='' OR target_tbl='' OR id_column='' OR log_table='' OR max_rows='' THEN
        EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||id_column||
                       ''',''ABORT - Empty parameters'','''||insert_query||''','||insert_count||';';
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
    
    --Check that `id_column` is a numeric value
    sql := 'SELECT CASE WHEN a.atttypid IN (20,21,23,700,701,1700) THEN TRUE ELSE FALSE END '||
           '  FROM pg_attribute a '||
           '  JOIN pg_class     b     ON a.attrelid = b.relfilenode '||
           '  JOIN pg_namespace c     ON b.relnamespace = c.oid '||
           ' WHERE TRIM(a.attname) = '''||quote_ident(id_column)||''''||
           '   AND TRIM(c.nspname) = '''||rs_schm||''''||
           '   AND TRIM(b.relname) = '''||rs_tbl||''''||
           'LIMIT 1;';
    EXECUTE sql INTO is_numeric;
    IF is_numeric IS FALSE THEN
        EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||id_column||
                       ''',''ABORT - `id_column` is not numeric'','''||insert_query||''','||insert_count||';';
        COMMIT; --Persist the log entry
        RAISE EXCEPTION 'Aborted - `%` is not a numeric column.', id_column;
    END IF;
    
    --Get the max value of `id_column` in `target_tbl`
    sql := 'SELECT MAX('||id_column||') FROM '||target_tbl||' ;';
    EXECUTE sql INTO max_id;
    IF max_id = 0 OR max_id IS NULL THEN
        EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||id_column||
                       ''',''ABORT - Invalid `max_id`'','''||insert_query||''','||insert_count||';';
        COMMIT; --Persist the log entry
        RAISE EXCEPTION 'Aborted - `max_id` was not retrieved correctly.';
    END IF;

    --Run the insert. 
        -- Note that we are already inside a transaction
        -- We can check `max_rows` against the INSERT row count and roll back if needed
        -- This means we SELECT from the source table **just once**.
    EXECUTE 'INSERT INTO '||target_tbl||' SELECT * FROM '||source_tbl||' WHERE '||quote_ident(id_column)||' > '||max_id||';';
    --Get row count for the INSERT
    GET DIAGNOSTICS insert_count := ROW_COUNT;
    --Get query id for the INSERT
    SELECT INTO insert_query pg_last_query_id();
    
    --Roll back if `max_rows` is set and insert rows exceeds it
    IF max_rows <> 0 AND insert_count > max_rows THEN
        ROLLBACK;
        EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||id_column||
                       ''',''ABORT - Sync exceeds `max_rows`'','''||insert_query||''','||insert_count||';';
        COMMIT; --Persist the log entry
        RAISE EXCEPTION 'Aborted - Sync row count exceeds `max_rows` value.';

    --Otherwise commit the insert and record success
    ELSE
        IF insert_count > 0 THEN 
            COMMIT; --Persist the insert
            EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                    ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||id_column||
                           ''',''SUCCESS'','''||insert_query||''','||insert_count||';';
            RAISE INFO 'SUCCESS - % rows inserted.', insert_count;
        ELSE 
            EXECUTE 'INSERT INTO '||log_table||' (batch_time, source_table, target_table, sync_column, sync_status, sync_queries, row_count)'||
                    ' SELECT '''||batch_time||''','''||source_tbl||''','''||target_tbl||''','''||id_column||
                           ''',''SKIPPED'','''||insert_query||''','||insert_count||';';
            RAISE INFO 'SKIPPED - No new rows found.';
        END IF;
    END IF;
END
$$ LANGUAGE plpgsql;



/* Usage Example:

    -- Postgres: create source table and populate with data
    DROP TABLE IF EXISTS public.pg_tbl;
    CREATE TABLE public.pg_tbl ( 
          pk_col   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
        , data_col VARCHAR(20));
    INSERT INTO public.pg_tbl (data_col)
    VALUES ('aardvark'),('aardvarks'),('aardwolf'),('aardwolves');
    
    -- Redshift: create external schema for federated database
    CREATE EXTERNAL SCHEMA IF NOT EXISTS pg_fed
    FROM POSTGRES DATABASE 'dev' SCHEMA 'public'
    URI 'aurora-postgres-ro.ch1pch0psh0p.us-west-2.rds.amazonaws.com' PORT 8192
    IAM_ROLE 'arn:aws:iam::123456789012:role/apg-federation-role'
    SECRET_ARN 'arn:aws:secretsmanager:us-west-2:123456789012:secret:apg-federation-secret-187Asd';
    
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
          pk_col   INTEGER
        , data_col VARCHAR(20));
    INSERT INTO public.rs_tbl 
    VALUES (1,'aardvark'), (2,'aardvarks');

    -- Redshift: create **invalid** target table to test the SP
    DROP TABLE IF EXISTS public.rs_invalid;
    CREATE TABLE public.rs_invalid ( 
          pk_col   VARCHAR(20)
        , data_col VARCHAR(20));
    
    -- Redshift: call the stored procedure using the invalid table
    CALL sp_sync_get_new_rows(SYSDATE,'pg_fed.pg_tbl','public.rs_invalid','pk_col','public.sp_logs',0);
    -- ERROR:  Aborted - `pk_col` is not a numeric column.
    SELECT * FROM sp_logs ORDER BY batch_time DESC LIMIT 1;
    -- -[ RECORD 1 ]+-----------------------------------
    -- batch_time   | 2020-04-09 20:23:42.856964
    -- source_table | apg_100g.pg_tbl
    -- target_table | public.rs_invalid
    -- sync_column  | pk_col
    -- sync_status  | ABORT - `pk_col` is not numeric
    -- sync_queries |
    -- row_count    | 0
    
    -- Redshift: call the stored procedure with missing parameters
    CALL sp_sync_get_new_rows(SYSDATE,'pg_fed.pg_tbl','public.rs_tbl','','public.sp_logs',0);
    -- ERROR:  Aborted - required parameters are missing.
    SELECT * FROM sp_logs ORDER BY batch_time DESC LIMIT 1;
    -- -[ RECORD 1 ]+-----------------------------------
    -- batch_time   | 2020-04-09 20:29:03.486522
    -- source_table | apg_100g.pg_tbl
    -- target_table | public.rs_tbl
    -- sync_column  |
    -- sync_status  | ABORT - Empty parameters
    -- sync_queries |
    -- row_count    | 0
    
    -- Redshift: call the stored procedure with max_rows = 1
    CALL sp_sync_get_new_rows(SYSDATE,'pg_fed.pg_tbl','public.rs_tbl','pk_col','public.sp_logs',1);
    -- INFO:  A maximum of 1 rows will be inserted.
    -- ERROR:  Aborted - Sync row count exceeds `max_rows` value.
    SELECT * FROM sp_logs ORDER BY batch_time DESC LIMIT 1;
    -- -[ RECORD 1 ]+-----------------------------------
    -- batch_time   | 2020-04-09 20:29:44.054005
    -- source_table | apg_100g.pg_tbl
    -- target_table | public.rs_tbl
    -- id_column    | pk_col
    -- sync_status  | ABORT - Sync exceeds `max_rows`
    -- sync_queries | 1224588
    -- row_count    | 2

    -- Redshift: call the stored procedure with max_rows unlimited
    CALL sp_sync_get_new_rows(SYSDATE,'pg_fed.pg_tbl','public.rs_tbl','pk_col','public.sp_logs',0);
    -- INFO:  SUCCESS - 2 new rows inserted into `target_tbl`.
    SELECT * FROM sp_logs ORDER BY batch_time DESC LIMIT 1;
    -- -[ RECORD 1 ]+---------------------------
    -- batch_time   | 2020-04-09 20:33:17.954014
    -- source_table | apg_100g.pg_tbl
    -- target_table | public.rs_tbl
    -- sync_column  | pk_col
    -- sync_status  | SUCCESS
    -- sync_queries | 1224618
    -- row_count    | 2
    
    -- Redshift: call the stored procedure again - no rows will sync
    CALL sp_sync_get_new_rows(SYSDATE,'pg_fed.pg_tbl','public.rs_tbl','pk_col','public.sp_logs',0);
    -- INFO:  NONE - No new rows found in `source_tbl`
    SELECT * FROM sp_logs ORDER BY batch_time DESC LIMIT 1;
    -- -[ RECORD 1 ]+-------------------------------
    -- batch_time    | 2020-04-01 00:01:34.849821
    -- source_table  | pg_fed.pg_tbl
    -- target_table  | public.rs_tbl
    -- sync_column   | pk_col
    -- sync_status   | SUCCESS
    -- sync_queries  | <NULL>
    -- row_count     | 0

*/

