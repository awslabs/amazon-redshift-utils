/**********************************************************************************************
Purpose: Check the integrity of the PRIMARY KEY declared on a table

Notes:   Only checks PK declared to Redshift. Will error if no PK on the table.

Parameters:
        batch_time  : Timestamp for this batch. Can be used to group multiple fixes
        check_table : Schema qualified name of table to be queried
        check_action: Action to be taken when PK errors are found
                      * "LOG" => PK errors will be logged to `log_table`
                      * "FIX" => PK duplicates will be removed
        log_table   : Schema qualified table where actions are to be logged
                      DDL structure must be as follows:
                        CREATE TABLE $(log_table) (
                            batch_time   TIMESTAMP,
                            check_table  VARCHAR,
                            check_time   TIMESTAMP,
                            check_status VARCHAR,
                            error_count  INT);
        max_fix_rows: Maximum number of PK errors to correct automatically

History:
2019-10-25 - joeharris76 - Created
**********************************************************************************************/

-- DROP PROCEDURE sp_check_primary(TIMESTAMP,VARCHAR,VARCHAR,VARCHAR,INTEGER);
CREATE OR REPLACE PROCEDURE sp_check_primary_key (
      batch_time   IN TIMESTAMP
    , check_table  IN VARCHAR(256)
    , check_action IN VARCHAR(10)
    , log_table    IN VARCHAR(256)
    , max_fix_rows IN INTEGER)
AS $$
DECLARE
    sql        VARCHAR(MAX) := '';
    pk_columns VARCHAR(512) := '';
    dupe_count INTEGER := 0;
    temp_count INTEGER := 0;
BEGIN
    IF check_action NOT IN ('LOG', 'FIX') THEN
         RAISE EXCEPTION 'The `check_action` parameter must be "LOG" or "FIX".';
    END IF;
    IF check_table='' OR log_table='' THEN
         RAISE EXCEPTION 'Parameters `check_table` and `log_table` cannot be empty.';
    END IF;
    IF check_action='FIX' AND max_fix_rows<=0 THEN
         RAISE EXCEPTION 'Parameter `max_fix_rows` must be >0 when `check_action` = "FIX".';
    END IF;
    --Retrieve the primary key column(s) for the table
    sql := 'SELECT REPLACE(REPLACE(pg_get_constraintdef(con.oid),''PRIMARY KEY ('',''''),'')'','''')'||
           ' FROM pg_constraint AS con '||
           ' JOIN pg_class      AS tbl ON tbl.relnamespace = con.connamespace AND tbl.oid = con.conrelid'||
           ' JOIN pg_namespace  AS sch ON sch.oid =tbl.relnamespace'||
           ' WHERE tbl.relkind = ''r'' AND con.contype = ''p''';
    IF CHARINDEX('.',check_table) > 0 THEN
        sql := sql||' AND sch.nspname = \''||quote_ident(SPLIT_PART(check_table,'.',1))||'\''
                  ||' AND tbl.relname = \''||quote_ident(SPLIT_PART(check_table,'.',2))||'\';';
    ELSE
        sql := sql||' AND tbl.relname = '''||check_table||''';';
    END IF;
    EXECUTE sql||' ;' INTO pk_columns;
    IF pk_columns = '' THEN
         RAISE INFO 'No PRIMARY KEY found for table "%".',check_table;
    ELSE
        --Count the number of duplicates in the PRIMARY KEY
        sql := 'SELECT SUM(dupes) FROM (SELECT '||pk_columns||', COUNT(*) dupes'||
                ' FROM '||check_table||' GROUP BY '||pk_columns||' HAVING COUNT(*) > 1)';
        EXECUTE sql||' ;' INTO dupe_count;
        IF dupe_count IS NULL THEN
            dupe_count := 0;
        END IF;
        IF dupe_count = 0 THEN
             EXECUTE 'INSERT INTO '||log_table||' SELECT '''||batch_time||''','''||check_table||''', '''||SYSDATE||''',''OK - No duplicates found'',0;';
             RAISE INFO 'OK - No duplicates found';
        ELSIF check_action = 'LOG' THEN
             EXECUTE 'INSERT INTO '||log_table||' SELECT '''||batch_time||''','''||check_table||''', '''||SYSDATE||''',''ERROR (LOG) - Duplicates found'','||dupe_count||';';
             RAISE INFO 'ERROR (LOG) - % Duplicates found',dupe_count;
        ELSIF check_action = 'FIX' THEN
            IF max_fix_rows <= dupe_count THEN --Too many dupes, cannot fix
                 EXECUTE 'INSERT INTO '||log_table||' SELECT '''||batch_time||''','''||check_table||''', '''||SYSDATE||''',''ERROR (FIX) - Duplicate count exceeds `max_fix_rows` value.'','||dupe_count||';';
                 RAISE INFO 'ERROR (FIX) - Duplicate count % exceeds `max_fix_rows` %',dupe_count,max_fix_rows;
            ELSE --Attempt to correct the PK
                EXECUTE 'DROP TABLE IF EXISTS tmp_sp_fix_pk;';
                EXECUTE 'CREATE TEMPORARY TABLE tmp_sp_fix_pk (LIKE '||check_table||' );';
                --Insert distinct rows for PK duplicates into temp table
                EXECUTE 'INSERT INTO tmp_sp_fix_pk'||
                       ' SELECT DISTINCT * FROM '||check_table||' WHERE ('||pk_columns||') IN (SELECT '||pk_columns||
                         ' FROM '||check_table||' GROUP BY '||pk_columns||' HAVING COUNT(*) > 1)';
                --Check that PK duplciates are removed in the temp table
                EXECUTE 'SELECT COUNT(*) FROM (SELECT '||pk_columns||
                        ' FROM tmp_sp_fix_pk GROUP BY '||pk_columns||' HAVING COUNT(*) > 1)' INTO temp_count ;
                IF temp_count > 0 THEN
                    EXECUTE 'INSERT INTO '||log_table||' SELECT '''||batch_time||''','''||check_table||''','''||SYSDATE||''',''ERROR (FIX) - Failed. Duplicate PK rows are not identical.'','||dupe_count||';';
                    RAISE INFO 'ERROR (FIX) - Failed. Duplicate PK rows are not identical';
                ELSE
                    --Delete all rows for the PK duplicates from the source table
                    EXECUTE 'DELETE FROM '||check_table||' WHERE ('||pk_columns||') IN (SELECT '||pk_columns||' FROM tmp_sp_fix_pk);';
                    --Insert the deduped rows from the temp table into the source
                    EXECUTE 'INSERT INTO '||check_table||' SELECT * FROM tmp_sp_fix_pk;';
                    --Update the log for the fix
                    EXECUTE 'INSERT INTO '||log_table||' SELECT '''||batch_time||''','''||check_table||''','''||SYSDATE||''',''SUCCESS (FIX) - Duplicates corrected.'','||dupe_count||';';
                    --Commit the fixed data
                    COMMIT;
                    RAISE INFO 'SUCCESS (FIX) - % duplicates corrected.',dupe_count;
                END IF;
            END IF;
        ELSE
            RAISE EXCEPTION 'ERROR in `sp_check_primary_key` execution';
        END IF;
    END IF;
END
$$ LANGUAGE plpgsql;


/* Usage Example:

    DROP TABLE IF EXISTS tmp_pk_log;
    CREATE TABLE tmp_pk_log(
          batch_time   TIMESTAMP
        , check_table  VARCHAR
        , check_time   TIMESTAMP
        , check_status VARCHAR
        , error_count  INT);
    DROP TABLE IF EXISTS tmp_pk_test;
    CREATE TEMP TABLE tmp_pk_test ( 
          pk_col INTEGER PRIMARY KEY
        , entries INTEGER);
    INSERT INTO tmp_pk_test VALUES
          (100,  7001) 
        , (100,  7001)
        , (100,  7001)
        , (100,  7001)
        , (101, 20225)
        , (102, 22772)
        , (103,  4577);
    CALL sp_check_primary_key(SYSDATE,'tmp_pk_test','LOG','tmp_pk_log',0);
    -- INFO:  ERROR (LOG) - 4 Duplicates found
    SELECT * FROM tmp_pk_log;
    -- -[ RECORD 1 ]+-------------------------------
    -- batch_time   | 2019-10-31 00:01:33.849821
    -- check_table  | tmp_pk_test
    -- check_time   | 2019-10-31 00:01:33.849821
    -- check_status | ERROR (LOG) - Duplicates found
    -- error_count  | 4

    CALL sp_check_primary_key(SYSDATE,'tmp_pk_test','FIX','tmp_pk_log',3);
    -- INFO:  ERROR (FIX) - Duplicate count 4 exceeds `max_fix_rows` 3
    SELECT * FROM tmp_pk_log;
    -- -[ RECORD 1 ]+------------------------------------------------------------
    -- batch_time   | 2019-10-31 00:01:34.479395
    -- check_table  | tmp_pk_test
    -- check_time   | 2019-10-31 00:01:34.479395
    -- check_status | ERROR (FIX) - Duplicate count exceeds `max_fix_rows` value.
    -- error_count  | 4

    CALL sp_check_primary_key(SYSDATE,'tmp_pk_test','FIX','tmp_pk_log',99);
    -- INFO:  SUCCESS (FIX) - 4 duplicates corrected.
    SELECT * FROM tmp_pk_log;
    -- -[ RECORD 1 ]+------------------------------------------------------------
    -- batch_time   | 2019-10-31 00:01:34.892679
    -- check_table  | tmp_pk_test
    -- check_time   | 2019-10-31 00:01:34.892679
    -- check_status | SUCCESS (FIX) - Duplicates corrected.
    -- error_count  | 4
*/

