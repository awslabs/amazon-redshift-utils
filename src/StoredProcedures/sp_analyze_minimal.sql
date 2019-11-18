/**********************************************************************************************
Purpose: Analyze **ONE** column of a table. To be used on a staging table right after loading

Notes:   It will analyze the first column of the SK, the DK or the first column of the table

Parameters:
    schema_name:     Schema
    table_name:      Table
    analyze_percent: Percent Threshold for analyze
    wait_for_lock :  Wait for table locks

Usage:
    CALL sp_analyze_minimal('public','mytable');
    CALL sp_analyze_minimal('public','mytable', 1, True);

History:
2019-06-12 ericfe Created as "MinAnalyze"
2019-11-12 joeharris76 Renamed to "sp_analyze_minimal" and revised to standard style and format
**********************************************************************************************/

-- DROP PROCEDURE sp_analyze_minimal(VARCHAR, VARCHAR, INT, BOOLEAN);
CREATE OR REPLACE PROCEDURE sp_analyze_minimal (
      schema_name     VARCHAR
    , table_name      VARCHAR
    , analyze_percent INT
    , wait_for_lock   BOOLEAN ) 
AS $$
DECLARE
    schema_n   VARCHAR;
    anlyz_pct  INT;
    tbl_locks  RECORD;
    anlyz_set  VARCHAR(MAX);
    anlyz_sql  VARCHAR(MAX);
BEGIN
    -- Default to public schema
    schema_n := NVL(schema_name,'public');
    -- Default to 1 percent
    anlyz_pct := NVL(analyze_percent,1);
    -- Generagte ANALYZE SQL
    anlyz_set := 'SET ANALYZE_THRESHOLD_PERCENT TO '||anlyz_pct::varchar||'; ';
    SELECT INTO anlyz_sql 'ANALYZE '||n.nspname||'.'||c.relname||' ('||NVL(NVL(srtk.attname,dstk.attname),cols.attname)||');' AS sql
    FROM pg_namespace n 
    JOIN pg_class c             ON n.oid = c.relnamespace
    JOIN pg_attribute      cols ON cols.attrelid = c.oid AND cols.attnum = 1
    LEFT JOIN pg_attribute srtk ON srtk.attrelid = c.oid AND srtk.attsortkeyord = 1
    LEFT JOIN pg_attribute dstk ON dstk.attrelid = c.oid AND dstk.attisdistkey = 't'
    WHERE c.relname = LOWER(table_name)::CHAR(128)
      AND n.nspname = LOWER(schema_n)::CHAR(128)
      AND c.relkind = 'r';
    IF FOUND then
        -- BODY  
        SELECT INTO tbl_locks svv.xid, l.pid, svv.txn_owner username, l.mode, l.granted
        FROM pg_locks          l
        JOIN svv_transactions  svv ON l.pid = svv.pid
                                   AND l.relation = svv.relation
                                   AND svv.lockable_object_type IS NOT NULL
        LEFT JOIN pg_class     c   ON c.oid = svv.relation
        LEFT JOIN pg_namespace n   ON n.oid = c.relnamespace
        LEFT JOIN pg_database  d   ON d.oid = l.database
        LEFT JOIN stv_recents  rct ON rct.pid = l.pid
        WHERE l.pid <> PG_BACKEND_PID()
          AND l.granted = true
          AND n.nspname = schema_name::CHAR(128)
          AND c.relname = table_name::CHAR(128);
        IF FOUND THEN
            IF wait_for_lock THEN
                RAISE NOTICE 'User % has table locked in % mode. ANALYZE will wait until the lock is released.',tbl_locks.username,tbl_locks.mode;
                RAISE NOTICE 'Use ''SELECT PG_TERMINATE_BACKEND(%);'' on the session holding the lock if needed.',tbl_locks.pid;
                EXECUTE anlyz_set; EXECUTE anlyz_sql;
            ELSE
                RAISE NOTICE    'User % has table locked in % mode. Try again to see if it has been released.',tbl_locks.username,tbl_locks.mode;
                RAISE EXCEPTION 'Use ''SELECT PG_TERMINATE_BACKEND(%);'' to kill session holding the lock if needed.', tbl_locks.pid;
            END IF;
        ELSE
            RAISE INFO 'Running: %', anlyz_sql;
            EXECUTE anlyz_set; EXECUTE anlyz_sql;
        END IF;
    ELSE
          RAISE EXCEPTION 'No table found';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE sp_analyze_minimal (
      schema_name     VARCHAR
    , table_name      VARCHAR ) 
AS $$
BEGIN
    -- Will wait by default
    CALL sp_analyze_minimal(schema_name, table_name, 0, TRUE);
END;
$$ LANGUAGE plpgsql;


/* Usage Example: 

    DROP TABLE IF EXISTS public.tmp_analyze;
    CREATE TABLE public.tmp_analyze ( 
          pkey_col   INTEGER PRIMARY KEY
        , second_col INTEGER);
    INSERT INTO public.tmp_analyze VALUES
        (100,7001),(101,20225),(102,22772),(103,4577);
    -- Call the stored proc
    CALL sp_analyze_minimal('public','tmp_analyze', 0, TRUE);
    -- INFO:  Running: ANALYZE public.tmp_analyze (sortkey_col);
    
    DROP TABLE IF EXISTS public.tmp_analyze;
    CREATE TABLE public.tmp_analyze ( 
          distkey_col  INTEGER DISTKEY
        , second_col   INTEGER);
    INSERT INTO public.tmp_analyze VALUES
        (100,7001),(101,20225),(102,22772),(103,4577);
    -- Call the stored proc
    CALL sp_analyze_minimal('public','tmp_analyze');
    -- INFO:  Running: ANALYZE public.tmp_analyze (sortkey_col);
    
    DROP TABLE IF EXISTS public.tmp_analyze;
    CREATE TABLE public.tmp_analyze ( 
          sortkey_col  INTEGER SORTKEY
        , second_col   INTEGER);
    INSERT INTO public.tmp_analyze VALUES
        (100,7001),(101,20225),(102,22772),(103,4577);
    -- Call the stored proc
    CALL sp_analyze_minimal('public','tmp_analyze', 1, True);
    -- INFO:  Running: ANALYZE public.tmp_analyze (sortkey_col);

    DROP TABLE IF EXISTS public.tmp_analyze;

*/
