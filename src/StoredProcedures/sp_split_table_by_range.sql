/**********************************************************************************************
Purpose: Split a large table into parts using a numeric column

Notes:  Splits the provided MIN/MAX values into equal ranges
        Strongly recommend splitting on the sort key column if possible
Logic:
        FOR split = 2 LOOP
            CREATE TABLE IF NOT EXISTS $(schema).$(table)_002 (LIKE $(schema).$(table));
            INSERT INTO $(schema).$(table)_002 SELECT * FROM $(schema).$(table) LIMIT 100;
            DELETE FROM $(schema).$(table)_002;
            INSERT INTO $(schema).$(table)_002 SELECT * FROM $(schema).$(table) WHERE $(split_column) >= $(min_value) AND $(split_column) < $(max_value);
        END LOOP;
        {CHECK SUM(COUNT(*)) FROM ALL SPLITS == COUNT(*) FROM SOURCE TABLE}
Input:
        Requires the following temp table (see Testing at bottom): 
        CREATE TEMP TABLE tmp_split_tables (
            schema_name   VARCHAR,
            table_name    VARCHAR,
            split_key_col VARCHAR,
            split_count   INTEGER,
            min_sk_val    BIGINT,
            max_sk_val    BIGINT);

History:
2019-10-29 - joeharris76 - Created
**********************************************************************************************/

-- DROP PROCEDURE sp_split_table_by_range( );
CREATE OR REPLACE PROCEDURE sp_split_table_by_range ()
AS $$
DECLARE
  split_config   RECORD;
  source_tbl_n   VARCHAR := '';
  target_tbl_n   VARCHAR := '';
  source_row_cnt BIGINT := 0;
  target_row_cnt BIGINT := 0;
  range_start    BIGINT := 0;
  range_end      BIGINT := 0;
  range_interval BIGINT := 0;
  total_range    BIGINT := 0;
  tbl_cnt        SMALLINT := 0;
  split_cnt      SMALLINT := 0;
  split_cnt_val  VARCHAR(10) := '';
  split_sql      VARCHAR(MAX) := '';
  check_sql      VARCHAR(MAX) := '';
  rows_affected  BIGINT;
BEGIN
    -- Iterate over the list of tables being split
    FOR split_config IN SELECT * FROM tmp_split_tables
    LOOP
        tbl_cnt := tbl_cnt + 1;
        split_cnt := 0;
        check_sql := '';
        source_tbl_n := quote_ident(split_config.schema_name)||'.'||quote_ident(split_config.table_name);
        RAISE INFO 'Starting split #%, splitting `%`', tbl_cnt, source_tbl_n;
        total_range := split_config.max_sk_val - split_config.min_sk_val;
        range_interval := CEIL(total_range::FLOAT/split_config.split_count)::BIGINT;
        RAISE INFO ' >> Total splits = %, interval = %, total range = %', split_config.split_count, range_interval, total_range;
        -- Iterate over the number of splits
        WHILE split_cnt <= split_config.split_count
        LOOP
            split_cnt_val := LPAD(split_cnt::VARCHAR,3,'0');
            IF split_cnt > 0 THEN
                range_start := split_config.min_sk_val+(range_interval*(split_cnt-1));
                IF split_cnt = split_config.split_count THEN
                    range_end := split_config.max_sk_val+1;
                ELSE            
                    range_end := split_config.min_sk_val+(range_interval*split_cnt);
                END IF;
            END IF;
            target_tbl_n := quote_ident(split_config.schema_name)||'.'||quote_ident(split_config.table_name||'_'||split_cnt_val);
            RAISE INFO ' > Split part _%: Starting',split_cnt_val;
            EXECUTE 'DROP TABLE IF EXISTS '||target_tbl_n||' ;';
            EXECUTE 'CREATE TABLE IF NOT EXISTS '||target_tbl_n||' (LIKE '||source_tbl_n||');';
            EXECUTE 'INSERT INTO '||target_tbl_n||' SELECT * FROM '||source_tbl_n||' LIMIT 100;';
            EXECUTE 'DELETE FROM '||target_tbl_n||';';
            split_sql :=  'INSERT INTO '||target_tbl_n||' SELECT * FROM '||source_tbl_n;
            IF split_cnt = 0 THEN -- Zero split for rows with NULL in the split column
                split_sql :=  split_sql||' WHERE '||quote_ident(split_config.split_key_col)||' IS NULL;';
            ELSE 
                split_sql :=  split_sql||' WHERE '||quote_ident(split_config.split_key_col)||' >= '||range_start
                                       ||' AND '||quote_ident(split_config.split_key_col)||' < '||range_end||';';
            END IF;
            EXECUTE split_sql;
            GET DIAGNOSTICS rows_affected := ROW_COUNT;
            RAISE INFO ' - Split part _%: Rows inserted = %', split_cnt_val, rows_affected;
            COMMIT; -- Commit to avoid rework if we encounter an issue
            IF check_sql <> '' THEN
                check_sql := check_sql||' UNION ALL ';
            END IF;
            check_sql := check_sql||' SELECT COUNT(*) row_cnt FROM '||target_tbl_n ;
            split_cnt := split_cnt + 1;
        END LOOP;
        split_sql := 'SELECT COUNT(*) FROM '||source_tbl_n||';';
        EXECUTE split_sql INTO source_row_cnt ;
        split_sql := 'SELECT SUM(row_cnt) FROM ('||check_sql||');';
        EXECUTE split_sql INTO target_row_cnt ;
        RAISE INFO ' -- Complete: Split rows: % / Source rows: %',target_row_cnt, source_row_cnt;
        --Check that the row counts match
        IF source_row_cnt <> target_row_cnt THEN
            RAISE EXCEPTION 'ERROR: Split failed for %: Total split table rows does not match origin row count.', source_tbl_n;
        ELSE
            RAISE INFO 'SUCCESS: Split complete for %',source_tbl_n ;
            -- NOTE: Add logic here to auto-delete the original table
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


/* Usage Example:

    -- Find the min.max sortk keys for the tables to be split
    -- SELECT 'public', 'store_sales',     'ss_sold_date_sk',     MIN(ss_sold_date_sk    ), MAX(ss_sold_date_sk    ) FROM public.store_sales     UNION ALL
    -- SELECT 'public', 'catalog_sales',   'cs_sold_date_sk',     MIN(cs_sold_date_sk    ), MAX(cs_sold_date_sk    ) FROM public.catalog_sales   UNION ALL
    -- SELECT 'public', 'web_sales',       'ws_sold_date_sk',     MIN(ws_sold_date_sk    ), MAX(ws_sold_date_sk    ) FROM public.web_sales       UNION ALL
    -- SELECT 'public', 'store_returns',   'sr_returned_date_sk', MIN(sr_returned_date_sk), MAX(sr_returned_date_sk) FROM public.store_returns   UNION ALL
    -- SELECT 'public', 'catalog_returns', 'cr_returned_date_sk', MIN(cr_returned_date_sk), MAX(cr_returned_date_sk) FROM public.catalog_returns UNION ALL
    -- SELECT 'public', 'web_returns',     'wr_returned_date_sk', MIN(wr_returned_date_sk), MAX(wr_returned_date_sk) FROM public.web_returns     ;

    -- Config table declares the sort key range and how many parts to split into
    CREATE TEMP TABLE tmp_split_tables ( schema_name VARCHAR, table_name VARCHAR, split_key_col VARCHAR, split_count INTEGER, min_sk_val BIGINT, max_sk_val BIGINT);
    INSERT INTO tmp_split_tables VALUES
          ('public', 'store_sales',     'ss_sold_date_sk',     25, 2450816, 2452642)
        , ('public', 'catalog_sales',   'cs_sold_date_sk',     20, 2450821, 2452924)
        , ('public', 'web_sales',       'ws_sold_date_sk',     10, 2450816, 2452642)
        , ('public', 'store_returns',   'sr_returned_date_sk',  6, 2450820, 2452822)
        , ('public', 'catalog_returns', 'cr_returned_date_sk',  4, 2450815, 2452654)
        , ('public', 'web_returns',     'wr_returned_date_sk',  2, 2450819, 2453002);

    -- Call the stored procedure    
    CALL sp_split_table_by_range();
    -- INFO:  Starting split #1, splitting `public.catalog_sales`
    -- INFO:   >> Total splits = 20, interval = 106, total range = 2103
    -- INFO:   > Split part _001: Starting
    -- INFO:   - Split part _001: Rows inserted = 14288879
    -- INFO:   > Split part _002: Starting
    -- INFO:   - Split part _002: Rows inserted = 15294501
    -- INFO:   > Split part _003: Starting
    -- INFO:   - Split part _003: Rows inserted = 35862731
    -- INFO:   > Split part _004: Starting
    -- INFO:   - Split part _004: Rows inserted = 27950288
    -- INFO:   > Split part _005: Starting
    -- INFO:   - Split part _005: Rows inserted = 14426566
    -- INFO:   > Split part _006: Starting
    -- INFO:   - Split part _006: Rows inserted = 25537838
    -- INFO:   > Split part _007: Starting
    -- INFO:   - Split part _007: Rows inserted = 39275422
    -- INFO:   > Split part ...

*/