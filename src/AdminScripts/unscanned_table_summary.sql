/**********************************************************************************************
Purpose: Returns a summary of storage consumption by all tables and all unscanned tables. 

Columns:
total_table_storage:      # of tables (all), size of tables (all), cluster storage (constant)
unscanned_table_storage:  # of tables (unscanned), size of tables (unscanned), cluster storage

Notes:

- A table is considered scanned or unscanned based on logs in STL_SCAN. This query would 
erroroneously count a table as unscanned if table is only scanned sparingly and the log has 
rotated from the system tables by the time this query is run. 
- Leverage regular queries against the view defined in 
amazon-redshift-utils/src/AdminViews/v_get_tbl_scan_frequency.sql to determine if a table is
truely unscanned over longer periods of time. 

History:
2016-01-18 chriz-bigdata created
2020-11-17 marynap chnaged storage calculation, showing data per cluster  
**********************************************************************************************/

WITH
        nodes AS (SELECT COUNT(DISTINCT node) nodenum FROM stv_slices WHERE TYPE = 'D'), 
        storage as (SELECT  nodes.nodenum * ( CASE 
            WHEN capacity IN (381407, 190633, 361859)
                THEN 160.0 / 1024
            WHEN capacity IN (380319, 760956)
                THEN 2.56 
            WHEN capacity IN (1906314, 952455)
                THEN 2 
            WHEN capacity = 945026
                THEN 16 
            WHEN capacity = 3339176
                THEN 64 
            ELSE NULL
            END::decimal(7,2)) AS total_storage
    FROM stv_partitions, nodes
    WHERE part_begin = 0
        AND failed = 0 
        group by 1
        ), 
    table_scans AS (
        SELECT 
            tbl,
            COUNT(DISTINCT query) num_qs 
        FROM 
            stl_scan s 
        WHERE 
            s.userid > 1 
            AND s.type = 2 
        GROUP BY 
            tbl),
    table_sizes AS (
        SELECT 
            tbl,
            count(*)/1024.0/1024.0 AS size 
        FROM stv_blocklist 
        GROUP BY 
            tbl
        ),
    scan_aggs AS (
        SELECT 
            ROUND(SUM(tz.size),2) total_table_size,
            COUNT(stvp.id) AS total_table_num,
            SUM(NVL2(ts.num_qs, 0, 1)) AS num_unscanned_tables,
            ROUND(SUM(NVL2(ts.num_qs, 0, tz.size)),2) AS size_unscanned_tables 
        FROM 
        (SELECT 
            id 
        FROM stv_tbl_perm 
        GROUP BY 
            id) stvp
        LEFT JOIN table_sizes tz ON stvp.id = tz.tbl
        LEFT JOIN table_scans ts ON stvp.id = ts.tbl)
SELECT
    total_table_num || ' total tables @ ' || total_table_size || 'TB / ' || total_storage || 'TB (' || ROUND(100.0*total_table_size/total_storage,2) || '%)' AS total_table_storage,
    num_unscanned_tables || ' unscanned tables @ ' || size_unscanned_tables || 'TB / ' || total_storage || 'TB (' || ROUND(100.0*size_unscanned_tables/total_storage,2) || '%)' AS unscanned_table_storage
FROM
    scan_aggs,storage;
