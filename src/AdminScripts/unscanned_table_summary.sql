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
**********************************************************************************************/
WITH
    nodes AS (SELECT COUNT(DISTINCT node) nodenum FROM stv_slices),
    slices AS (SELECT COUNT(DISTINCT slice) slices FROM stv_slices s WHERE node=0),
    disks AS (SELECT COUNT(p.owner) disks FROM stv_partitions p WHERE p.owner=0),
    storage AS (
        SELECT 
            nodes.nodenum * (CASE 
                WHEN slices.slices = 32 THEN 2.56
                WHEN slices.slices = 16 THEN 16.00
                WHEN disks.disks > 2 THEN 2
                ELSE 0.16 END) AS total_storage
        FROM 
            nodes, slices, disks), 
    table_scans AS (
        SELECT 
            database, 
            schema, 
            table_id, 
            "table", 
            ROUND(size::float/(1024*1024)::float,2) AS size, 
            sortkey1, 
            NVL(s.num_qs,0) num_qs
        FROM svv_table_info t
        LEFT JOIN (
            SELECT
                tbl, 
                perm_table_name,
                COUNT(DISTINCT query) num_qs
            FROM
                stl_scan s
            WHERE 
                s.userid > 1
                AND s.perm_table_name NOT IN ('Internal Worktable','S3')
            GROUP BY 
                tbl, perm_table_name) s ON s.tbl = t.table_id
	WHERE t."schema" NOT IN ('pg_internal')),
    scan_aggs AS (
        SELECT 
            sum(size) AS total_table_size,
            count(*) AS total_table_num,
            SUM(CASE WHEN num_qs = 0 THEN 1 ELSE 0 END) AS num_unscanned_tables,
            SUM(CASE WHEN num_qs = 0 THEN size ELSE 0 END) AS size_unscanned_tables,
            storage.total_storage
        FROM
            table_scans, storage GROUP BY total_storage)
SELECT
    total_table_num || ' total tables @ ' || total_table_size || 'TB / ' || total_storage || 'TB (' || ROUND(100*(total_table_size::float/total_storage::float),1) || '%)' AS total_table_storage,
    num_unscanned_tables || ' unscanned tables @ ' || size_unscanned_tables || 'TB / ' || total_storage || 'TB (' || ROUND(100*(size_unscanned_tables::float/total_storage::float),1) || '%)' AS unscanned_table_storage
FROM
    scan_aggs;

