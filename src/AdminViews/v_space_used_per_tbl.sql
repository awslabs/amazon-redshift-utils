--DROP VIEW admin.v_space_used_per_tbl;
/**********************************************************************************************
Purpose: View to get pull space used per table
History:
2014-01-30 jjschmit Created
2014-02-18 jjschmit Removed hardcoded where clause against 'public' schema
2014-02-21 jjschmit Added pct_unsorted and recommendation fields
2015-03-31 tinkerbotfoo Handled a special case to avoid divide by zero for pct_unsorted
2018-08-10 alexlsts Changed column "tablename" to use "relname" column from pg_class  
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_space_used_per_tbl
AS with info_table as ( SELECT TRIM(pgdb.datname) AS dbase_name
        ,TRIM(pgn.nspname) as schemaname
        ,TRIM(pgc.relname) AS tablename
        ,id AS tbl_oid
        ,b.mbytes AS megabytes
       ,CASE WHEN pgc.reldiststyle = 8
            THEN a.rows_all_dist
            ELSE a.rows END AS rowcount
       ,CASE WHEN pgc.reldiststyle = 8
            THEN a.unsorted_rows_all_dist
            ELSE a.unsorted_rows END AS unsorted_rowcount
       ,CASE WHEN pgc.reldiststyle = 8
          THEN decode( det.n_sortkeys,0, NULL,DECODE( a.rows_all_dist,0,0, (a.unsorted_rows_all_dist::DECIMAL(32)/a.rows_all_dist)*100))::DECIMAL(20,2)
          ELSE decode( det.n_sortkeys,0, NULL,DECODE( a.rows,0,0, (a.unsorted_rows::DECIMAL(32)/a.rows)*100))::DECIMAL(20,2) END
        AS pct_unsorted
FROM ( SELECT
              db_id
              ,id
              ,name
             ,MAX(ROWS) AS rows_all_dist
             ,MAX(ROWS) - MAX(sorted_rows) AS unsorted_rows_all_dist
              ,SUM(rows) AS rows
              ,SUM(rows)-SUM(sorted_rows) AS unsorted_rows
FROM stv_tbl_perm
GROUP BY db_id, id, name
       ) AS a
INNER JOIN
       pg_class AS pgc
ON pgc.oid = a.id
INNER JOIN
       pg_namespace AS pgn
ON pgn.oid = pgc.relnamespace
INNER JOIN
       pg_database AS pgdb
ON pgdb.oid = a.db_id
INNER JOIN (SELECT attrelid,
                     MIN(CASE attisdistkey WHEN 't' THEN attname ELSE NULL END) AS "distkey",
                     MIN(CASE attsortkeyord WHEN 1 THEN attname ELSE NULL END) AS head_sort,
                     MAX(attsortkeyord) AS n_sortkeys,
                     MAX(attencodingtype) AS max_enc,
                     SUM(case when attencodingtype <> 0 then 1 else 0 end)::DECIMAL(20,3)/COUNT(attencodingtype)::DECIMAL(20,3)  *100.00 as pct_enc
              FROM pg_attribute
              GROUP BY 1) AS det ON det.attrelid = a.id
LEFT OUTER JOIN
       ( SELECT
              tbl
              ,COUNT(*) AS mbytes
FROM stv_blocklist
GROUP BY tbl
       ) AS b
ON a.id=b.tbl
WHERE pgc.relowner > 1)
select info.*
    ,CASE WHEN info.rowcount = 0 THEN 'n/a'
        WHEN info.pct_unsorted  >= 20 THEN 'VACUUM SORT recommended'
        ELSE 'n/a'
    END AS recommendation
    from info_table info;
