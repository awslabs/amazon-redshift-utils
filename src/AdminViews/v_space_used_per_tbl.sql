--DROP VIEW admin.v_space_used_per_tbl;
/**********************************************************************************************
Purpose: View to get pull space used per table
History:
2014-01-30 jjschmit Created
2014-02-18 jjschmit Removed hardcoded where clause against 'public' schema
2014-02-21 jjschmit Added pct_unsorted and recommendation fields
2015-03-31 tinkerbotfoo Handled a special case to avoid divide by zero for pct_unsorted 
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_space_used_per_tbl
AS
SELECT
		TRIM(pgdb.datname) AS dbase_name
		,TRIM(pgn.nspname) as schemaname
		,TRIM(a.name) AS tablename
		,id AS tbl_oid
		,b.mbytes AS megabytes
		,a.rows AS rowcount
		,a.unsorted_rows AS unsorted_rowcount
		,CASE WHEN a.rows = 0 then 0
			ELSE ROUND((a.unsorted_rows::FLOAT / a.rows::FLOAT) * 100, 5)
		END AS pct_unsorted
		,CASE WHEN a.rows = 0 THEN 'n/a'
			WHEN (a.unsorted_rows::FLOAT / a.rows::FLOAT) * 100 >= 20 THEN 'VACUUM SORT recommended'
			ELSE 'n/a'
		END AS recommendation
FROM
       (
       SELECT
              db_id
              ,id
              ,name
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
LEFT OUTER JOIN
       (
       SELECT
              tbl
              ,COUNT(*) AS mbytes 
       FROM stv_blocklist 
       GROUP BY tbl
       ) AS b 
              ON a.id=b.tbl
WHERE pgc.relowner > 1
ORDER BY 1,3,2;
