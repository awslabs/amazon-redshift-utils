--DROP VIEW admin.v_check_data_distribution;
/**********************************************************************************************
Purpose: View to get data distribution across slices
History:
2014-01-30 jjschmit Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_check_data_distribution
AS
SELECT 
	slice
	,pgn.oid AS schema_oid
	,pgn.nspname AS schemaname
	,id AS tbl_oid
	,name AS tablename
	,stv.diststyle AS diststyle
  	,stv.sortkey1 AS sortkey
	,rows AS rowcount_on_slice
	,SUM(rows) OVER (PARTITION BY name, id) AS total_rowcount
	,CASE
		WHEN rows IS NULL OR rows = 0 THEN 0
		ELSE ROUND(CAST(rows AS FLOAT) / CAST((SUM(rows) OVER (PARTITION BY id)) AS FLOAT) * 100, 3) 
		END AS distrib_pct
	,CASE
		WHEN rows IS NULL OR rows = 0 THEN 0
		ELSE ROUND(CAST((MIN(rows) OVER (PARTITION BY id)) AS FLOAT) / CAST((SUM(rows) OVER (PARTITION BY id)) AS FLOAT) * 100, 3) 
		END AS min_distrib_pct
	,CASE
		WHEN rows IS NULL OR rows = 0 THEN 0
		ELSE ROUND(CAST((MAX(rows) OVER (PARTITION BY id)) AS FLOAT) / CAST((SUM(rows) OVER (PARTITION BY id)) AS FLOAT) * 100, 3) 
		END AS max_distrib_pct
FROM 
	stv_tbl_perm AS perm
INNER JOIN
	pg_class AS pgc 
		ON pgc.oid = perm.id
INNER JOIN
	pg_namespace AS pgn 
		ON pgn.oid = pgc.relnamespace
INNER JOIN
  svv_table_info AS stv
    ON stv.schema = pgn.nspname AND stv."table" = name
WHERE slice < 3201
AND pgc.relowner > 1
;
