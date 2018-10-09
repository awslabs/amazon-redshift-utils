--DROP VIEW admin.v_get_tbl_scan_frequency;
/**********************************************************************************************
Purpose: View to identify how frequently queries scan database tables.
History:
2016-03-14 chriz-bigdata Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_get_tbl_scan_frequency
AS
SELECT 
	database, 
	schema AS schemaname, 
	table_id, 
	"table" AS tablename, 
	size, 
	sortkey1, 
	NVL(s.num_qs,0) num_qs
FROM svv_table_info t
LEFT JOIN (SELECT
   tbl, perm_table_name,
   COUNT(DISTINCT query) num_qs
FROM
   stl_scan s
WHERE 
   s.userid > 1
   AND s.perm_table_name NOT IN ('Internal Worktable','S3')
GROUP BY 
   tbl, perm_table_name) s ON s.tbl = t.table_id
AND t."schema" NOT IN ('pg_internal')
ORDER BY 7 desc;
