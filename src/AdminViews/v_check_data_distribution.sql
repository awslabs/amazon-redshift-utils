--DROP VIEW admin.v_check_data_distribution;
/**********************************************************************************************
Purpose: View to get data distribution across slices
History:
2014-01-30 jjschmit Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_check_data_distribution
AS
SELECT slice,
       pgn.oid AS schema_oid,
       pgn.nspname AS schemaname,
       id AS tbl_oid,
       name AS tablename,
       ROWS AS rowcount_on_slice,
       SUM(ROWS) OVER (PARTITION BY name,id) AS total_rowcount,
       CASE
         WHEN ROWS IS NULL OR ROWS = 0 THEN 0
         ELSE ROUND(CAST(ROWS AS FLOAT) / CAST((SUM(ROWS) OVER (PARTITION BY id)) AS FLOAT)*100,3)
       END AS distrib_pct,
       CASE
         WHEN ROWS IS NULL OR ROWS = 0 THEN 0
         ELSE ROUND(CAST((MIN(ROWS) OVER (PARTITION BY id)) AS FLOAT) / CAST((SUM(ROWS) OVER (PARTITION BY id)) AS FLOAT)*100,3)
       END AS min_distrib_pct,
       CASE
         WHEN ROWS IS NULL OR ROWS = 0 THEN 0
         ELSE ROUND(CAST((MAX(ROWS) OVER (PARTITION BY id)) AS FLOAT) / CAST((SUM(ROWS) OVER (PARTITION BY id)) AS FLOAT)*100,3)
       END AS max_distrib_pct
FROM stv_tbl_perm AS perm
  INNER JOIN pg_class AS pgc ON pgc.oid = perm.id
  INNER JOIN pg_namespace AS pgn ON pgn.oid = pgc.relnamespace
WHERE slice < 3201
