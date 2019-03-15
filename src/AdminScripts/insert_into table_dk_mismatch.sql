/**********************************************************************************************
Purpose: Return Insert as select distribution key mismatch
Columns:

  Target:			Target Table
  Target_dk:		DK of Target Table
  Source:			Source Table
  Source_dk:		DK of Source Table

Notes:

History:
2015-02-16 ericfe created
**********************************************************************************************/
SELECT DISTINCT trim(pgn.nspname) || '.' ||trim(pgc.relname) AS Target,
       tt.distkey AS target_dk,
       trim(pgn2.nspname) || '.' ||trim(pgc2.relname) AS Source,
       ts.distkey AS source_dk
FROM stl_insert i
  JOIN stl_scan s ON i.query = s.query
  JOIN pg_class AS pgc ON pgc.oid = i.tbl
  JOIN pg_namespace AS pgn ON pgn.oid = pgc.relnamespace
  JOIN pg_class AS pgc2 ON pgc2.oid = s.tbl
  JOIN pg_namespace AS pgn2 ON pgn2.oid = pgc2.relnamespace
  LEFT JOIN (SELECT attrelid,
                    MIN(CASE attisdistkey WHEN 't' THEN attname ELSE NULL END) AS "distkey",
                    MIN(CASE attsortkeyord WHEN 1 THEN attname WHEN -1 THEN 'INTERLEAVED' ELSE NULL END) AS head_sort,
                    MAX(attsortkeyord) AS n_sortkeys,
                    MAX(attencodingtype) AS max_enc
             FROM pg_attribute
             GROUP BY 1) AS tt ON tt.attrelid = i.tbl
  LEFT JOIN (SELECT attrelid,
                    MIN(CASE attisdistkey WHEN 't' THEN attname ELSE NULL END) AS "distkey",
                    MIN(CASE attsortkeyord WHEN 1 THEN attname WHEN -1 THEN 'INTERLEAVED' ELSE NULL END) AS head_sort,
                    MAX(attsortkeyord) AS n_sortkeys,
                    MAX(attencodingtype) AS max_enc
             FROM pg_attribute
             GROUP BY 1) AS ts ON ts.attrelid = s.tbl
WHERE i.tbl <> s.tbl
AND   s.perm_table_name <> 'Internal Worktable'
AND   i.slice = 0
AND   s.slice = 0
AND   (tt.distkey <> ts.distkey OR tt.distkey IS NULL OR ts.distkey IS NULL)
ORDER BY 1;