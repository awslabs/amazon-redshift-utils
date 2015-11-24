/*

Table Skew Inspector. Please see http://docs.aws.amazon.com/redshift/latest/dg/c_analyzing-table-design.html
for more information.

Notes:


History:
2015-11-26 meyersi created

*/
DROP TABLE IF EXISTS temp_staging_tables_1;
DROP TABLE IF EXISTS temp_staging_tables_2;
DROP TABLE IF EXISTS temp_tables_report;

CREATE TEMP TABLE temp_staging_tables_1
                 (schemaname TEXT,
                  tablename TEXT,
                  tableid BIGINT,
                  size_in_megabytes BIGINT);

INSERT INTO temp_staging_tables_1
SELECT n.nspname, c.relname, c.oid, 
      (SELECT COUNT(*) FROM STV_BLOCKLIST b WHERE b.tbl = c.oid)
FROM pg_namespace n, pg_class c
WHERE n.oid = c.relnamespace 
  AND nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
  AND c.relname <> 'temp_staging_tables_1';

CREATE TEMP TABLE temp_staging_tables_2
                 (tableid BIGINT,
                  min_blocks_per_slice BIGINT,
                  max_blocks_per_slice BIGINT,
                  slice_count BIGINT);

INSERT INTO temp_staging_tables_2
SELECT tableid, MIN(c), MAX(c), COUNT(DISTINCT slice)
FROM (SELECT t.tableid, slice, COUNT(*) AS c
      FROM temp_staging_tables_1 t, STV_BLOCKLIST b
      WHERE t.tableid = b.tbl
      GROUP BY t.tableid, slice)
GROUP BY tableid;

CREATE TEMP TABLE temp_tables_report
                 (schemaname TEXT,
                 tablename TEXT,
                 tableid BIGINT,
                 size_in_mb BIGINT,
                 has_dist_key INT,
                 has_sort_key INT,
                 has_col_encoding INT,
                 pct_skew_across_slices FLOAT,
                 pct_slices_populated FLOAT);

INSERT INTO temp_tables_report
SELECT t1.*,
       CASE WHEN EXISTS (SELECT *
                         FROM pg_attribute a
                         WHERE t1.tableid = a.attrelid
                           AND a.attnum > 0
                           AND NOT a.attisdropped
                           AND a.attisdistkey = 't')
            THEN 1 ELSE 0 END,
       CASE WHEN EXISTS (SELECT *
                         FROM pg_attribute a
                         WHERE t1.tableid = a.attrelid
                           AND a.attnum > 0
                           AND NOT a.attisdropped
                           AND a.attsortkeyord > 0)
           THEN 1 ELSE 0 END,
      CASE WHEN EXISTS (SELECT *
                        FROM pg_attribute a
                        WHERE t1.tableid = a.attrelid
                          AND a.attnum > 0
                          AND NOT a.attisdropped
                          AND a.attencodingtype <> 0)
            THEN 1 ELSE 0 END,
      100 * CAST(t2.max_blocks_per_slice - t2.min_blocks_per_slice AS FLOAT)
            / CASE WHEN (t2.min_blocks_per_slice = 0) 
                   THEN 1 ELSE t2.min_blocks_per_slice END,
      CAST(100 * t2.slice_count AS FLOAT) / (SELECT COUNT(*) FROM STV_SLICES)
FROM temp_staging_tables_1 t1, temp_staging_tables_2 t2
WHERE t1.tableid = t2.tableid;

SELECT * FROM temp_tables_report
ORDER BY schemaname, tablename;