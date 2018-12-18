/**********************************************************************************************
Purpose: Return extended table information for permanent database tables.

Columns:
database: database name
table_id: table oid
tablename: Schema qualified table name
columns: encoded columns / total columns
pk: Y if PK constraint exists, otherwise N
fk: Y if FK constraint exists, otherwise N
max_varchar: Size of the largest column that uses a VARCHAR data type. 
diststyle: diststyle(distkey column): distribution skew ratio
sortkey: sortkey column(sortkey skew)
size: size in MB / minimum table size (percentage storage used)
tbl_rows: Total number of rows in the table
unsorted: Percent of rows in the unsorted space of the table
stats_off: Number that indicates how stale the table's statistics are; 0 is current, 100 is out of date.
scans:rr:filt:sel:del :
  scans: number of scans against the table
  rr: number of range restricted scans (scans which leverage the zone maps) against the table
  filt: number of scans against the table which leveraged filter criteria
  sel: number of scans against the table which triggered an alert for selective query filter
  del: number of scans against the table which triggered an alert for scanning number of deleted rows
last_scan: last time the table was scanned

Notes:
History:
2016-09-12 chriz-bigdata created
**********************************************************************************************/

CREATE OR REPLACE VIEW admin.v_extended_table_info AS
WITH tbl_ids AS
(
  SELECT DISTINCT oid
  FROM pg_class c
  WHERE relowner > 1
  AND   relkind = 'r'
),
scan_alerts AS
(
  SELECT s.tbl AS TABLE,
         Nvl(SUM(CASE WHEN TRIM(SPLIT_PART(l.event,':',1)) = 'Very selective query filter' THEN 1 ELSE 0 END),0) AS selective_scans,
         Nvl(SUM(CASE WHEN TRIM(SPLIT_PART(l.event,':',1)) = 'Scanned a large number of deleted rows' THEN 1 ELSE 0 END),0) AS delrows_scans
  FROM stl_alert_event_log AS l
    JOIN stl_scan AS s
      ON s.query = l.query
     AND s.slice = l.slice
     AND s.segment = l.segment
     AND s.step = l.step
  WHERE l.userid > 1
  AND   s.slice = 0
  AND   s.tbl IN (SELECT oid FROM tbl_ids)
  AND   l.event_time >= Dateadd (DAY,-7,CURRENT_DATE)
  AND   TRIM(SPLIT_PART(l.event,':',1)) IN ('Very selective query filter','Scanned a large number of deleted rows')
  GROUP BY 1
),
tbl_scans AS
(
  SELECT tbl,
         MAX(endtime) last_scan,
         Nvl(COUNT(DISTINCT query || LPAD(segment,3,'0')),0) num_scans
  FROM stl_scan s
  WHERE s.userid > 1
  AND   s.tbl IN (SELECT oid FROM tbl_ids)
  GROUP BY tbl
),
rr_scans AS
(
SELECT tbl,
NVL(SUM(CASE WHEN is_rrscan='t' THEN 1 ELSE 0 END),0) rr_scans,
NVL(SUM(CASE WHEN p.info like 'Filter:%' and p.nodeid > 0 THEN 1 ELSE 0 END),0) filtered_scans,
Nvl(COUNT(DISTINCT s.query || LPAD(s.segment,3,'0')),0) num_scans
  FROM stl_scan s
  JOIN stl_plan_info i on (s.userid=i.userid and s.query=i.query and s.segment=i.segment and s.step=i.step)
  JOIN stl_explain p on ( i.userid=p.userid and i.query=p.query and i.nodeid=p.nodeid  )
  WHERE s.userid > 1
  AND s.type = 2
  AND s.slice = 0
  AND   s.tbl IN (SELECT oid FROM tbl_ids)
  GROUP BY tbl
),
pcon AS
(
  SELECT conrelid,
         CASE
           WHEN SUM(
             CASE
               WHEN contype = 'p' THEN 1
               ELSE 0
             END 
           ) > 0 THEN 'Y'
           ELSE NULL
         END pk,
         CASE
           WHEN SUM(
             CASE
               WHEN contype = 'f' THEN 1
               ELSE 0
             END 
           ) > 0 THEN 'Y'
           ELSE NULL
         END fk
  FROM pg_constraint
  WHERE conrelid > 0
  AND   conrelid IN (SELECT oid FROM tbl_ids)
  GROUP BY conrelid
),
colenc AS
(
  SELECT attrelid,
         SUM(CASE WHEN a.attencodingtype = 0 THEN 0 ELSE 1 END) AS encoded_cols,
         COUNT(*) AS cols
  FROM pg_attribute a
  WHERE a.attrelid IN (SELECT oid FROM tbl_ids)
  AND   a.attnum > 0
  GROUP BY a.attrelid
),
stp AS
(
  SELECT id,
         SUM(ROWS) sum_r,
         SUM(sorted_rows) sum_sr,
         MIN(ROWS) min_r,
         MAX(ROWS) max_r,
         Nvl(COUNT(DISTINCT slice),0) pop_slices
  FROM stv_tbl_perm
  WHERE id IN (SELECT oid FROM tbl_ids)
  AND   slice < 6400
  GROUP BY id
),
cluster_info AS
(
  SELECT COUNT(DISTINCT node) node_count FROM stv_slices
)
SELECT ti.database,
       ti.table_id,
       ti.SCHEMA || '.' || ti."table" AS tablename,
       colenc.encoded_cols || '/' || colenc.cols AS "columns",
       pcon.pk,
       pcon.fk,
       ti.max_varchar,
       CASE
         WHEN ti.diststyle NOT IN ('EVEN','ALL') THEN ti.diststyle || ': ' || ti.skew_rows
         ELSE ti.diststyle
       END AS diststyle,
       CASE
         WHEN ti.sortkey1 IS NOT NULL AND ti.sortkey1_enc IS NOT NULL THEN ti.sortkey1 || '(' || nvl (skew_sortkey1,0) || ')'
         WHEN ti.sortkey1 IS NOT NULL THEN ti.sortkey1
         ELSE NULL
       END AS "sortkey",
       ti.size || '/' || CASE
         WHEN stp.sum_r = stp.sum_sr OR stp.sum_sr = 0 THEN
           CASE
             WHEN "diststyle" = 'EVEN' THEN (stp.pop_slices*(colenc.cols + 3))
             WHEN SUBSTRING("diststyle",1,3) = 'KEY' THEN (stp.pop_slices*(colenc.cols + 3))
             WHEN "diststyle" = 'ALL' THEN (cluster_info.node_count*(colenc.cols + 3))
           END 
         ELSE
           CASE
             WHEN "diststyle" = 'EVEN' THEN (stp.pop_slices*(colenc.cols + 3)*2)
             WHEN SUBSTRING("diststyle",1,3) = 'KEY' THEN (stp.pop_slices*(colenc.cols + 3)*2)
             WHEN "diststyle" = 'ALL' THEN (cluster_info.node_count*(colenc.cols + 3)*2)
           END 
         END|| ' (' || ti.pct_used || ')' AS size,
         ti.tbl_rows,
         ti.unsorted,
         ti.stats_off,
         Nvl(tbl_scans.num_scans,0) || ':' || Nvl(rr_scans.rr_scans,0) || ':' || Nvl(rr_scans.filtered_scans,0) || ':' || Nvl(scan_alerts.selective_scans,0) || ':' || Nvl(scan_alerts.delrows_scans,0) AS "scans:rr:filt:sel:del",tbl_scans.last_scan 
FROM svv_table_info ti 
LEFT JOIN colenc ON colenc.attrelid = ti.table_id 
LEFT JOIN stp ON stp.id = ti.table_id 
LEFT JOIN tbl_scans ON tbl_scans.tbl = ti.table_id 
LEFT JOIN rr_scans ON rr_scans.tbl = ti.table_id
LEFT JOIN pcon ON pcon.conrelid = ti.table_id 
LEFT JOIN scan_alerts ON scan_alerts.table = ti.table_id 
CROSS JOIN cluster_info 
WHERE ti.SCHEMA NOT IN ('pg_internal') 
ORDER BY ti.pct_used DESC;
