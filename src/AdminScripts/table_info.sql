/**********************************************************************************************
Purpose: Return Table storage information (size, skew, etc)
Columns:
schema:		Schema name
Table:		Table name
id:		Table id
DistKey:	Distribution Key (shows EVEN for event disttributed, ALL for Diststyle ALL)
Skew:		Table Skew. Proportion between largest slice and smallest slice (null for diststyle ALL)
Sortkey:	First column of Sortkey
#SKs:		Number of columns in the compound sortkey
rows:		Number of rows
mbytes:		Size of the table in Megabytes
Enc:		Y if the table has at least one compressed column, N otherwise
pct_enc:        Proportion of number of encoded columns to total number of columns
pct_of_total:	Size of the table in proportion to the cluster size
pct_stats_off:	Measure of staleness of table statistics (real size versus size recorded in stats)
pct_unsorted:	Proportion of unsorted rows compared to total rows
Notes:

History:
2015-02-16 ericfe created
2017-03-23 thiyagu Added percentage encoded column metric (pct_enc) and fixes  
**********************************************************************************************/

SELECT TRIM(pgn.nspname) AS SCHEMA,
       TRIM(a.name) AS TABLE,
       id AS TableId,
       decode(pgc.reldiststyle,
             0, 'EVEN',
             1,det.distkey ,
             8,'ALL'
       ) AS DistKey,
       decode(pgc.reldiststyle,
             8,NULL,
             dist_ratio.ratio::DECIMAL(20,4)
       ) AS Skew,
       det.head_sort AS "SortKey",
       det.n_sortkeys AS "#SKs",
       a.rows,
       b.mbytes,
       decode(det.max_enc,
             0,'N',
             'Y'
       ) AS Enc,
       det.pct_enc,
       decode(b.mbytes,
             0,0,
             ((b.mbytes/part.total::DECIMAL)*100)::DECIMAL(20,2)
       ) AS pct_of_total,
       (CASE WHEN a.rows = 0 THEN NULL ELSE ((a.rows - pgc.reltuples)::DECIMAL(20,3) / a.rows::DECIMAL(20,3)*100)::DECIMAL(20,2) END) AS pct_stats_off,
       decode( det.n_sortkeys,
              0, NULL ,
              DECODE( a.rows,0,0, (a.unsorted_rows::DECIMAL(32)/a.rows)*100) 
       ) ::DECIMAL(20,2) AS pct_unsorted
FROM (SELECT db_id,
             id,
             name,
             SUM(ROWS) AS ROWS,
             SUM(ROWS) - SUM(sorted_rows) AS unsorted_rows
      FROM stv_tbl_perm a
      GROUP BY db_id,
               id,
               name) AS a
  JOIN pg_class AS pgc ON pgc.oid = a.id
  JOIN pg_namespace AS pgn ON pgn.oid = pgc.relnamespace
  LEFT OUTER JOIN (SELECT tbl, COUNT(*) AS mbytes FROM stv_blocklist GROUP BY tbl) b ON a.id = b.tbl
  INNER JOIN (SELECT attrelid,
                     MIN(CASE attisdistkey WHEN 't' THEN attname ELSE NULL END) AS "distkey",
                     MIN(CASE attsortkeyord WHEN 1 THEN attname ELSE NULL END) AS head_sort,
                     MAX(attsortkeyord) AS n_sortkeys,
                     MAX(attencodingtype) AS max_enc,
                     SUM(case when attencodingtype <> 0 then 1 else 0 end)::DECIMAL(20,3)/COUNT(attencodingtype)::DECIMAL(20,3)  *100.00 as pct_enc
              FROM pg_attribute
              GROUP BY 1) AS det ON det.attrelid = a.id
  INNER JOIN (SELECT tbl,
                     MAX(Mbytes)::DECIMAL(32) /MIN(Mbytes) AS ratio
              FROM (SELECT tbl,
                           TRIM(name) AS name,
                           slice,
                           COUNT(*) AS Mbytes
                    FROM svv_diskusage
                    GROUP BY tbl,
                             name,
                             slice)
              GROUP BY tbl,
                       name) AS dist_ratio ON a.id = dist_ratio.tbl
  JOIN (SELECT SUM(capacity) AS total
        FROM stv_partitions
        WHERE part_begin = 0) AS part ON 1 = 1
WHERE mbytes IS NOT NULL
AND   pgc.relowner > 1
-- and pgn.nspname = 'schema' -- schemaname
-- and a.name like 'table%' -- tablename
-- and det.max_enc = 0 -- non-compressed tables
ORDER BY mbytes DESC;
