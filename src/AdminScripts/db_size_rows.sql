/* Query showing the current schema and size of objects in the current database */
SELECT trim(pgn.nspname) AS SCHEMA
       ,trim(a.name) AS TABLE
       ,id AS TableId
       ,decode(pgc.reldiststyle,
             0, 'EVEN',
             1,det.distkey ,
             8,'ALL'
       ) AS DistKey
       ,dist_ratio.ratio::decimal(10,4) AS Skew
       ,det.head_sort AS "SortKey"
       ,det.n_sortkeys AS "#SKs"
       ,b.mbytes
       ,decode(b.mbytes,
             0,0,
             ((b.mbytes/part.total::decimal)*100)::decimal(5,2)
       ) AS pct_of_total
       ,decode(det.max_enc,
             0,'N',
             'Y'
       ) AS Enc
       ,a.rows
       ,decode( det.n_sortkeys,
             0, NULL ,
             a.unsorted_rows
       ) AS unsorted_rows
       ,decode( det.n_sortkeys,
             0, NULL ,
             decode( a.rows,0,0, (a.unsorted_rows::decimal(32)/a.rows)*100)
       ) ::decimal(5,2) AS pct_unsorted
FROM (SELECT db_id
             ,id
             ,name
             ,SUM(ROWS) AS ROWS
             ,SUM(ROWS) - SUM(sorted_rows) AS unsorted_rows
      FROM stv_tbl_perm a
      GROUP BY db_id
               ,id
               ,name) AS a
  JOIN pg_class AS pgc ON pgc.oid = a.id
  JOIN pg_namespace AS pgn ON pgn.oid = pgc.relnamespace
  left outer join (SELECT tbl,COUNT (*) AS mbytes FROM stv_blocklist
GROUP BY tbl) b ON a.id = b.tbl INNER JOIN (SELECT attrelid
                                                   ,MIN(CASE attisdistkey WHEN 't' THEN attname ELSE NULL END) AS "distkey"
                                                   ,MIN(CASE attsortkeyord WHEN 1 THEN attname ELSE NULL END) AS head_sort
                                                   ,MAX(attsortkeyord) AS n_sortkeys
                                                   ,MAX(attencodingtype) AS max_enc
                                            FROM pg_attribute
                                            GROUP BY 1)
AS det ON det.attrelid = a.id INNER JOIN (SELECT tbl
                                                 ,MAX(Mbytes)::DECIMAL(32) /MIN(Mbytes) AS ratio
                                          FROM (SELECT tbl
                                                       ,TRIM(name) AS name
                                                       ,slice
                                                       ,COUNT(*) AS Mbytes
                                                FROM svv_diskusage
                                                GROUP BY tbl
                                                         ,name
                                                         ,slice)
                                          GROUP BY tbl
                                                   ,name)
AS dist_ratio ON a.id = dist_ratio.tbl JOIN (SELECT SUM(capacity) AS total
                                             FROM stv_partitions
                                             WHERE part_begin = 0)
AS part ON 1 = 1 WHERE mbytes IS NOT NULL
-- and pgn.nspname = ‘myschema' -- and a.name like '%user_event%' order by  mbytes desc
