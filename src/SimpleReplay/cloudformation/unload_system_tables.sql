--query_stats
UNLOAD
(
$$
WITH queries
     AS (SELECT query,
                Listagg(DISTINCT schemaname
                                 ||'.'
                                 ||table_name, ',')
                  within GROUP(ORDER BY table_name) tables_scanned
         FROM   (WITH scan_delete_insert
                      AS (SELECT 'scan'                query_type,
                                 query,
                                 Lpad(segment, 3, '0') segment,
                                 tbl
                          FROM   stl_scan
                          WHERE  userid > 1
                                 AND perm_table_name != 'Internal Worktable'
                                 AND tbl <> 0
                          UNION ALL
                          SELECT 'delete'              query_type,
                                 query,
                                 Lpad(segment, 3, '0') segment,
                                 tbl
                          FROM   stl_delete
                          WHERE  userid > 1
                                 AND tbl <> 0
                          UNION ALL
                          SELECT 'insert'              query_type,
                                 query,
                                 Lpad(segment, 3, '0') segment,
                                 tbl
                          FROM   stl_insert
                          WHERE  userid > 1
                                 AND tbl <> 0)
                 SELECT sdi.query_type,
                        sdi.query,
                        sdi.segment,
                        sdi.tbl,
                        Trim(n.nspname) AS schemaname,
                        Trim(c.relname) table_name
                  FROM   scan_delete_insert sdi
                         join pg_class c
                           ON c.oid = sdi.tbl
                         join pg_namespace n
                           ON n.oid = c.relnamespace)
         GROUP  BY query),
     compiles
     AS (SELECT query,
                SUM(Datediff (microsecond, q.starttime, q.endtime))
                total_compile_time
         FROM   svl_compile q
         WHERE  COMPILE = 1
         GROUP  BY query)
SELECT Trim(s.name)                                      queue,
       Trim(u.usename)                                   AS username,
       CASE
         WHEN q.concurrency_scaling_status = 1 THEN 1
         ELSE 0
       END                                               AS cc_scaling,
       q.aborted,
       w.total_queue_time,
       Nvl(ct.total_compile_time, 0)                     total_compile_time,
       w.total_exec_time - Nvl(ct.total_compile_time, 0) total_exec_time,
       Datediff(microsecond, q.starttime, q.endtime)     AS total_query_time,
       q.userid,
       q.query,
       q.label                                           query_label,
       q.xid,
       q.pid,
       w.service_class,
       q.starttime,
       q.endtime,
       tables_scanned,
       Trim(q.querytxt)                                  querytxt
FROM   stl_query q
       join stl_wlm_query w USING (userid, query)
       join pg_user u
         ON u.usesysid = q.userid
       join stv_wlm_service_class_config s
         ON w.service_class = s.service_class
       left outer join queries
                    ON queries.query = q.query
       left outer join compiles ct
                    ON w.query = ct.query
WHERE  q.userid > 1
       AND w.service_class > 5
$$)  TO '' CREDENTIALS '' FORMAT AS PARQUET ALLOWOVERWRITE;
