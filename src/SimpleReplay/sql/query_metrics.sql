SELECT q.userid,
       TRIM(u.usename) AS usename,
       nvl(queue,'Result Cache') as queue,
       w.service_class,
       date_trunc('hour',service_class_start_time) AS "period",
       q.xid,
       q.query,
       querytxt::CHAR(50),
       total_queue_time / 1000000.00 queue_s,
       ((total_exec_time / 1000.00) - nvl(compile_time_s,0.00)) / 1000.00 AS exec_only_s,
       q.aborted,
       compile_time_s / 1000.00 AS compile_S,
       datediff(sec,startqueue,startwork) AS commit_queue_s,
       datediff(sec,startwork,cm.endtime) AS commit_s,
       (datediff(ms,q.starttime,q.endtime)::DECIMAL- nvl(compile_time_s,0.00)) / 1000.00 AS total_elapsed_s
FROM stl_query q
  left outer  JOIN (SELECT w.*,
                           TRIM(s.name) AS queue
                    FROM stl_wlm_query w
                      INNER JOIN STV_WLM_SERVICE_CLASS_CONFIG s ON w.service_class = s.service_class) w ON q.query = w.query
--inner join stl_internal_query_details i on w.query =i.query

  LEFT JOIN (SELECT c.xid,
                    c.userid,
                    c.pid,
                    c.query,
                    SUM(datediff (ms,starttime,endtime)) / nvl(CASE WHEN COUNT(DISTINCT service_class) = 0 THEN 1 ELSE COUNT(DISTINCT service_class) END,1) compile_time_s
             FROM svl_compile c
               LEFT OUTER JOIN stl_wlm_query w ON c.query = w.query
             GROUP BY 1,
                      2,
                      3,
                      4) cp ON cp.query = q.query
  LEFT JOIN (SELECT * FROM stl_commit_stats WHERE node = -1) cm ON cm.xid = q.xid
  LEFT JOIN pg_user u ON u.usesysid = q.userid
WHERE q.userid > 1
  -- and spectrum_tables_accessed=0
  AND   q.starttime >={{START_TIME}}
  AND   q.starttime <={{END_TIME}}
  AND   q.querytxt LIKE '%replay_start%'
  AND   aborted = 0;