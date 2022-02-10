WITH queries AS
(
  SELECT q.userid,
         -- TRIM(u.usename) AS usename,
         -- TRIM(s.name) AS queue,
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
    LEFT OUTER JOIN stl_wlm_query w ON q.query = w.query
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
  WHERE q.userid > 1
  -- and spectrum_tables_accessed=0
  AND   q.starttime >= {{START_TIME}}
  AND   q.starttime <= {{END_TIME}}
  AND   q.querytxt LIKE '%replay_start%'
  AND   aborted = 0
),
elapsed_time AS
(
  SELECT 'Query Latency' AS measure_type,
         COUNT(*) AS query_count,
         ROUND(PERCENTILE_CONT (0.25) WITHIN GROUP(ORDER BY total_elapsed_s),2) AS p25_s,
         ROUND(PERCENTILE_CONT (0.50) WITHIN GROUP(ORDER BY total_elapsed_s),2) AS p50_s,
         ROUND(PERCENTILE_CONT (0.75) WITHIN GROUP(ORDER BY total_elapsed_s),2) AS p75_s,
         ROUND(PERCENTILE_CONT (0.90) WITHIN GROUP(ORDER BY total_elapsed_s),2) AS p90_s,
         ROUND(PERCENTILE_CONT (0.95) WITHIN GROUP(ORDER BY total_elapsed_s),2) AS p95_s,
         ROUND(PERCENTILE_CONT (0.99) WITHIN GROUP(ORDER BY total_elapsed_s),2) AS p99_s,
         MAX(total_elapsed_s) AS max_s,
         AVG(total_elapsed_s) AS avg_s,
         stddev(total_elapsed_s) AS std_s
  FROM queries
  GROUP BY 1
),
exec_time AS
(
  SELECT 'Execution Time' AS measure_type,
         COUNT(*) AS query_count,
         ROUND(PERCENTILE_CONT (0.25) WITHIN GROUP(ORDER BY exec_only_s),2) AS p25_s,
         ROUND(PERCENTILE_CONT (0.50) WITHIN GROUP(ORDER BY exec_only_s),2) AS p50_s,
         ROUND(PERCENTILE_CONT (0.75) WITHIN GROUP(ORDER BY exec_only_s),2) AS p75_s,
         ROUND(PERCENTILE_CONT (0.90) WITHIN GROUP(ORDER BY exec_only_s),2) AS p90_s,
         ROUND(PERCENTILE_CONT (0.95) WITHIN GROUP(ORDER BY exec_only_s),2) AS p95_s,
         ROUND(PERCENTILE_CONT (0.99) WITHIN GROUP(ORDER BY exec_only_s),2) AS p99_s,
         MAX(exec_only_s) AS max_s,
         AVG(exec_only_s) AS avg_s,
         stddev(exec_only_s) AS std_s
  FROM queries
  GROUP BY 1
),
queue_time AS
(
  SELECT 'Queue Time' AS measure_type,
         COUNT(*) AS query_count,
         ROUND(PERCENTILE_CONT (0.25) WITHIN GROUP(ORDER BY queue_s),2) AS p25_s,
         ROUND(PERCENTILE_CONT (0.50) WITHIN GROUP(ORDER BY queue_s),2) AS p50_s,
         ROUND(PERCENTILE_CONT (0.75) WITHIN GROUP(ORDER BY queue_s),2) AS p75_s,
         ROUND(PERCENTILE_CONT (0.90) WITHIN GROUP(ORDER BY queue_s),2) AS p90_s,
         ROUND(PERCENTILE_CONT (0.95) WITHIN GROUP(ORDER BY queue_s),2) AS p95_s,
         ROUND(PERCENTILE_CONT (0.99) WITHIN GROUP(ORDER BY queue_s),2) AS p99_s,
         MAX(queue_s) AS max_s,
         AVG(queue_s) AS avg_s,
         stddev(queue_s) AS std_s
  FROM queries
  GROUP BY 1
),
compile_time AS
(
  SELECT 'Compile Time' AS measure_type,
         COUNT(*) AS query_count,
         ROUND(PERCENTILE_CONT (0.25) WITHIN GROUP(ORDER BY compile_s),2) AS p25_s,
         ROUND(PERCENTILE_CONT (0.50) WITHIN GROUP(ORDER BY compile_s),2) AS p50_s,
         ROUND(PERCENTILE_CONT (0.75) WITHIN GROUP(ORDER BY compile_s),2) AS p75_s,
         ROUND(PERCENTILE_CONT (0.90) WITHIN GROUP(ORDER BY compile_s),2) AS p90_s,
         ROUND(PERCENTILE_CONT (0.95) WITHIN GROUP(ORDER BY compile_s),2) AS p95_s,
         ROUND(PERCENTILE_CONT (0.99) WITHIN GROUP(ORDER BY compile_s),2) AS p99_s,
         MAX(compile_s) AS max_s,
         AVG(compile_s) AS avg_s,
         stddev(compile_s) AS std_s
  FROM queries
  GROUP BY 1
),
commit_q_time AS
(
  SELECT 'Commit Queue Time' AS measure_type,
         COUNT(*) AS query_count,
         ROUND(PERCENTILE_CONT (0.25) WITHIN GROUP(ORDER BY commit_queue_s),2) AS p25_s,
         ROUND(PERCENTILE_CONT (0.50) WITHIN GROUP(ORDER BY commit_queue_s),2) AS p50_s,
         ROUND(PERCENTILE_CONT (0.75) WITHIN GROUP(ORDER BY commit_queue_s),2) AS p75_s,
         ROUND(PERCENTILE_CONT (0.90) WITHIN GROUP(ORDER BY commit_queue_s),2) AS p90_s,
         ROUND(PERCENTILE_CONT (0.95) WITHIN GROUP(ORDER BY commit_queue_s),2) AS p95_s,
         ROUND(PERCENTILE_CONT (0.99) WITHIN GROUP(ORDER BY commit_queue_s),2) AS p99_s,
         MAX(commit_queue_s) AS max_s,
         AVG(commit_queue_s) AS avg_s,
         stddev(commit_queue_s) AS std_s
  FROM queries
  GROUP BY 1
),
commit_time AS
(
  SELECT 'Commit Time' AS measure_type,
         COUNT(*) AS query_count,
         ROUND(PERCENTILE_CONT (0.25) WITHIN GROUP(ORDER BY commit_s),2) AS p25_s,
         ROUND(PERCENTILE_CONT (0.50) WITHIN GROUP(ORDER BY commit_s),2) AS p50_s,
         ROUND(PERCENTILE_CONT (0.75) WITHIN GROUP(ORDER BY commit_s),2) AS p75_s,
         ROUND(PERCENTILE_CONT (0.90) WITHIN GROUP(ORDER BY commit_s),2) AS p90_s,
         ROUND(PERCENTILE_CONT (0.95) WITHIN GROUP(ORDER BY commit_s),2) AS p95_s,
         ROUND(PERCENTILE_CONT (0.99) WITHIN GROUP(ORDER BY commit_s),2) AS p99_s,
         MAX(commit_s) AS max_s,
         AVG(commit_s) AS avg_s,
         stddev(commit_s) AS std_s
  FROM queries
  GROUP BY 1
)
SELECT measure_type,
       query_count,
       p25_s,
       p50_s,
       p75_s,
       p90_s,
       p95_s,
       p99_s,
       max_s,
       avg_s,
       std_s
FROM exec_time
UNION ALL
SELECT measure_type,
       query_count,
       p25_s,
       p50_s,
       p75_s,
       p90_s,
       p95_s,
       p99_s,
       max_s,
       avg_s,
       std_s
FROM queue_time
UNION ALL
SELECT measure_type,
       query_count,
       p25_s,
       p50_s,
       p75_s,
       p90_s,
       p95_s,
       p99_s,
       max_s,
       avg_s,
       std_s
FROM elapsed_time
UNION ALL
SELECT measure_type,
       query_count,
       p25_s,
       p50_s,
       p75_s,
       p90_s,
       p95_s,
       p99_s,
       max_s,
       avg_s,
       std_s
FROM compile_time
UNION ALL
SELECT measure_type,
       query_count,
       p25_s,
       p50_s,
       p75_s,
       p90_s,
       p95_s,
       p99_s,
       max_s,
       avg_s,
       std_s
FROM commit_q_time
UNION ALL
SELECT measure_type,
       query_count,
       p25_s,
       p50_s,
       p75_s,
       p90_s,
       p95_s,
       p99_s,
       max_s,
       avg_s,
       std_s
FROM commit_time
ORDER BY 1;

