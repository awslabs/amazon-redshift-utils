WITH queries AS
(
  select
  q.user_id as "userid"
  ,date_trunc('hour', q.start_time) as "period"
  ,q.transaction_id as "xid"
  ,q.query_id as "query"
  ,q.query_text::char(50) as "querytxt"
  ,q.queue_time / 1000000.00 as "queue_s"
  ,q.execution_time / 1000000.00 as "exec_time_s" -- This includes compile time. Differs in behavior from provisioned metric
  ,case when q.status = 'failed' then 1 else 0 end "aborted"
  ,q.elapsed_time / 1000000.00 as "total_elapsed_s" -- This includes compile time. Differs in behavior from provisioned metric
  FROM sys_query_history q
  WHERE q.user_id > 1
  AND   q.start_time >= {{START_TIME}}
  AND   q.start_time <= {{END_TIME}}
  AND   q.query_text LIKE '%replay_start%'
  AND   q.status != 'failed'
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
         ROUND(PERCENTILE_CONT (0.25) WITHIN GROUP(ORDER BY exec_time_s),2) AS p25_s,
         ROUND(PERCENTILE_CONT (0.50) WITHIN GROUP(ORDER BY exec_time_s),2) AS p50_s,
         ROUND(PERCENTILE_CONT (0.75) WITHIN GROUP(ORDER BY exec_time_s),2) AS p75_s,
         ROUND(PERCENTILE_CONT (0.90) WITHIN GROUP(ORDER BY exec_time_s),2) AS p90_s,
         ROUND(PERCENTILE_CONT (0.95) WITHIN GROUP(ORDER BY exec_time_s),2) AS p95_s,
         ROUND(PERCENTILE_CONT (0.99) WITHIN GROUP(ORDER BY exec_time_s),2) AS p99_s,
         MAX(exec_time_s) AS max_s,
         AVG(exec_time_s) AS avg_s,
         stddev(exec_time_s) AS std_s
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
ORDER BY 1;