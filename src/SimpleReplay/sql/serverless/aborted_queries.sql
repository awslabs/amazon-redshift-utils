SELECT 
q.user_id as "userid",
TRIM(u.usename) AS usename
,case when q.result_cache_hit = 't' then 'Result Cache' else 'Default queue' end as "queue"
,date_trunc('hour', q.start_time) as "period"
,q.transaction_id as "xid"
,q.query_id as "query"
,q.query_text::char(50) as "querytxt"
,q.queue_time / 1000000.00 as "queue_s"
,q.execution_time / 1000000.00 as "exec_time_s" -- This includes compile time. Differs in behavior from provisioned metric
,case when q.status = 'failed' then 1 else 0 end "aborted"
,q.elapsed_time / 1000000.00 as "total_elapsed_s" -- This includes compile time. Differs in behavior from provisioned metric
FROM sys_query_history q
  LEFT JOIN pg_user u ON u.usesysid = q.user_id
WHERE q.user_id > 1
  AND   q.start_time >={{START_TIME}}
  AND   q.start_time <={{END_TIME}}
  AND   q.query_text LIKE '%replay_start%'
  AND   q.status = 'failed'
ORDER BY total_elapsed_s DESC;