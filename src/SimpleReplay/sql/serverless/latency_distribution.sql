WITH queries AS
(
  SELECT q.query_id
        ,q.elapsed_time / 1000000.00 as total_elapsed_s
  FROM sys_query_history q
  WHERE q.user_id > 1
  AND   q.start_time >= {{START_TIME}}
  AND   q.start_time <= {{END_TIME}}
  AND   q.query_text LIKE '%replay_start%'
  AND   status != 'failed'
)
,
pct AS
(
  SELECT ROUND(PERCENTILE_CONT (0.98) WITHIN GROUP(ORDER BY q1.total_elapsed_s),2) AS p98_s,
         COUNT(*) AS query_count,
         MAX(q1.total_elapsed_s) max_s,
         MIN(q1.total_elapsed_s) min_s,
         MIN(CASE WHEN q1.total_elapsed_s = 0.00 THEN NULL ELSE q1.total_elapsed_s END) min_2s
  FROM queries q1
),
bucket_count AS
(
  SELECT CASE
           WHEN query_count > 100 THEN 40
           ELSE 5
         END AS b_count
  FROM pct
),
buckets AS
(
  SELECT (min_2s +((n)*(p98_s / b_count))) AS sec_end,
         n,
         (min_2s +((n -1)*(p98_s / b_count))) AS sec_start
  FROM (SELECT ROW_NUMBER() OVER () n FROM pg_class LIMIT 39),
       bucket_count,
       pct
  WHERE sec_end <= p98_s
  UNION ALL
  SELECT min_2s AS sec_end,
         0 AS n,
         0.00 AS sec_start
  FROM pct
  UNION ALL
  SELECT (max_s +0.01) AS sec_end,
         b_count AS n,
         p98_s AS sec_start
  FROM pct,
       bucket_count
)
SELECT sec_end,
       n,
       sec_start,
       COUNT(query_id)
FROM buckets
  LEFT JOIN queries
         ON total_elapsed_s >= sec_start
        AND total_elapsed_s < sec_end
GROUP BY 1,
         2,
         3
ORDER BY 2;