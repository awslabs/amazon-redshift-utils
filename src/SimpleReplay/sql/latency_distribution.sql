WITH queries AS
(
  SELECT q.query,
         (datediff(ms,q.starttime,q.endtime)::DECIMAL- nvl(compile_time_s,0.00)) / 1000.00 AS total_elapsed_s
  FROM stl_query q
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
                        4) cp
           ON cp.query = q.query
          AND cp.xid = q.xid
  WHERE q.userid > 1
  AND   q.starttime >= {{START_TIME}}
  AND   q.starttime <= {{END_TIME}}
  AND   q.querytxt LIKE '%replay_start%'
  AND   aborted = 0
)
--select count(*) from queries
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
       COUNT(query)
FROM buckets
  LEFT JOIN queries
         ON total_elapsed_s >= sec_start
        AND total_elapsed_s < sec_end
GROUP BY 1,
         2,
         3
ORDER BY 2;
