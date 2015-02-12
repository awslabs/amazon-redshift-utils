/* Query showing longest running queryes by runtime */
SELECT DATABASE
       ,COUNT(query) AS n_qry
       ,max(substring(qrytext,1,80)) AS qrytext
       ,min(run_minutes) AS "min"
       ,max(run_minutes) AS "max"
       ,avg(run_minutes) AS "avg"
       ,SUM(run_minutes) AS total
       ,max(query) AS max_query_id
       ,max(starttime)::DATE AS last_run
       ,SUM(alerts) AS alerts
       ,aborted
FROM (SELECT userid
, stl_query.query
, TRIM(DATABASE) AS DATABASE
, TRIM(querytxt) AS qrytext
, md5(TRIM(querytxt)) AS qry_md5
, starttime
, endtime
, datediff(seconds, starttime,endtime)::NUMERIC(12,2) AS run_minutes
, alrt.num_events AS alerts
, aborted 
FROM stl_query LEFT OUTER JOIN ( SELECT query, 1 AS num_events 
FROM STL_ALERT_EVENT_LOG GROUP BY query ) AS alrt ON alrt.query = stl_query.query 
WHERE userid <> 1 AND (querytxt LIKE 'SELECT%' OR querytxt LIKE 'Select%' ) 
-- and database = '' -- and starttime >=  dateadd(day, -7, current_Date) ) 
GROUP BY DATABASE, qry_md5, aborted ORDER BY total desc LIMIT 35
