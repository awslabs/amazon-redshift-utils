/* Query showing explain plans that triggered entries into STL_ALERT_EVENT_LOG */
SELECT trim(s.perm_table_name) AS TABLE
       ,(SUM(abs(datediff(seconds,s.starttime,s.endtime))) / 60)::numeric(24,0) AS minutes
       ,trim(split_part (l.event,':',1)) AS event
       ,trim(l.solution) AS solution
       ,max(l.query) AS sample_query
       ,COUNT(*)
FROM stl_alert_event_log AS l
  LEFT JOIN stl_scan AS s
        ON s.query = l.query
        AND s.slice = l.slice
        AND s.segment = l.segment
        AND s.step = l.step
WHERE l.event_time >= dateadd(day,-7,CURRENT_DATE)
GROUP BY 1
         ,3
         ,4
ORDER BY 2 DESC
         ,6 DESC
