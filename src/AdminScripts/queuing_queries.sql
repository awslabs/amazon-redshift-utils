/* query showing queries which are waiting on a WLM Query Slot */
SELECT w.query
       ,substring(q.querytxt,1,100) AS querytxt
       ,w.queue_start_time
       ,w.service_class AS class
       ,w.slot_count AS slots
       ,w.total_queue_time / 1000000 AS queue_seconds
       ,w.total_exec_time / 1000000 exec_seconds
       ,(w.total_queue_time + w.total_Exec_time) / 1000000 AS total_seconds
FROM stl_wlm_query w
  LEFT JOIN stl_query q
         ON q.query = w.query
        AND q.userid = w.userid
WHERE w.queue_start_Time >= dateadd(day,-7,CURRENT_DATE)
AND   w.total_queue_Time > 0
-- and q.starttime >= dateadd(day, -7, current_Date)    
-- and ( querytxt like 'select%' or querytxt like 'SELECT%' ) 
ORDER BY w.total_queue_time DESC
         ,w.queue_start_time DESC limit 35
