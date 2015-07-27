
--DROP VIEW admin.v_check_wlm_query_time;
/**********************************************************************************************
Purpose: View to get  WLM Queue Wait Time , Execution Time and Total Time by Query for the past 7 Days 
History:
2015-07-01 srinikri Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_check_wlm_query_time
AS
SELECT TRIM(DATABASE) AS DB,
       w.query,
       SUBSTRING(q.querytxt,1,100) AS querytxt,
       w.queue_start_time,
       w.service_class AS class,
       w.slot_count AS slots,
       w.total_queue_time / 1000000 AS queue_seconds,
       w.total_exec_time / 1000000 exec_seconds,
       (w.total_queue_time + w.total_exec_time) / 1000000 AS total_seconds
FROM stl_wlm_query w
  LEFT JOIN stl_query q
         ON q.query = w.query
        AND q.userid = w.userid
WHERE w.queue_start_time >= DATEADD (day,-7,CURRENT_DATE)
AND   w.total_queue_time > 0
AND   w.userid > 1
AND   q.starttime >= DATEADD (day,-7,CURRENT_DATE)
ORDER BY w.total_queue_time DESC,
         w.queue_start_time DESC;
