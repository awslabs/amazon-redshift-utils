
--DROP VIEW admin.v_check_wlm_query_trend_hourly;
/**********************************************************************************************
Purpose: View to get  WLM Query Count, Queue Wait Time , Execution Time and Total Time by Hour
History:
2015-07-01 srinikri Created
2015-07-23 ericfe updated column to be a proper timestamp
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_check_wlm_query_trend_hourly
AS
SELECT date_trunc('hour',a.service_class_start_time) AS hour,
       a.service_class,
       b.condition AS service_class_condition,
       COUNT(a.query) AS query_count,
       SUM(a.total_queue_time) AS total_queue_time_sum,
       SUM(a.total_exec_time) AS total_exec_time_sum,
       (NVL(SUM(a.total_queue_time)::FLOAT,0)/ nullif(SUM(a.total_exec_time)::FLOAT,0))*100 AS percent_wlm_queue_time
FROM stl_wlm_query a
  join stv_wlm_classification_config b ON a.service_class = b.action_service_class
GROUP BY date_trunc('hour',a.service_class_start_time),
         a.service_class,
         b.condition
ORDER BY date_trunc('hour',a.service_class_start_time) DESC,
         a.service_class DESC;