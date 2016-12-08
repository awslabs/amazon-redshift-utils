
--DROP VIEW admin.v_check_wlm_query_trend_daily;
/**********************************************************************************************
Purpose: View to get  WLM Query Count, Queue Wait Time , Execution Time and Total Time by Day 
History:
2015-07-01 srinikri Created
2015-07-23 ericfe updated column to be a proper date
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_check_wlm_query_trend_daily
AS
SELECT trunc(a.service_class_start_time) AS day, 
       a.service_class, 
       b.condition AS service_class_condition, 
       COUNT(a.query) AS query_count, 
       SUM(a.total_queue_time) AS total_queue_time_sum, 
       SUM(a.total_exec_time) AS total_exec_time_sum, 
       (SUM(a.total_queue_time)::FLOAT/ NULLIF(SUM(a.total_exec_time),0)::FLOAT)*100 AS percent_wlm_queue_time 
FROM stl_wlm_query a 
  JOIN stv_wlm_classification_config b ON a.service_class = b.action_service_class 
GROUP BY trunc(a.service_class_start_time) , 
         a.service_class, 
         b.condition 
ORDER BY trunc(a.service_class_start_time) DESC, 
         a.service_class DESC;
