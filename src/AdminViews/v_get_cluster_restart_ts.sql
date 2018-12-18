--DROP VIEW admin.v_get_cluster_restart_ts ;
/**********************************************************************************************
Purpose: View to get the datetime of when Redshift cluster was recently restarted
History:
2015-07-01 srinikri Created
2016-11-07 chriz-bigdata added userid=1 filter to eliminate false positives
**********************************************************************************************/ 
CREATE OR REPLACE VIEW admin.v_get_cluster_restart_ts 
AS
SELECT sysdate current_ts, endtime AS restart_ts
FROM stl_utilitytext
WHERE text LIKE '%xen_is_up.sql%' AND userid = 1
ORDER BY endtime DESC;
