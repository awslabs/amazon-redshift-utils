
--DROP VIEW admin.v_get_cluster_restart_ts ;
/**********************************************************************************************
Purpose: View to get the datetime of when Redshift cluster was recently restarted
History:
2015-07-01 srinikri Created
**********************************************************************************************/ 
CREATE OR REPLACE VIEW admin.v_get_cluster_restart_ts 
AS
SELECT sysdate current_ts, endtime AS restart_ts
FROM stl_utilitytext
WHERE text LIKE '%xen_is_up.sql%'
ORDER BY endtime DESC;