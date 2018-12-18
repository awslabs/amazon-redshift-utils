--DROP VIEW admin.v_generate_cancel_query;
/**********************************************************************************************
Purpose: View to get cancel query 
History:
2015-07-01 srinikri Created
**********************************************************************************************/ 
CREATE OR REPLACE VIEW admin.v_generate_cancel_query
AS
SELECT pid,
       starttime,
       duration,
       TRIM(user_name) AS "USER",
       TRIM(query) AS querytxt,
       'CANCEL  ' + pid::VARCHAR(20) + ';' AS cancel_query
FROM stv_recents
WHERE status = 'Running'
ORDER BY starttime DESC;
