--DROP VIEW admin.v_generate_terminate_session;
/**********************************************************************************************
Purpose: View to generate pg_terminate_backend statements 
History:
2016-05-25 chriz-bigdata Created
**********************************************************************************************/ 
CREATE OR REPLACE VIEW admin.v_generate_terminate_session
AS
SELECT process,
       starttime,
       TRIM(user_name) AS "user",
       'SELECT pg_terminate_backend(' || process || ');' AS terminate_stmt
FROM stv_sessions
WHERE user_name != 'rdsdb'
AND process != pg_backend_pid()
ORDER BY starttime;
