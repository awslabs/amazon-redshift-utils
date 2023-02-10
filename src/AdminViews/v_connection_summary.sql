
/**********************************************************************************************
Purpose:      View to flatten stl_connection_log table and provide details like session start
              and end time, duration in human readble format and current state i.e disconnected,
              terminated by admin, active or connection lost
History:
2017-12-29 adedotua created
2023-02-09 updated view to use sessionid for joins. added new columns 
           os_version, driver_version and sessionid. fully qualified table names.

**********************************************************************************************/ 
CREATE OR REPLACE VIEW admin.V_CONNECTION_SUMMARY AS
SELECT a.username::varchar
,a.pid
,a.recordtime as authentication_time
,b.recordtime as session_starttime
,d.recordtime as session_endtime
,a.dbname::varchar
,c.application_name::varchar as app_name
,b.authmethod::varchar
,case when d.duration > 0 then (d.duration/1000000)/86400||' days '||((d.duration/1000000)%86400)/3600||'hrs '
||((d.duration/1000000)%3600)/60||'mins '||(d.duration/1000000%60)||'secs' when f.process is null then null else datediff(s,a.recordtime,getdate())/86400||' days '||(datediff(s,a.recordtime,getdate())%86400)/3600||'hrs '
||(datediff(s,a.recordtime,getdate())%3600)/60||'mins '||(datediff(s,a.recordtime,getdate())%60)||'secs' end as duration
,b.mtu
,b.sslversion::varchar
,b.sslcipher::varchar
,b.remotehost::varchar
,b.remoteport::varchar
,case when e.recordtime is not null then 'Terminated by administrator' 
when d.recordtime is not null then 'Disconnected' 
when f.process is not null then 'Active' else 'Connection Lost' end as current_state
,a.sessionid::varchar
,nvl(a.os_version,b.os_version)::varchar as os_version
,nvl(a.driver_version,b.driver_version)::varchar as driver_version
FROM
(SELECT * FROM pg_catalog.stl_connection_log WHERE event='authenticated') a
LEFT JOIN (SELECT * FROM pg_catalog.stl_connection_log WHERE event='initiating session') b using (sessionid)
LEFT JOIN (SELECT * FROM pg_catalog.stl_connection_log WHERE event='set application_name') c using (sessionid)
LEFT JOIN (SELECT * FROM pg_catalog.stl_connection_log WHERE event='disconnecting session') d using (sessionid) 
LEFT JOIN (SELECT * FROM pg_catalog.stl_connection_log WHERE event='Terminating backend on administrator''s request') e using (sessionid) 
LEFT JOIN pg_catalog.stv_sessions f on a.pid=f.process and a.dbname=f.db_name and a.username=f.user_name 
and datediff(s,f.starttime,a.recordtime) < 5
WHERE a.username <> 'rdsdb'
ORDER BY 3;
