
/**********************************************************************************************
Purpose:      View to flatten stl_connection_log table and provide details like session start
              and end time, duration in human readble format and current state i.e disconnected,
              terminated by admin, active or connection lost
History:
2017-12-29 adedotua created
**********************************************************************************************/ 
CREATE OR REPLACE VIEW admin.V_CONNECTION_SUMMARY as
select trim(a.username) as username,
a.pid,
a.recordtime as authentication_time,
b.recordtime as session_starttime,
d.recordtime session_endtime,
trim(a.dbname) as dbname,
trim(c.application_name) as app_name,
trim(b.authmethod) as authmethod,
case when d.duration > 0 then (d.duration/1000000)/86400||' days '||((d.duration/1000000)%86400)/3600||'hrs '
||((d.duration/1000000)%3600)/60||'mins '||(d.duration/1000000%60)||'secs' when f.process is null then null else datediff(s,a.recordtime,getdate())/86400||' days '||(datediff(s,a.recordtime,getdate())%86400)/3600||'hrs '
||(datediff(s,a.recordtime,getdate())%3600)/60||'mins '||(datediff(s,a.recordtime,getdate())%60)||'secs' end as duration,
b.mtu,
trim(b.sslversion) as sslversion,
trim(b.sslcipher) as sslcipher,
trim(b.remotehost) as remotehost,
trim(b.remoteport) as remoteport,
case when e.recordtime is not null then 'Terminated by administrator' 
when d.recordtime is not null then 'Disconnected' 
when f.process is not null then 'Active' else 'Connection Lost' end as current_state
from
(select * from stl_connection_log where event='authenticated') a
left join (select * from stl_connection_log where event='initiating session') b using (pid,dbname,remotehost,remoteport,username)
left join (select * from stl_connection_log where event='set application_name') c using (pid,dbname,remotehost,remoteport,username)
left join (select * from stl_connection_log where event='disconnecting session') d using (pid,dbname,remotehost,remoteport,username) 
left join (select * from stl_connection_log where event='Terminating backend on administrator''s request') e using (pid,dbname,remotehost,remoteport,username) 
left join stv_sessions f on a.pid=f.process and a.dbname=f.db_name and a.username=f.user_name 
and datediff(s,f.starttime,a.recordtime) < 5
where a.username<>'rdsdb'
order by 3;
