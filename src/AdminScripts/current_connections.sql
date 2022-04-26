select
    s.process                           as process_id,
    c.remotehost || ':' || c.remoteport as remote_address,
    s.user_name                         as username,
    s.starttime                         as session_start_time,
    s.db_name,
    i.starttime                         as current_query_time,
    i.text                              as query
from
    stv_sessions s
        left join pg_user u
            on u.usename = s.user_name
        left join stl_connection_log c
            on c.pid = s.process
            and c.event = 'authenticated'
        left join stv_inflight i
            on u.usesysid = i.userid
            and s.process = i.pid
where
    username <> 'rdsdb'
order by
    session_start_time desc;
