  -- alerts with tables 
select trim(s.perm_table_name) as table , (sum(abs(datediff(seconds, s.starttime, s.endtime)))/60)::numeric(24,0) as minutes, trim(split_part(l.event,':',1)) as event,  substring(trim(l.solution),1,60) as solution , max(l.query) as sample_query, count(*) 
from stl_alert_event_log as l 
left join stl_scan as s on s.query = l.query and s.slice = l.slice and s.segment = l.segment and s.step = l.step
where l.userid >1 
and  l.event_time >=  dateadd(day, -7, current_Date) 
group by 1,3,4 order by 2 desc,6 desc;

