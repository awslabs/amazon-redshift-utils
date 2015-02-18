/**********************************************************************************************
Purpose: Return Alerts from past 7 days

Columns:
table:		Name of the table where the alert happened, when applicable
minutes:	Number of minutes spent doing the action (usually table scan). Not available for other alerts
event:		What is the Alerted Event
solution	Proposed Solution to avoid the alert (and performance issue) in the future
sample_query:	query_id of the latest occurency of that alert
count:		Number of occurences of the alert

Notes:

History:
2015-02-09 ericfe created
**********************************************************************************************/
select trim(s.perm_table_name) as table , (sum(abs(datediff(seconds, s.starttime, s.endtime)))/60)::numeric(24,0) as minutes, trim(split_part(l.event,':',1)) as event,  
substring(trim(l.solution),1,60) as solution , max(l.query) as sample_query, count(*) as "count"
from stl_alert_event_log as l 
left join stl_scan as s on s.query = l.query and s.slice = l.slice and s.segment = l.segment and s.step = l.step
where l.userid >1 
  and  l.event_time >=  dateadd(day, -7, current_Date) 
group by 1,3,4 order by 2 desc,6 desc;

