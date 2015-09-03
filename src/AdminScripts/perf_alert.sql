/**********************************************************************************************
Purpose: Return Alerts from past 7 days

Columns:
table:		    Name of the table where the alert happened, when applicable
minutes:	    Number of minutes spent doing the action. Not available for all alerts
rows:           Number for rows of the scan/dist/bcast
event:		    What is the Alerted Event
solution	    Proposed Solution to avoid the alert (and performance issue) in the future
sample_query:	query_id of the latest occurency of that alert
count:		    Number of occurences of the alert

Notes:

History:
2015-02-09 ericfe created
2015-04-17 ericfe Added detail information on distributions and broadcasts. Added rows column
**********************************************************************************************/
select trim(s.perm_table_name) as table , (sum(abs(datediff(seconds, coalesce(b.starttime,d.starttime,s.starttime), coalesce(b.endtime,d.endtime,s.endtime))))/60)::numeric(24,0) as minutes,
       sum(coalesce(b.rows,d.rows,s.rows)) as rows, trim(split_part(l.event,':',1)) as event,  substring(trim(l.solution),1,60) as solution , max(l.query) as sample_query, count(*)
from stl_alert_event_log as l
left join stl_scan as s on s.query = l.query and s.slice = l.slice and s.segment = l.segment
left join stl_dist as d on d.query = l.query and d.slice = l.slice and d.segment = l.segment
left join stl_bcast as b on b.query = l.query and b.slice = l.slice and b.segment = l.segment
where l.userid >1
and  l.event_time >=  dateadd(day, -7, current_Date)
group by 1,4,5 order by 2 desc,6 desc;
