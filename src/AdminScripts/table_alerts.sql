/**********************************************************************************************
Purpose: Return Table related Performance Allerts from past 7 days

Columns:
schemaL			Name of Schema
table:		    Name of the table where the alert happened, when applicable
table_rows:		Size of table in rows
minutes:	    Number of minutes spent doing the action. Not available for all alerts
scanned_rows:   Number for rows of the scan/dist/bcast
event:		    What is the Alerted Event
solution	    Proposed Solution to avoid the alert (and performance issue) in the future
sample_query:	query_id of the latest occurency of that alert
count:		    Number of occurences of the alert

Notes:

History:
2015-02-09 ericfe created
2018-09-12 ericfe re-publish to differentiate with the one with SQL text
**********************************************************************************************/
select trim(pgn.nspname) as Schema, trim(s.perm_table_name) as table , tot_rows as table_rows, (sum(abs(datediff(seconds, coalesce(b.starttime,d.starttime,s.starttime), coalesce(b.endtime,d.endtime,s.endtime))))/60)::numeric(24,0) as minutes, 
       sum(coalesce(b.rows,d.rows,s.rows)) as scanned_rows, trim(split_part(l.event,':',1)) as event,  substring(trim(l.solution),1,60) as solution , max(l.query) as sample_query, count(*) as count
from stl_alert_event_log as l 
left join stl_scan as s on s.query = l.query and s.slice = l.slice and s.segment = l.segment
left join stl_dist as d on d.query = l.query and d.slice = l.slice and d.segment = l.segment 
left join stl_bcast as b on b.query = l.query and b.slice = l.slice and b.segment = l.segment
left join ( select id, sum(rows) as tot_rows from stv_Tbl_perm group by id) as t on s.tbl = t.id 
left join pg_class as pgc on pgc.oid = s.tbl left join pg_namespace as pgn on pgn.oid = pgc.relnamespace
where l.userid >1  and  l.event_time >=  dateadd(day, -7, getdate()) AND s.perm_table_name NOT LIKE 'volt_tt%' AND s.perm_table_name NOT LIKE 'Internal Worktable'
group by  1,2,3, 6,7 order by 4 desc,8 desc;