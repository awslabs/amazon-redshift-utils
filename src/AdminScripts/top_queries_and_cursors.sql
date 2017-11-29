/**********************************************************************************************
Purpose: Return the top 50 time consuming statements aggregated by it's text.
Columns:
DB:				Database where the query ran
n_qry:			Number of Queries with same SQL text
qrytext:		First 80 Characters of the query SQL
qrytext_cur:    First 80 Characters of the cursor SQL, not null when cursors are used instead of direct SQL
max/avg:		Max/Avg Execution time
total:			Total execution time of all occurrences
queue:			Highest queue query ran at
cpu:			AVG Miliseconds of cpu consumed
cpupct:			AVG pct CPU consumed
spill:			AVG Temporary space used by spill in mb
mb_read:		AVG MB read by the query
rows_ret:		AVG rows returned to leader node / client
max_query:	Largest query id of the query occurrence
last_run:		Last day the query ran
aborted:		0 if query ran to completion, 1 if it was canceled.
alert:          Alert event related to the query

History:
2017-04-07 thiyagu created
2017-05-19 ericfe removed listagg
2017-08-09 ericfe added QMR statistics columns
2017-11-21 ericfe Fixed Event Alert Bug
**********************************************************************************************/
select trim(database) as DB, count(query) as n_qry, max(substring (qrytext ,1,120)) as qrytext, max(run_seconds) as "max_s", avg(run_seconds) as "avg_s", sum(run_seconds) as total_s, max(service_class) as queue, avg(queue_sec) as avg_q_sec,
       avg(cpu) as cpu_sec, avg(cpupct) as cpupct, avg(spill/1024) as spill_gb, avg(mb_read) as mb_Read, avg(rows_ret) as rows_ret, max(mem_bytes/(1024*1024)) as peak_mem_gb,
       max(query) as max_query, to_char(max(starttime), 'YYYY-MM-DD HH24:MM:SS') as last_run, aborted, -- max(mylabel) as Label,
       event as Alert
from (
select q.userid, q.query, trim(q.database) as database, trim(q.label) as label, nvl(qrytext_cur.text,trim(q.querytxt) ) as qrytext, md5(nvl(qrytext_cur.text,trim(q.querytxt))) as qry_md5, q.starttime, q.endtime, 
datediff(seconds, greatest(q.starttime,nvl(w.exec_start_time,'2000-01-01')),q.endtime)::numeric(12) as run_seconds, q.aborted, alrt.event, q.label as mylabel, qs.service_Class, qs.query_cpu_time as cpu, qs.query_cpu_usage_percent as cpupct, qs.query_temp_blocks_to_disk as spill,
qs.query_blocks_read as mb_read, qs.return_row_count as rows_ret, datediff('seconds',w.queue_Start_time, w.queue_end_time) as queue_sec, w.est_peak_mem as mem_bytes
from stl_query q
left outer join stl_wlm_query w on (q.query = w.query and q.userid = w.userid)
left outer Join svl_Query_metrics_summary qs on ( q.userid = qs.userid and q.query = qs.query )
left outer join ( select query,listagg (distinct decode(trim(split_part(event,':',1)),'Very selective query filter','SK','Scanned a large number of deleted rows','Del','Nested Loop Join in the query plan','NL','Distributed a large number of rows across the network','Dist',
'Broadcasted a large number of rows across the network','Bcast','Missing query planner statistics','Stats','DS_DIST_ALL_INNER for Hash Join in the query plan','InvAll', split_part(event,':',1)),' ' ) WITHIN GROUP (order by event)  as event from STL_ALERT_EVENT_LOG  where event_time >=  dateadd(day, -7, current_Date) group by query  ) as alrt on alrt.query = q.query
LEFT OUTER JOIN (SELECT ut.xid,'CURSOR ' || TRIM( substring ( TEXT from strpos(upper(TEXT),'SELECT') )) as TEXT
                   FROM stl_utilitytext ut
		           WHERE sequence = 0
		           AND upper(TEXT) like 'DECLARE%'
                   GROUP BY text, ut.xid) qrytext_cur ON (q.xid = qrytext_cur.xid)
where q.userid <> 1 
-- and q.querytxt ilike 'SELECT%' 
-- and q.querytxt ilike 'COPY%'  
-- and q.database = ''
-- and q.aborted = 1
and q.starttime >=  dateadd(day, -7, current_Date)
 ) 
group by database, userid, qry_md5, aborted, event
order by total_s desc limit 50;