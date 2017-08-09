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
**********************************************************************************************/
select trim(database) as DB, count(query) as n_qry, max(substring (qrytext ,1,120)) as qrytext, max(run_seconds) as "max", avg(run_seconds) as "avg", sum(run_seconds) as total, max(service_class) as queue,
       avg(cpu) as cpu, avg(cpupct) as cpupct, avg(spill) as spill, avg(mb_read) as mb_Read, avg(rows_ret) as rows_ret,
       max(query) as max_query, max(starttime)::date as last_run, aborted, -- max(mylabel),
       trim(decode(event&1,1,'Sortkey ','') || decode(event&2,2,'Deletes ','') || decode(event&4,4,'NL ','') ||  decode(event&8,8,'Dist ','') || decode(event&16,16,'Broacast ','') || decode(event&32,32,'Stats ','')) as Alert
from (
select q.userid, q.query, trim(q.database) as database, nvl(qrytext_cur.text,trim(q.querytxt) ) as qrytext, md5(nvl(qrytext_cur.text,trim(q.querytxt))) as qry_md5, q.starttime, q.endtime, 
datediff(seconds, q.starttime,q.endtime)::numeric(12,2) as run_seconds, q.aborted, event, q.label as mylabel, qs.service_Class, qs.query_cpu_time as cpu, qs.query_cpu_usage_percent as cpupct, qs.query_temp_blocks_to_disk as spill,
qs.query_blocks_read as mb_read, qs.return_row_count as rows_ret
from stl_query q
left outer Join svl_Query_metrics_summary qs on ( q.userid = qs.userid and q.query = qs.query )
left outer join ( select query,sum(decode(trim(split_part(event,':',1)),'Very selective query filter',1,'Scanned a large number of deleted rows',2,'Nested Loop Join in the query plan',4,'Distributed a large number of rows across the network',8,'Broadcasted a large number of rows across the network',16,'Missing query planner statistics',32,0)) as event from STL_ALERT_EVENT_LOG 
     where event_time >=  dateadd(day, -7, current_Date) group by query  ) as alrt on alrt.query = q.query
LEFT OUTER JOIN (SELECT ut.xid,'CURSOR ' || TRIM( substring ( TEXT from strpos(upper(TEXT),'SELECT') )) as TEXT
                   FROM stl_utilitytext ut
		           WHERE sequence = 0
		           AND upper(TEXT) like 'DECLARE%'
                   GROUP BY text, ut.xid) qrytext_cur ON (q.xid = qrytext_cur.xid)
where q.userid <> 1 
-- and (q.querytxt like 'SELECT%' or querytxt like 'select%' ) 
-- and q.querytxt ilike 'COPY%'  
-- and q.database = ''
-- and q.aborted = 1
and q.starttime >=  dateadd(day, -2, current_Date)
 ) 
group by database, userid, qry_md5, aborted, event
order by total desc limit 50;