/**********************************************************************************************
Purpose: Return the top 50 time consuming statements aggregated by it's text.
Columns:
DB:				Database where the query ran
n_qry:			Number of Queries with same SQL text
qrytext:		First 80 Characters of the query SQL
qrytext_cur:    First 80 Characters of the cursor SQL, not null when cursors are used instead of direct SQL
min/max/avg:	Min/Max/Avg Execution time
total:			Total execution time of all occurrences
max_query_id:	Largest query id of the query occurrence
last_run:		Last day the query ran
aborted:		0 if query ran to completion, 1 if it was canceled.
alert:          Alert event related to the query

History:
2017-04-07 thiyagu created
2017-05-19 ericfe removed listagg
**********************************************************************************************/
select trim(database) as DB, count(query) as n_qry, max(substring (qrytext ,1,120)) as qrytext, min(run_minutes) as "min" , max(run_minutes) as "max", avg(run_minutes) as "avg", sum(run_minutes) as total,  
       max(query) as max_query_id, max(starttime)::date as last_run, aborted, max(mylabel),
       trim(decode(event&1,1,'Sortkey ','') || decode(event&2,2,'Deletes ','') || decode(event&4,4,'NL ','') ||  decode(event&8,8,'Dist ','') || decode(event&16,16,'Broacast ','') || decode(event&32,32,'Stats ','')) as Alert
from (
select userid, label, stl_query.query, trim(database) as database, nvl(qrytext_cur.text,trim(querytxt) ) as qrytext, md5(nvl(qrytext_cur.text,trim(querytxt))) as qry_md5, starttime, endtime, datediff(seconds, starttime,endtime)::numeric(12,2) as run_minutes, aborted, event, stl_query.label as mylabel
from stl_query 
left outer join ( select query,sum(decode(trim(split_part(event,':',1)),'Very selective query filter',1,'Scanned a large number of deleted rows',2,'Nested Loop Join in the query plan',4,'Distributed a large number of rows across the network',8,'Broadcasted a large number of rows across the network',16,'Missing query planner statistics',32,0)) as event from STL_ALERT_EVENT_LOG 
     where event_time >=  dateadd(day, -7, current_Date) group by query  ) as alrt on alrt.query = stl_query.query
LEFT OUTER JOIN (SELECT ut.xid,TRIM( substring ( TEXT from strpos(upper(TEXT),'SELECT') )) as TEXT
                   FROM stl_utilitytext ut  
                   WHERE sequence = 0 AND upper(TEXT) like 'DECLARE%'
                   GROUP BY text, ut.xid) qrytext_cur ON (stl_query.xid = qrytext_cur.xid)
where userid <> 1 
-- and (querytxt like 'SELECT%' or querytxt like 'select%' ) 
-- and querytxt ilike 'COPY%'  
-- and database = ''
-- and aborted = 1
and starttime >=  dateadd(day, -2, current_Date)

 ) 
group by database, userid, label, qry_md5, aborted, event
order by total desc limit 50;