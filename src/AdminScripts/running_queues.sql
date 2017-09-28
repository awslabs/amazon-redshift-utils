/**********************************************************************************************
Purpose: Return the Current queries running and queueing, along with resource consumption.
Columns:

user :			User name
pid :			Pid of the session
xid :			Transaction identity
query :			Query Id
q :				Queue
slt :			Slots Uses
start :			Time query was issued
state :			Current State
q_sec :			Seconds in queue
exe_sec :		Seconds Executed
cpu_sec :		CPU seconds consumed
read_mb :		MB read by the query
spill_mb :		MB spilled to disk
ret_rows :		Rows returned to Leader -> Client
nl_rows :		# of rows of Nested Loop Join
sql :			First 90 Characters of the query SQL
alert :			Alert events related to the query

History:
2017-09-28 ericfe created
**********************************************************************************************/
select trim(u.usename) as user, s.pid, q.xid,q.query,q.service_class as "q", q.slot_count as slt, date_trunc('second',q.wlm_start_time) as start,decode(trim(q.state), 'Running','Run','QueuedWaiting','Queue','Returning','Return',trim(q.state)) as state, 
q.queue_Time/1000000 as q_sec, q.exec_time/1000000 as exe_sec, m.cpu_time/1000000 cpu_sec, m.blocks_read read_mb, decode(m.blocks_to_disk,-1,null,m.blocks_to_disk) spill_mb , m2.rows as ret_rows, m3.rows as NL_rows,
substring(replace(nvl(qrytext_cur.text,trim(translate(s.text,chr(10)||chr(13)||chr(9) ,''))),'\\n',' '),1,90) as sql,
trim(decode(event&1,1,'SK ','') || decode(event&2,2,'Del ','') || decode(event&4,4,'NL ','') ||  decode(event&8,8,'Dist ','') || decode(event&16,16,'Bcast ','') || decode(event&32,32,'Stats ','')) as Alert
from  stv_wlm_query_state q 
left outer join stl_querytext s on (s.query=q.query and sequence = 0)
left outer join stv_query_metrics m on ( q.query = m.query and m.segment=-1 and m.step=-1 )
left outer join stv_query_metrics m2 on ( q.query = m2.query and m2.step_type = 38 )
left outer join ( select query, sum(rows) as rows from stv_query_metrics m3 where step_type = 15 group by 1) as m3 on ( q.query = m3.query )
left outer join pg_user u on ( s.userid = u.usesysid )
LEFT OUTER JOIN (SELECT ut.xid,'CURSOR ' || TRIM( substring ( TEXT from strpos(upper(TEXT),'SELECT') )) as TEXT
                   FROM stl_utilitytext ut
                   WHERE sequence = 0
                   AND upper(TEXT) like 'DECLARE%'
                   GROUP BY text, ut.xid) qrytext_cur ON (q.xid = qrytext_cur.xid)
left outer join ( select query,sum(decode(trim(split_part(event,':',1)),'Very selective query filter',1,'Scanned a large number of deleted rows',2,'Nested Loop Join in the query plan',4,'Distributed a large number of rows across the network',8,'Broadcasted a large number of rows across the network',16,'Missing query planner statistics',32,0)) as event from STL_ALERT_EVENT_LOG 
     where event_time >=  dateadd(hour, -8, current_Date) group by query  ) as alrt on alrt.query = q.query
order by q.service_class,q.exec_time desc, q.wlm_start_time;