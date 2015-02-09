/**********************************************************************************************
Purpose: Return the top 50 time consuming statements aggregated by it's text.
 
History:
2015-02-09 ericfe created
**********************************************************************************************/
select trim(database) as DB, count(query) as n_qry, max(substring (qrytext,1,80)) as qrytext, min(run_minutes) as "min" , max(run_minutes) as "max", avg(run_minutes) as "avg", sum(run_minutes) as total,  max(query) as max_query_id, max(starttime)::date as last_run, /*label, */ sum(alerts) as alerts, aborted
from (
select userid, label, stl_query.query, trim(database) as database, trim(querytxt) as qrytext, md5(trim(querytxt)) as qry_md5, starttime, endtime, datediff(seconds, starttime,endtime)::numeric(12,2) as run_minutes, alrt.num_events as alerts, aborted 
from stl_query 
left outer join ( select query, 1 as num_events from STL_ALERT_EVENT_LOG group by query ) as alrt on alrt.query = stl_query.query
where userid <> 1 
-- and (querytxt like 'SELECT%' or querytxt like 'select%' ) 
-- and database = ''
and starttime >=  dateadd(day, -7, current_Date)
 ) 
group by database, label, qry_md5, aborted
order by total desc limit 50;
