/**********************************************************************************************
Purpose: Return the top 50 time consuming statements aggregated by it's text.

Columns:
DB:		Database where the query ran
n_qry:		Number of Queries with same SQL text
qrytext:	First 80 Characters of the query SQL
min/max/avg:	Min/Max/Avg Execution time
total:		Total execution time of all occurences
max_query_id:	Largest query id of the query occurence
last_run:	Last day the query ran
aborted:	0 if query ran to completion, 1 if it was canceled.
alerts:		Alert events related to the query

Notes:
There is a commented filter of the query to filter for only Select statements (otherwise it includes all statements like insert, update, COPY)
There is a commented filter to narrow the query to a given database

History:
2015-02-09 ericfe created
2015-04-17 ericfe Added event name and event time filter
**********************************************************************************************/
-- query runtimes
select
--        trim(database)                                     as DB,
trim(u.usename)                                             as username,
substring(qrytext, 1, 256)                                  as qrytext,
starttime                                                   as start_time,
run_seconds                                                 as total_seconds,
query                                                       as query_id,
aborted,
listagg(distinct event, ', ') within group (order by query) as events
from
    (
        select
            userid,
            label,
            stl_query.query,
            trim(database)                                        as database,
            trim(querytxt)                                        as qrytext,
            md5(trim(querytxt))                                   as qry_md5,
            starttime,
            endtime,
            datediff(seconds, starttime, endtime)::numeric(12, 2) as run_seconds,
            aborted,
            alrt.event
        from
            stl_query
                left outer join (select
                                     query,
                                     trim(split_part(event, ':', 1)) as event
                                 from
                                     STL_ALERT_EVENT_LOG
                                 where
                                     event_time >= dateadd(hr, -3, getdate())
                                 group by query, trim(split_part(event, ':', 1))) as alrt
                    on alrt.query = stl_query.query
        where
              userid <> 1
          and starttime >= dateadd(hr, -3, getdate())
    ) as sqa
        left outer join pg_user u
            on (sqa.userid = u.usesysid)
group by
    username, qrytext, starttime, run_seconds, query, aborted
order by
    total_seconds desc
limit 1000;
