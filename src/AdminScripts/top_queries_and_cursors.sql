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
**********************************************************************************************/

SELECT qry.pid,
       TRIM(DATABASE) AS DB,
       COUNT(query) AS n_qry,
       MAX(SUBSTRING(qrytext,1,80)) AS qrytext,
       MAX(SUBSTRING(qrytext_cur.listagg,1,80)) AS qrytext_cur,
       MIN(run_seconds) AS "min",
       MAX(run_seconds) AS "max",
       AVG(run_seconds) AS "avg",
       SUM(run_seconds) AS total,
       MAX(query) AS max_query_id,
       MAX(starttime)::DATE AS last_run,
       aborted,
       event
FROM (SELECT pid,
             userid,
             label,
             stl_query.query,
             TRIM(DATABASE) AS DATABASE,
             TRIM(querytxt) AS qrytext,
             MD5(TRIM(querytxt)) AS qry_md5,
             starttime,
             endtime,
             datediff(seconds,starttime,endtime)::NUMERIC(12,2) AS run_seconds,
             aborted,
             decode(alrt.event,
                   'Very selective query filter','Filter',
                   'Scanned a large number of deleted rows','Deleted',
                   'Nested Loop Join in the query plan','Nested Loop',
                   'Distributed a large number of rows across the network','Distributed',
                   'Broadcasted a large number of rows across the network','Broadcast',
                   'Missing query planner statistics','Stats',
                   alrt.event
             ) AS event
      FROM stl_query
        LEFT OUTER JOIN (SELECT query,
                                TRIM(SPLIT_PART(event,':',1)) AS event
                         FROM STL_ALERT_EVENT_LOG
                         WHERE event_time >= dateadd(DAY,-7,CURRENT_DATE)
                         GROUP BY query,
                                  TRIM(SPLIT_PART(event,':',1))) AS alrt ON alrt.query = stl_query.query
      WHERE userid <> 1
      -- and (querytxt like 'SELECT%' or querytxt like 'select%' )
      -- and database = ''
      AND   starttime >= dateadd(DAY,-7,CURRENT_DATE)) qry
  LEFT OUTER JOIN (SELECT ut.pid,
                          listagg(TEXT) within GROUP (ORDER BY SEQUENCE)
                   FROM stl_utilitytext ut,
                        stl_query q
                   WHERE ut.pid = q.pid
                   AND   q.starttime >= dateadd(DAY,-7,CURRENT_DATE)
                   AND   q.userid <> 1
                   GROUP BY ut.pid) qrytext_cur ON (qry.pid = qrytext_cur.pid)
GROUP BY qry.pid,
         DATABASE,
         label,
         qry_md5,
         aborted,
         event
ORDER BY total DESC LIMIT 50;
