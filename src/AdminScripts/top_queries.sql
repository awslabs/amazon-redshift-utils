/**********************************************************************************************
Purpose: Return the top 50 time consuming statements aggregated by it's text.

Columns:
DB:		Database where the query ran
n_qry:		Number of Queries with same SQL text
qrytext:	First 80 Characters of the query SQL
min/max/avg:	Min/Max/Avg Execution time
total:		Total execution time of all occurences
max_query_id:	Largest query id of the query occurence
last_run:	Last day the query dan
aborted:	0 if query ran to completion, 1 if it was canceled.
alert:      Alert event related to the query
 
Notes:
There is a comented filter of the query to filter for only Select statements (otherwise it includes all statements like insert, update, COPY)
There is a comented filter to narrow the query to a given database

History:
2015-02-09 ericfe created
2015-04-17 ericfe Added event name and event time filter
**********************************************************************************************/
-- query runtimes
SELECT TRIM(DATABASE) AS DB,
       COUNT(query) AS n_qry,
       MAX(SUBSTRING(qrytext,1,80)) AS qrytext,
       MIN(run_minutes) AS "min",
       MAX(run_minutes) AS "max",
       AVG(run_minutes) AS "avg",
       SUM(run_minutes) AS total,
       MAX(query) AS max_query_id,
       MAX(starttime)::DATE AS last_run,
       aborted,
       event
FROM (SELECT userid,
             label,
             stl_query.query,
             TRIM(DATABASE) AS DATABASE,
             TRIM(querytxt) AS qrytext,
             MD5(TRIM(querytxt)) AS qry_md5,
             starttime,
             endtime,
             datediff(seconds,starttime,endtime)::NUMERIC(12,2) AS run_minutes,
             aborted,
             DECODE(alrt.event,
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
                         WHERE event_time >= dateadd (DAY,-7,CURRENT_DATE)
                         GROUP BY query,
                                  TRIM(SPLIT_PART(event,':',1))) AS alrt ON alrt.query = stl_query.query
      WHERE userid <> 1
      -- and (querytxt like 'SELECT%' or querytxt like 'select%' ) 
      -- and database = ''
      AND   starttime >= dateadd (DAY,-7,CURRENT_DATE))
GROUP BY DATABASE,
         label,
         qry_md5,
         aborted,
         event
ORDER BY total DESC LIMIT 50
