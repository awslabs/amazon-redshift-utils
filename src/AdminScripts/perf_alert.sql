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
2017-08-11 andrewgross Added schema and query text information.
**********************************************************************************************/
SELECT t.schema AS SCHEMA,
       trim(s.perm_table_name) AS TABLE,
       (sum(abs(datediff(seconds, coalesce(b.starttime,d.starttime,s.starttime), CASE WHEN coalesce(b.endtime,d.endtime,s.endtime) > coalesce(b.starttime,d.starttime,s.starttime) THEN coalesce(b.endtime,d.endtime,s.endtime) ELSE coalesce(b.starttime,d.starttime,s.starttime) END)))/60)::numeric(24,0) AS minutes,
       sum(coalesce(b.rows,d.rows,s.rows)) AS ROWS,
       trim(split_part(l.event,':',1)) AS event,
       substring(trim(l.solution),1,60) AS solution,
       max(l.query) AS sample_query,
       count(DISTINCT l.query),
       q.text AS query_text
FROM stl_alert_event_log AS l
LEFT JOIN stl_scan AS s ON s.query = l.query
AND s.slice = l.slice
AND s.segment = l.segment
LEFT JOIN stl_dist AS d ON d.query = l.query
AND d.slice = l.slice
AND d.segment = l.segment
LEFT JOIN stl_bcast AS b ON b.query = l.query
AND b.slice = l.slice
AND b.segment = l.segment
LEFT JOIN
  (SELECT query,
          LISTAGG(text) WITHIN
   GROUP (
          ORDER BY sequence) AS text
   FROM stl_querytext
   WHERE sequence < 100
   GROUP BY query) AS q ON q.query = l.query
LEFT JOIN svv_table_info AS t ON t.table_id = s.tbl
WHERE l.userid >1
  AND l.event_time >= dateadd(DAY, -7, CURRENT_DATE)
  AND s.perm_table_name NOT LIKE 'volt_tt%'
  AND SCHEMA IS NOT NULL
GROUP BY 1,
         2,
         5,
         6,
         query_text
ORDER BY 3 DESC, 7 DESC;
