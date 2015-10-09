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
**********************************************************************************************/
SELECT TRIM(s.perm_table_name) AS TABLE,
       (SUM(ABS(datediff (seconds,COALESCE(b.starttime,d.starttime,s.starttime),COALESCE(b.endtime,d.endtime,s.endtime)))) / 60)::numeric(24,0) AS minutes,
       SUM(COALESCE(b.rows,d.rows,s.rows)) AS ROWS,
       TRIM(SPLIT_PART(l.event,':',1)) AS event,
       SUBSTRING(TRIM(l.solution),1,60) AS solution,
       MAX(l.query) AS sample_query,
       COUNT(*)
FROM stl_alert_event_log AS l
  LEFT JOIN stl_scan AS s
         ON s.query = l.query
        AND s.slice = l.slice
        AND s.segment = l.segment
  LEFT JOIN stl_dist AS d
         ON d.query = l.query
        AND d.slice = l.slice
        AND d.segment = l.segment
  LEFT JOIN stl_bcast AS b
         ON b.query = l.query
        AND b.slice = l.slice
        AND b.segment = l.segment
WHERE l.userid > 1
AND   l.event_time >= dateadd (day,-7,CURRENT_DATE)
GROUP BY 1,
         4,
         5
ORDER BY 2 DESC,
         6 DESC
