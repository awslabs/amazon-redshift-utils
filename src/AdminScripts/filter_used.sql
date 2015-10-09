/**********************************************************************************************
Purpose: Return instances of table filter for all or a given table in the past 7 days

Columns:
table:		Table Name
filter:		Text of the filter from explain plan
secs:		Number of seconds spend scaning the table
num:		Number of times that filter occured
query:		Latest query id of a query that used that filter on that table

Notes:
Use the perm_table_name fileter to narrow the results


History:
2015-02-09 ericfe created
**********************************************************************************************/
SELECT TRIM(s.perm_Table_name) AS TABLE,
       SUBSTRING(TRIM(info),1,180) AS FILTER,
       SUM(datediff (seconds,starttime,endtime)) AS secs,
       COUNT(*) AS num,
       MAX(i.query) AS query
FROM stl_explain p
  JOIN stl_plan_info i
    ON (i.userid = p.userid
   AND i.query = p.query
   AND i.nodeid = p.nodeid)
  JOIN stl_scan s
    ON (s.userid = i.userid
   AND s.query = i.query
   AND s.segment = i.segment
   AND s.step = i.step)
WHERE s.starttime > dateadd (day,-7,CURRENT_DATE)
AND   s.perm_table_name NOT LIKE 'Internal Worktable%'
AND   p.info <> ''
AND   s.perm_table_name LIKE '%' -- chose table(s) to look for
GROUP BY 1,
         2
ORDER BY 1,
         4 DESC,
         3 DESC

