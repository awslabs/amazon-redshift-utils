/* Query showing information about sessions with currently running queries */
SELECT s.process AS pid
       ,date_Trunc ('second',s.starttime) AS S_START
       ,datediff(minutes,s.starttime,getdate ()) AS conn_mins
       ,trim(s.user_name) AS USER
       ,trim(s.db_name) AS DB
       ,date_trunc ('second',i.starttime) AS Q_START
       ,i.query
       ,trim(i.query) AS sql
FROM stv_sessions s
  LEFT JOIN stv_recents i
         ON s.process = i.pid
        AND i.status = 'Running'
WHERE s.user_name <> 'rdsdb'
ORDER BY 1