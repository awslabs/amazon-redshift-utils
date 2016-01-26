/*******************************************************************************
Purpose: Return COPY information from past 7 days

Columns:
Starttime:		    Date and Time COPY started
query:	    		Query id
querytxt:           Partial SQL
n_files:		    Number of files
size_mb:	    	Size of the COPY in Megabytes
time_seconds:		Duration in Seconds
mb_per_s:		    Megabytes per second

Notes:

History:
2016-01-26 ericfe created
*******************************************************************************/
select q.starttime,  s.query, substring(q.querytxt,1,120) as querytxt,
       s.n_files, size_mb, s.time_seconds,
       s.size_mb/decode(s.time_seconds,0,1,s.time_seconds)  as mb_per_s
from (select query, count(*) as n_files,
     sum(transfer_size/(1024*1024)) as size_MB, (max(end_Time) -
         min(start_Time))/(1000000) as time_seconds , max(end_time) as end_time
      from stl_s3client where http_method = 'GET' and query > 0
       and transfer_time > 0 group by query ) as s
LEFT JOIN stl_Query as q on q.query = s.query
where s.end_Time >=  dateadd(day, -7, current_Date)
order by s.time_Seconds desc, size_mb desc, s.end_time desc
limit 50;
