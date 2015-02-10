--Commit stats
select startqueue,node, datediff(ms,startqueue,startwork) as queue_time, datediff(ms, startwork, endtime) as commit_time, queuelen from stl_commit_stats 
where startqueue >=  dateadd(day, -2, current_Date)
order by queuelen desc , startwork desc, queue_time desc;


