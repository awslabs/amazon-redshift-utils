/**********************************************************************************************
Purpose: Return commit queue statistics from past 2 days, showing largest queue length and queue time first

Columns:
startqueue:		Time when the queueing started
node:			Node where the queue started. Usually -1 (leader). A number between 0 and # nodes might indicate a issue with a node
queue_time:		Queue time in miliseconds
commit_time:		Commit time in miliseconds
queuelen:		Number of transactions in the queue

Notes:

History:
2015-02-09 ericfe created
**********************************************************************************************/
select startqueue,node, datediff(ms,startqueue,startwork) as queue_time, datediff(ms, startwork, endtime) as commit_time, queuelen 
from stl_commit_stats 
where startqueue >=  dateadd(day, -2, current_Date)
order by queuelen desc , queue_time desc;
