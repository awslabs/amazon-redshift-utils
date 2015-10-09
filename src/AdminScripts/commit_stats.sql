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
SELECT startqueue,
       node,
       datediff(ms,startqueue,startwork) AS queue_time,
       datediff(ms,startwork,endtime) AS commit_time,
       queuelen
FROM stl_commit_stats
WHERE startqueue >= dateadd (day,-2,CURRENT_DATE)
ORDER BY queuelen DESC,
         queue_time DESC
