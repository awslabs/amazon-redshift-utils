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
2015-11-20 ericfe filter off nodeid 0 rows and non proper filter plan info
2019-02-21 ericfe add Join Filter back
**********************************************************************************************/
select trim(s.perm_Table_name) as table , substring(trim(info),1,580) as filter, sum(datediff(seconds,starttime,case when starttime > endtime then starttime else endtime end)) as secs, count(distinct i.query) as num, max(i.query) as query
from stl_explain p
join stl_plan_info i on ( i.userid=p.userid and i.query=p.query and i.nodeid=p.nodeid  )
join stl_scan s on (s.userid=i.userid and s.query=i.query and s.segment=i.segment and s.step=i.step)
where s.starttime > dateadd(day, -7, current_Date)
and s.perm_table_name not like 'Internal Worktable%'
and (( p.info like 'Filter:%'  and p.nodeid > 0 ) or p.info like 'Join Filter:%')
and s.perm_table_name like '%' -- choose table(s) to look for
group by 1,2 order by 1, 3 desc , 4 desc;
