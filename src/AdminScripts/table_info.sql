/**********************************************************************************************
Purpose: Return Table storage information (size, skew, etc)

Columns:
schema:		Schema name
Table:		Table name
id:		Table id
DistKey:	Distribution Key (shows EVEN for event disttributed, ALL for Diststyle ALL)
Skew:		Table Skew. Proportion between largest slice and smallest slice (null for diststyle ALL)
Sortkey:	First column of Sortkey
#SKs:		Number of columns in the compound sortkey
rows:		Number of rows
mbytes:		Size of the table in Megabytes
Enc:		Y if the table has at least one compressed column, N otherwise
pct_of_total:	Size of the table in proportion to the cluster size
pct_stats_off:	Measure of staleness of table statistics (real size versus size recorded in stats)
pct_unsorted:	Proportion of unsorted rows compared to total rows

Notes:


History:
2015-02-16 ericfe created
**********************************************************************************************/
select trim(pgn.nspname) as Schema, trim(a.name) as Table, id as TableId, decode(pgc.reldiststyle,0, 'EVEN',1,det.distkey ,8,'ALL') as DistKey, decode(pgc.reldiststyle,8,null,dist_ratio.ratio::decimal(10,4)) as Skew, 
det.head_sort as "SortKey", det.n_sortkeys as "#SKs",  a.rows, b.mbytes,  decode(det.max_enc,0,'N','Y')  as Enc, 
decode(b.mbytes,0,0,((b.mbytes/part.total::decimal)*100)::decimal(5,2)) as pct_of_total, 
 (case when a.rows = 0 then NULL else ((a.rows - pgc.reltuples)::decimal(19,3)/a.rows::decimal(19,3)*100)::decimal(5,2) end) as pct_stats_off,  
decode( det.n_sortkeys, 0, null , decode( a.rows,0,0, (a.unsorted_rows::decimal(32)/a.rows)*100) )::decimal(5,2) as pct_unsorted 
from ( select db_id, id, name, sum(rows) as rows, 
sum(rows)-sum(sorted_rows) as unsorted_rows from stv_tbl_perm a group by db_id, id, name ) as a 
join pg_class as pgc on pgc.oid = a.id
join pg_namespace as pgn on pgn.oid = pgc.relnamespace
left outer join (select tbl, count(*) as mbytes 
from stv_blocklist group by tbl) b on a.id=b.tbl
inner join ( SELECT   attrelid, min(case attisdistkey when  't' then attname else null end)  as "distkey",min(case attsortkeyord when 1 then attname  else null end ) as head_sort , max(attsortkeyord) as n_sortkeys, max(attencodingtype) as max_enc   FROM  pg_attribute group by 1) as det 
on det.attrelid = a.id
inner join ( select tbl, max(Mbytes)::decimal(32)/min(Mbytes) as ratio from
(select tbl, trim(name) as name, slice, count(*) as Mbytes
from svv_diskusage group by tbl, name, slice ) 
group by tbl, name ) as dist_ratio on a.id = dist_ratio.tbl
join ( select sum(capacity) as  total
  from stv_partitions where part_begin=0 ) as part on 1=1
where mbytes is not null
and pgc.relowner > 1 
-- and pgn.nspname = 'schema' -- schemaname
-- and a.name like 'table%' -- tablename
-- and det.max_enc = 0 -- non-compressed tables
order by  mbytes desc;

