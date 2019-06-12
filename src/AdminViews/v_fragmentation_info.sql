/**********************************************************************************************
Purpose:      View to list all fragmented tables in the database. Tables can become fragmented
              due to frequent vacuums overlapping with concurrent writes on the same table.
Columns:      tbl - id of the table
              tablename -  name of the table
              dbname - database that contains the table
              est_space_gain - estimated number of blocks that will be released if table is 
                               defragmented via vacuum or deep copy
                               
History:
2017-12-29    adedotua and indubh created
2018-02-06    adedotua refactored the script to use rowid column for estimation
**********************************************************************************************/ 

CREATE OR REPLACE VIEW admin.v_fragmentation_info
AS 
select tbl,tablename,dbname,sum(t_excess_blks) est_space_gain 
from 
(
  select tbl,col,node,tablename,trim(datname) as dbname,sum(excess_blks)*(col+1) as t_excess_blks 
  from 
  (select tbl,slice,col,count(*) total_blks from stv_blocklist where num_values > 0 group by 1,2,3) a
  join (select tbl,slice,max(col) as col from stv_blocklist group by 1,2) b using (tbl,slice,col)
  join (select tbl,slice,col,count(*) - ceil(sum(num_values)/130994.0) as excess_blks from stv_blocklist 
      where num_values > 0 and num_values < 130994 group by 1,2,3) c using (tbl,slice,col)
  join stv_slices d using (slice) 
  join (select id,trim("name") as tablename,db_id from stv_tbl_perm where slice=0) f on b.tbl=f.id
  join pg_database g on f.db_id=g.oid
  where excess_blks > 1
  group by 1,2,3,4,5
)
where tbl > 1 
and t_excess_blks > (select case when sum(capacity) > 200000 then 1024 else 102.4 end from stv_partitions 
                                   where host=owner and host=0 group by host)
 group by 1,2,3
order by 4 desc;
