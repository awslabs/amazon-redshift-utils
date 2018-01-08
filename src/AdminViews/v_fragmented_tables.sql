/**********************************************************************************************
Purpose:      View to list all fragmented tables in the database. Tables can become fragmented
              due to frequent vacuums overlapping with concurrent writes on the same table.

History:
2017-12-29    adedotua and indubh created
**********************************************************************************************/ 

CREATE VIEW admin.v_fragmented_tables as
select tbl,
trim(datname) as dbname,
trim("name") as tablename,
temp,
backup 
from (select tbl,floor(log(num_values)) from stv_blocklist where col=0 and num_values>0 and tbl > 1 group by 1,2) a 
join (select db_id,"name",id,temp,backup from stv_tbl_perm where slice=0) b on a.tbl=b.id 
join pg_database c on b.db_id=c.oid group by 1,2,3,4,5 having count(*) > 2 order by count(*) desc;
