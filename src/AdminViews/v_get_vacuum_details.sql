   
--DROP VIEW admin.v_get_vacuum_details;

/**********************************************************************************************
Purpose: View to get vacuum details like table name, Schema Name, Deleted Rows , processing time.
This view could be used to identify tables that are frequently deleted/ updated. 
History:
2015-07-01 srinikri Created
**********************************************************************************************/ 
CREATE OR REPLACE VIEW admin.v_get_vacuum_details
AS 
SELECT vac_start.userid,
       vac_start.xid,
       vac_start.table_id,
       tab.schema_name AS schema_name,
       tab.table_name AS table_name,
       vac_start.status start_status,
       vac_start. "rows" start_rows,
       vac_start. "blocks" start_blocks,
       vac_start. "eventtime" start_time,
	--vac_end.userid,
	--vac_end.xid,
	--vac_end.table_id,
       vac_end.status end_status,
       vac_end. "rows" end_rows,
       vac_end. "blocks" end_blocks,
       vac_end. "eventtime" end_time,
       (vac_start. "rows" - vac_end. "rows") AS rows_deleted,
       (vac_start. "blocks" - vac_end. "blocks") AS blocks_deleted_added,
       datediff(seconds,vac_start. "eventtime",vac_end. "eventtime") AS processing_seconds
FROM stl_vacuum vac_start
 LEFT JOIN stl_vacuum vac_end
    ON vac_start.userid = vac_end.userid
   AND vac_start.table_id = vac_end.table_id
   AND vac_start.xid = vac_end.xid
   AND vac_start.status = 'Started'
   AND vac_end.status = 'Finished'

  JOIN (SELECT DISTINCT TRIM(pgn.nspname) AS schema_name,
               name AS table_name,
               tbl.id AS table_id
        FROM stv_tbl_perm tbl
          JOIN pg_class pgc ON pgc.oid = tbl.id
          JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace) tab ON tab.table_id = vac_start.table_id
ORDER BY rows_deleted DESC;
