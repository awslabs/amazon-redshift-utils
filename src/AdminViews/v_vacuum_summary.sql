/**********************************************************************************************
Purpose:      View to flatten stl_vacuum table and provide details like vacuum start and 
              end times, current status, changed rows and freed blocks all in one row
              
Current Version:        1.04

History:
Version 1.01
        2017-12-24 adedotua created
Version 1.02
        2018-12-22 adedotua updated view to account for background auto vacuum process 
Version 1.03
        2018-12-30 adedotua fixed join condition to make vacuum on dropped tables visible
Version 1.04
        2019-04-30 adedotua added is_auto_vacuum flag to indicate whether vacuum was auto vacuum 
Version 1.05
        2019-10-09 adedotua set vac_end_status as null for autovacuum if end status is unknown
**********************************************************************************************/ 

CREATE OR REPLACE VIEW admin.v_vacuum_summary as SELECT a.userid,
       a.xid,
       BTRIM(d.datname) AS database_name,
       a.table_id,
       BTRIM(c.name) AS tablename,
       BTRIM(a.status) AS vac_start_status,
       CASE
         WHEN (a.status ilike 'skipped%'::TEXT) THEN null
         WHEN (f.xid IS NOT NULL) THEN 'Running'::TEXT
         WHEN (b.status IS NULL) and a.status not ilike '%[VacuumBG]%' THEN 'Failed'::TEXT
         ELSE BTRIM((b.status)::TEXT)
       END AS vac_end_status,
       a.eventtime AS vac_start_time,
       b.eventtime AS vac_end_time,
       CASE 
       WHEN f.xid IS NOT NULL THEN datediff(s,a.eventtime,getdate())
       WHEN b.eventtime IS NOT NULL THEN datediff(s,a.eventtime,b.eventtime)
       ELSE NULL
       END AS vac_duration_secs,
       a."rows" AS vac_start_rows,
       b."rows" AS vac_end_rows,
       a."rows" - b."rows" AS vac_deleted_rows,
       a.sortedrows AS vac_start_sorted_rows,
       b.sortedrows AS vac_end_sorted_rows,
       a."blocks" AS vac_start_blocks,
       b."blocks" AS vac_end_blocks,
       (b."blocks" - a."blocks") AS vac_block_diff,
       NVL(e.empty_blk_cnt,0) AS empty_blk_cnt,
       case when a.status ilike '%[VacuumBG]%' then true else false end is_auto_vacuum
       FROM 
       (SELECT * FROM stl_vacuum WHERE status not ilike '%Finished%') a 
       LEFT JOIN stl_vacuum b on a.xid=b.xid and a.table_id=b.table_id and a.eventtime < b.eventtime 
       LEFT JOIN (SELECT id,name,db_id FROM stv_tbl_perm WHERE slice = 0) c ON a.table_id = c.id
       LEFT JOIN pg_database d ON c.db_id::oid = d.oid
       LEFT JOIN (SELECT tbl,COUNT(*) AS empty_blk_cnt FROM stv_blocklist WHERE num_values = 0 GROUP BY tbl) e ON a.table_id = e.tbl 
       LEFT JOIN (SELECT xid FROM svv_transactions WHERE lockable_object_type = 'transactionid') f on a.xid=f.xid
ORDER BY a.xid;
