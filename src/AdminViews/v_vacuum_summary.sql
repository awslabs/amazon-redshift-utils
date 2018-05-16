/**********************************************************************************************
Purpose:      View to flatten stl_vacuum table and provide details like vacuum start and 
              end times, current status, changed rows and freed blocks all in one row

History:
2017-12-24 adedotua created
**********************************************************************************************/ 

CREATE OR REPLACE VIEW admin.v_vacuum_summary as SELECT a.userid,
       a.xid,
       BTRIM((d.datname)::TEXT) AS database_name,
       a.table_id,
       BTRIM((c.name)::TEXT) AS tablename,
       BTRIM((a.status)::TEXT) AS vac_start_status,
       CASE
         WHEN (a.status ilike 'skipped%'::TEXT) THEN 'Skipped'::TEXT
         WHEN (f.xid IS NOT NULL) THEN 'Running'::TEXT
         WHEN (b.status IS NULL) THEN 'Failed'::TEXT
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
       COALESCE(e.empty_blk_cnt,(0)::BIGINT,e.empty_blk_cnt) AS empty_blk_cnt
       FROM ((((((SELECT stl_vacuum.userid,
                  stl_vacuum.xid,
                  stl_vacuum.table_id,
                  stl_vacuum.status,
                  stl_vacuum."rows",
                  stl_vacuum.sortedrows,
                  stl_vacuum."blocks",
                  stl_vacuum.max_merge_partitions,
                  stl_vacuum.eventtime
           FROM stl_vacuum
           WHERE (stl_vacuum.status <> 'Finished'::bpchar)) a 
           LEFT JOIN (SELECT stl_vacuum.userid,
           stl_vacuum.xid,
           stl_vacuum.table_id,
           stl_vacuum.status,
           stl_vacuum."rows",
           stl_vacuum.sortedrows,
           stl_vacuum."blocks",
           stl_vacuum.max_merge_partitions,
           stl_vacuum.eventtime
              FROM stl_vacuum
              WHERE (stl_vacuum.status = 'Finished'::bpchar)) b USING (xid)) 
              LEFT JOIN (SELECT stv_tbl_perm.id,stv_tbl_perm.name,stv_tbl_perm.db_id
                  FROM stv_tbl_perm WHERE (stv_tbl_perm.slice = 0)) c ON ((a.table_id = c.id))) 
              JOIN pg_database d ON (((c.db_id)::OID = d.oid))) 
                  LEFT JOIN (SELECT stv_blocklist.tbl,COUNT(*) AS empty_blk_cnt
                  FROM stv_blocklist
                  WHERE (stv_blocklist.num_values = 0) GROUP BY stv_blocklist.tbl) e ON ((a.table_id = e.tbl))) 
             LEFT JOIN (SELECT svv_transactions.xid
              FROM svv_transactions
              WHERE ((svv_transactions.lockable_object_type)::TEXT = 'transactionid'::TEXT)) f USING (xid))
ORDER BY a.xid;
