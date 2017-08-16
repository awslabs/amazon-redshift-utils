--DROP VIEW admin.v_get_blocking_locks;
/**********************************************************************************************
Purpose: View to identify blocking locks as well as determine what/who is blocking a query 

History:
2017-08-16 dbiddle Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_get_blocking_locks
AS
WITH locks AS (
       SELECT svv.xid
       ,      l.pid
       ,      svv.txn_owner as username
       ,      TRIM(d.datname) as dbname
       ,      svv.relation
       ,      TRIM(nsp.nspname) as schemaname
       ,      TRIM(c.relname) as objectname
       ,      l.mode
       ,      l.granted
       ,      svv.lockable_object_type as obj_type
       ,      svv.txn_start
       ,      ROUND((EXTRACT(EPOCH FROM current_timestamp) - EXTRACT(EPOCH FROM svv.txn_start)),2) as block_sec
       ,      ROUND((EXTRACT(EPOCH FROM current_timestamp) - EXTRACT(EPOCH FROM svv.txn_start))/60,2) as block_min
       ,      ROUND((EXTRACT(EPOCH FROM current_timestamp) - EXTRACT(EPOCH FROM svv.txn_start))/60/60,2) as block_hr
       ,      CASE WHEN l.granted is false THEN ROUND((EXTRACT(EPOCH FROM current_timestamp) - EXTRACT(EPOCH FROM rct.starttime)),2) ELSE NULL END as waiting
       FROM   pg_catalog.pg_locks l
       INNER JOIN pg_catalog.svv_transactions svv
        ON    l.pid = svv.pid
       AND    l.relation = svv.relation
       AND    svv.lockable_object_type is not null
       LEFT JOIN pg_catalog.pg_class c on c.oid = svv.relation
       LEFT JOIN pg_namespace nsp
        ON    nsp.oid = c.relnamespace
       LEFT JOIN pg_catalog.pg_database d on d.oid = l.database
       LEFT OUTER JOIN stv_recents rct
        ON    rct.pid = l.pid
       WHERE  l.pid <> pg_backend_pid()
)
select distinct * 
FROM  (
       SELECT l.xid
       ,      l.pid
       ,      l.username
       ,      l.dbname
       ,      l.relation
       ,      l.schemaname
       ,      l.objectname
       ,      l.mode
       ,      DECODE(l.granted, true, 'True', false, 'False') granted
       ,      l.obj_type
       ,      l.txn_start
       ,      DECODE(l.granted, true, l.block_sec, NULL) as block_sec
       ,      DECODE(l.granted, true, l.block_min, NULL) as block_min
       ,      DECODE(l.granted, true, l.block_hr, NULL) as block_hr
       ,      waiting
       ,      b.max_sec_blocking
       ,      b.num_blocking
       ,      b.pidlist
       FROM   locks l
       LEFT OUTER JOIN (
              SELECT relation
              ,      mode
              ,      listagg(b.pid, ',') as pidlist
              ,      MIN(block_sec) as min_sec_blocking
              ,      MAX(waiting) as max_sec_blocking
              ,      COUNT(*) as num_blocking
              FROM   locks b
              WHERE  granted is false
              GROUP BY relation
              ,      mode
       ) b
        ON    l.relation = b.relation
       AND    l.granted is true
       AND   (l.mode like '%Exclusive%'
       OR   (l.mode like '%Share%' AND b.mode LIKE '%ExclusiveLock' AND b.mode NOT LIKE '%Share%'))
)
ORDER BY granted DESC
,      max_sec_blocking desc nulls last
,      block_sec DESC
,      waiting desc nulls last
;
