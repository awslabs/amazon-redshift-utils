/**********************************************************************************************
Purpose: View to get the running queries in the Redshift
History:
2022-08-15 whrocha Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_get_running_queries AS
SELECT a.txn_owner,
       a.txn_db,
       a.xid,
       a.pid,
       a.txn_start,
       a.lock_mode,
       a.granted,
       b.pid                                                AS blocking_pid,
       (DATEDIFF(s, a.txn_start, GETDATE()) / 86400)        AS days,
       (DATEDIFF(s, a.txn_start, GETDATE()) % 86400 / 3600) AS hrs,
       (DATEDIFF(s, a.txn_start, GETDATE()) % 3600 / 60)    AS mins,
       (DATEDIFF(s, a.txn_start, GETDATE()) % 60)           AS secs
FROM svv_transactions a LEFT JOIN (SELECT pid, relation, granted FROM pg_locks GROUP BY 1, 2, 3) b ON a.relation = b.relation AND a.granted = 'f' AND b.granted = 't'
WHERE a.relation IS NOT NULL
ORDER BY txn_start
;
