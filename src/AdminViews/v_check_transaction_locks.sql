--DROP VIEW admin.v_check_transaction_locks;
/**********************************************************************************************
Purpose: View to get information about the locks held by open transactions 
History:
2015-07-01 srinikri Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_check_transaction_locks
AS
SELECT sysdate AS system_ts,
TRIM(n.nspname) schemaname,
TRIM(c.relname) tablename,
TRIM(l.database) databasename,
l.transaction ,
l.pid,
a.usename,
l.mode,
l.granted
FROM pg_catalog.pg_locks l
JOIN pg_catalog.pg_class c ON c.oid = l.relation
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_stat_activity a ON a.procpid = l.pid
;