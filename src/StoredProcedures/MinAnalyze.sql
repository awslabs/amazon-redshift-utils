/**********************************************************************************************
Purpose: Analyze one column of a table. To be used right after a load
		 It will analyze the first column of the SK, the DK or the first column of the table
Parameters:
  pSchema : Schema
  pTable : Table
  pPercent : Percent Threshold for analyze
  pWait : Wait for table locks
USAGE:
  call minanalyze('public','mytable');
  call minanalyze('public','mytable', 1, True);
History:
2019-06-12 ericfe Created
**********************************************************************************************/
CREATE OR REPLACE PROCEDURE MinAnalyze(pSchema varchar, pTable varchar, pPercent int, pWait boolean) AS $$
DECLARE
myROW record;
mySQL record;
vSChema varchar;
vPercent varchar;
BEGIN
-- DEFAULT to public schema
if pSchema is null then
  vSchema := 'public';
else
  vSchema := pSchema;
end if;
-- DEFAULT to 1 percet
if pPercent is null then
  vPercent := 'set analyze_threshold_percent to 1';
else
  vPercent := 'set analyze_threshold_percent to ' || pPercent::varchar;
end if;
-- GET sql
select into mySQL  'analyze ' || nspname || '.' || relname || ' (' || nvl( nvl( a1.attname, a2.attname), a.attname ) || ');' as sql
from pg_namespace n join pg_class c on n.oid = c.relnamespace
join pg_attribute a on  a.attrelid = c.oid and a.attnum = 1
left join pg_attribute a1 on  a1.attrelid = c.oid and a1.attsortkeyord = 1
left join pg_attribute a2 on  a2.attrelid = c.oid and a2.attisdistkey = 't'
where  c.relname = lower(pTable)::char(128)
and n.nspname = lower(vSchema)::char(128)
and c.relkind = 'r';
if FOUND then
  -- BODY  
  SELECT INTO myROW  svv.xid
  ,      l.pid
  ,      svv.txn_owner as username
  ,      l.mode
  ,      l.granted
  FROM   pg_catalog.pg_locks l
  INNER JOIN pg_catalog.svv_transactions svv
    ON   l.pid = svv.pid
  AND   l.relation = svv.relation
  AND   svv.lockable_object_type is not null
  LEFT JOIN pg_catalog.pg_class c on c.oid = svv.relation
  LEFT JOIN pg_namespace nsp
    ON   nsp.oid = c.relnamespace
  LEFT JOIN pg_catalog.pg_database d on d.oid = l.database
  LEFT OUTER JOIN stv_recents rct
    ON   rct.pid = l.pid
  WHERE  l.pid <> pg_backend_pid()
   AND   l.granted = true
   AND   nsp.nspname = vSchema::char(128)
   AND   c.relname = pTable::char(128);
  if FOUND then
    if pWait then
      RAISE NOTICE 'User % has the table locked in % mode. Use ''select pg_terminate_backend(%);'' on another session to kill or wait', myRow.username, myRow.mode, myRow.pid;
       EXECUTE vPercent;
       EXECUTE mySQL.sql;
     else
       RAISE NOTICE 'User % has the table locked in % mode. Use ''select pg_terminate_backend(%);'' to kill session or try again', myRow.username, myRow.mode, myRow.pid;
       RAISE exception 'QUIT!';
     end if;
  else
    RAISE NOTICE 'No lock found on table %.%. Running: %', vSchema, pTable, mySQL.sql;
    EXECUTE vPercent;
    EXECUTE mySQL.sql;
  end if;
else
  RAISE EXCEPTION 'No table found';
end if;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE MinAnalyze(pSchema varchar, pTable varchar) AS $$
  BEGIN
  -- Will wait by default
    CALL MinAnalyze(pSchema, pTable, 0, true);
  END;
$$ LANGUAGE plpgsql;
