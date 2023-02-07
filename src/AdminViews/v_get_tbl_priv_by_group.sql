/**********************************************************************************************
Purpose: View to get the tables that a user group has access to
History:
2021-09-27 milindo Created
2022-08-15 saeedma8 excluded system tables
**********************************************************************************************/
create or replace view admin.v_get_tbl_priv_by_group as
select
    t.namespace as schemaname, t.item as object, pu.groname as groupname
  , decode(charindex('r',split_part(split_part(array_to_string(t.relacl, '|'),pu.groname,2 ) ,'/',1)),0,false,true)  as sel
  , decode(charindex('w',split_part(split_part(array_to_string(t.relacl, '|'),pu.groname,2 ) ,'/',1)),0,false,true)  as upd
  , decode(charindex('a',split_part(split_part(array_to_string(t.relacl, '|'),pu.groname,2 ) ,'/',1)),0,false,true)  as ins
  , decode(charindex('d',split_part(split_part(array_to_string(t.relacl, '|'),pu.groname,2 ) ,'/',1)),0,false,true)  as del
  , decode(charindex('D',split_part(split_part(array_to_string(t.relacl, '|'),pu.groname,2 ) ,'/',1)),0,false,true)  as drp
  , decode(charindex('R',split_part(split_part(array_to_string(t.relacl, '|'),pu.groname,2 ) ,'/',1)),0,false,true)  as ref
from
      (select
            use.usename as subject,
            nsp.nspname as namespace,
            c.relname as item,
            c.relkind as type,
            use2.usename as owner,
            c.relacl
      from
            pg_user use
      cross join pg_class c
      left join pg_namespace nsp on (c.relnamespace = nsp.oid)
      left join pg_user use2 on (c.relowner = use2.usesysid)
      where c.relowner = use.usesysid
      and nsp.nspname !~ '^information_schema|catalog_history|pg_'
      ) t
join pg_group pu on array_to_string(t.relacl, '|') like '%'||pu.groname||'%'
;