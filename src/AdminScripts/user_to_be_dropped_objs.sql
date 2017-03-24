/**********************************************************************************************
Purpose: 		Find all objects owned by the user to be dropped

Columns -
objtype:		Type of object owned by the user. Object types are Function,Schema,Table or View
objowner:		Object owner
userid:			Object owner user id
schemaname:		Schema for the user object
objname:		Name of the object

Notes:			Create prepared statement. Run it i.e execute finduserobjs('<username>'). This
				will return all the objects owned by the user. You can then alter the object owner
				or drop the object

History:
2017-03-24 adedotua created
**********************************************************************************************/

prepare finduserobjs(varchar) as WITH
pgu as (select usesysid,usename from pg_user where usename=$1),
nc as (select oid,nspname from pg_namespace),
pproc as (select oid,proowner,pronamespace from pg_proc),
pgc as (select relnamespace,relname,relkind,relowner from pg_class)
select owner.objtype,owner.objowner,owner.userid,owner.schemaname,owner.objname from(
select 'Function',pgu.usename,pgu.usesysid,nc.nspname,textin(regprocedureout(pproc.oid::regprocedure))
from 
pproc,pgu,nc 
where pproc.pronamespace=nc.oid and pproc.proowner=pgu.usesysid and pproc.proowner>1
UNION ALL
select 'Schema',pgu.usename,pgu.usesysid,null,pgn.nspname from pg_namespace pgn,pgu where pgn.nspowner=pgu.usesysid
UNION ALL
select 
case pgc.relkind
when 'r' then 'Table'
when 'v' then 'View'
end,
pgu.usename,pgu.usesysid,nc.nspname,pgc.relname
from
pgc,pgu,nc 
where pgc.relnamespace=nc.oid and pgc.relkind in ('r','v') and pgu.usesysid=pgc.relowner) owner("objtype","objowner","userid","schemaname","objname");