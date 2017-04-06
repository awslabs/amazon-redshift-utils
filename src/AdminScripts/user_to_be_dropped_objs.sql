/**********************************************************************************************
Purpose: 		Find all objects owned by the user to be dropped

Columns -
objtype:		Type of object owned by the user. Object types are Function,Schema,Table or View
objowner:		Object owner
userid:			Object owner user id
schemaname:		Schema for the user object
objname:		Name of the object

Notes:			Create prepared statement. Run it i.e execute find_drop_userobjs('<username>'). This
				will return all the objects owned by the user. You can then alter the object owner
				or drop the object

History:
2017-03-24 adedotua created
2017-03-27 adedotua updated prepared statement name
2017-04-06 adedotua improvements
**********************************************************************************************/

prepare find_drop_userobjs(varchar) as 
select owner.objtype,owner.objowner,owner.userid,owner.schemaname,owner.objname from(
-- Functions owned by the user
select 'Function',pgu.usename,pgu.usesysid,nc.nspname,textin(regprocedureout(pproc.oid::regprocedure))
from 
pg_proc pproc,pg_user pgu,pg_namespace nc 
where pproc.pronamespace=nc.oid and pproc.proowner=pgu.usesysid
UNION ALL
-- Databases owned by the user
select 'Database',pgu.usename,pgu.usesysid,null,pgd.datname from pg_database pgd,pg_user pgu where pgd.datdba=pgu.usesysid
UNION ALL
-- Schemas owned by the user
select 'Schema',pgu.usename,pgu.usesysid,null,pgn.nspname from pg_namespace pgn,pg_user pgu where pgn.nspowner=pgu.usesysid
UNION ALL
-- Tables or Views owned by the user
select decode(pgc.relkind,'r','Table','v','View'),pgu.usename,pgu.usesysid,nc.nspname,pgc.relname
from
pg_class pgc,pg_user pgu,pg_namespace nc 
where pgc.relnamespace=nc.oid and pgc.relkind in ('r','v') and pgu.usesysid=pgc.relowner) owner("objtype","objowner","userid","schemaname","objname")
where owner.objowner = $1;
