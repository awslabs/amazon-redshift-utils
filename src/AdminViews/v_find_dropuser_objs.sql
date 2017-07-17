/**********************************************************************************************
Purpose:        View to help find all objects owned by the user to be dropped
Columns -


objtype:        Type of object user has privilege on. Object types are Function,Schema,
                Table or View, Database, Language or Default ACL
objowner:       Object owner 
userid:			    Owner user id
schemaname:     Schema for the object
objname:        Name of the object

Notes:           
                
History:
2017-03-27 adedotua created
2017-04-06 adedotua improvements
**********************************************************************************************/

CREATE OR REPLACE VIEW admin.v_find_dropuser_objs as
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
where owner.userid>1;
