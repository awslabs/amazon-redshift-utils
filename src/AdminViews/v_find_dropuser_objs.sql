/**********************************************************************************************
Purpose:        View to help find all objects owned by the user to be dropped
Columns -


objtype:        Type of object user has privilege on. Object types are Function,Schema,
                Table or View, Database, Language or Default ACL
objowner:       Object owner 
userid:			Owner user id
schemaname:     Schema for the object
objname:        Name of the object

Notes:           
                
History:
2017-03-27 adedotua created
**********************************************************************************************/

CREATE OR REPLACE VIEW v_find_dropuser_objs as WITH
pgu as (select usesysid,usename from pg_user),
nc as (select oid,nspname from pg_namespace),
pproc as (select oid,proowner,pronamespace from pg_proc),
pgc as (select relnamespace,relname,relkind,relowner from pg_class)
select owner.objtype,owner.objowner,owner.userid,owner.schemaname,owner.objname from(
-- Functions owned by the user
select 'Function',pgu.usename,pgu.usesysid,nc.nspname,textin(regprocedureout(pproc.oid::regprocedure))
from 
pproc,pgu,nc 
where pproc.pronamespace=nc.oid and pproc.proowner=pgu.usesysid and pproc.proowner>1
UNION ALL
-- Schemas created by the user
select 'Database',pgu.usename,pgu.usesysid,null,pgd.datname from pg_database pgd,pgu where pgd.datdba=pgu.usesysid
UNION ALL
-- Schemas created by the user
select 'Schema',pgu.usename,pgu.usesysid,null,pgn.nspname from pg_namespace pgn,pgu where pgn.nspowner=pgu.usesysid
UNION ALL
-- Tables or Views owned by the user
select case pgc.relkind when 'r' then 'Table' when 'v' then 'View' end,pgu.usename,pgu.usesysid,nc.nspname,pgc.relname
from 
pgc,pgu,nc 
where pgc.relnamespace=nc.oid and pgc.relkind in ('r','v') and pgu.usesysid=pgc.relowner) owner("objtype","objowner","userid","schemaname","objname");
