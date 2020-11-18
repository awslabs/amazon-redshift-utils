/**********************************************************************************************
Purpose:        View to help find all objects owned by the user to be dropped
Columns -
objtype:        Type of object user has privilege on. Object types are Function,Schema,
                Table or View, Database, Language or Default ACL
objowner:       Object owner 
userid:         Owner user id
schemaname:     Schema for the object
objname:        Name of the object
ddl:            Generate DDL string to transfer object ownership to new user
Notes:           
                
History:
2017-03-27 adedotua created
2017-04-06 adedotua improvements
2018-01-06 adedotua added ddl column to generate ddl for transferring object ownership
2018-01-15 pvbouwel Add QUOTE_IDENT for identifiers
2018-05-29 adedotua added filter to skip temp tables
2018-08-03 alexlsts added table pg_library with custom message in ddl column

**********************************************************************************************/


CREATE OR REPLACE VIEW admin.v_find_dropuser_objs as 
SELECT owner.objtype,
       owner.objowner,
       owner.userid,
       owner.schemaname,
       owner.objname,
       owner.ddl
FROM (
-- Functions owned by the user
     SELECT 'Function',pgu.usename,pgu.usesysid,nc.nspname,textin (regprocedureout (pproc.oid::regprocedure)),
     'alter function ' || QUOTE_IDENT(nc.nspname) || '.' ||textin (regprocedureout (pproc.oid::regprocedure)) || ' owner to ' 
     FROM pg_proc pproc,pg_user pgu,pg_namespace nc
WHERE pproc.pronamespace = nc.oid
AND   pproc.proowner = pgu.usesysid
UNION ALL
-- Databases owned by the user
SELECT 'Database',
       pgu.usename,
       pgu.usesysid,
       NULL,
       pgd.datname,
       'alter database ' || QUOTE_IDENT(pgd.datname) || ' owner to '
FROM pg_database pgd,
     pg_user pgu
WHERE pgd.datdba = pgu.usesysid
UNION ALL
-- Schemas owned by the user
SELECT 'Schema',
       pgu.usename,
       pgu.usesysid,
       NULL,
       pgn.nspname,
       'alter schema '|| QUOTE_IDENT(pgn.nspname) ||' owner to '
FROM pg_namespace pgn,
     pg_user pgu
WHERE pgn.nspowner = pgu.usesysid
UNION ALL
-- Tables or Views owned by the user
SELECT decode(pgc.relkind,
             'r','Table',
             'v','View'
       ) ,
       pgu.usename,
       pgu.usesysid,
       nc.nspname,
       pgc.relname,
       'alter table ' || QUOTE_IDENT(nc.nspname) || '.' || QUOTE_IDENT(pgc.relname) || ' owner to '
FROM pg_class pgc,
     pg_user pgu,
     pg_namespace nc
WHERE pgc.relnamespace = nc.oid
AND   pgc.relkind IN ('r','v')
AND   pgu.usesysid = pgc.relowner
AND   nc.nspname NOT ILIKE 'pg\_temp\_%'
UNION ALL
-- Python libraries owned by the user
SELECT 'Library',
       pgu.usename,
       pgu.usesysid,
       '',
       pgl.name,
       'No DDL available for Python Library. You should DROP OR REPLACE the Python Library'
FROM  pg_library pgl,
      pg_user pgu
WHERE pgl.owner = pgu.usesysid) OWNER ("objtype","objowner","userid","schemaname","objname","ddl") 
WHERE owner.userid > 1;

