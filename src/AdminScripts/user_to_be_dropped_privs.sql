/**********************************************************************************************
Purpose:        Find all objects where the user is the grantor or grantee

Columns -
objtype:        Type of object user has privilege on. Object types are Function,Schema,
                Table or View, Database, Language or Default ACL
grantor:        User that granted the privilege
grantee:        User/Group the privilege is granted to
schemaname:     Schema for the object
objname:        Name of the object

Notes:          Create prepared statement. Run it i.e execute finduserprivs('<username>'). This
                will return all the privileges granted to or by the user. You can then revoke these 
                privileges
                
History:
2017-03-24 adedotua created
**********************************************************************************************/

prepare finduserprivs(varchar) as WITH 
u as (select usesysid,usename from pg_user where usename= $1),
sc as (select oid,nspname,nspacl from pg_namespace),
lang as (select lanname,lanacl from pg_language)
select privs.schemaname,privs.objname,privs.objtype,privs.grantor,privs.grantee from (
-- Functions granted to this user
select sc.nspname,textin(regprocedureout(c.oid::regprocedure)),'Function',g.usename,u.usename
from pg_proc c join sc on c.pronamespace=sc.oid,pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.proacl, makeaclitem(u.usesysid,0,g.usesysid,'EXECUTE',false))
    )
UNION ALL
-- Functions granted by this user
select sc.nspname,textin(regprocedureout(c.oid::regprocedure)),'Function',u.usename,g.usename
from pg_proc c join sc on c.pronamespace=sc.oid,pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.proacl, makeaclitem(g.usesysid,0,u.usesysid,'EXECUTE',false))
    )
UNION ALL
-- Functions granted by this user to a group
select sc.nspname,textin(regprocedureout(c.oid::regprocedure)),'Function',u.usename,g.groname
from pg_proc c join sc on c.pronamespace=sc.oid,pg_group g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.proacl, makeaclitem(0,g.grosysid,u.usesysid,'EXECUTE',false))
    )
UNION ALL
-- Language granted to this user
select null,c.lanname,'Language',g.usename,u.usename
from lang c, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.lanacl, makeaclitem(u.usesysid,0,g.usesysid,'USAGE',false))
    )
UNION ALL
-- Language granted by this user
select null,c.lanname,'Language',u.usename,g.usename
from lang c, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.lanacl, makeaclitem(g.usesysid,0,u.usesysid,'USAGE',false))
    )
UNION ALL
-- Language granted by this user to a group
select null,c.lanname,'Language',u.usename,g.groname
from lang c, pg_group g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.lanacl, makeaclitem(0,g.grosysid,u.usesysid,'USAGE',false))
    )
UNION ALL
--Tables granted to this user
select sc.nspname,c.relname,case c.relkind when 'r' then 'Table' when 'v' then 'View' end,g.usename,u.usename
from pg_class c join sc on c.relnamespace=sc.oid, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.relacl, makeaclitem(u.usesysid,0,g.usesysid,'SELECT',false))
            UNION ALL
            select 1 WHERE aclcontains(c.relacl, makeaclitem(u.usesysid,0,g.usesysid,'DELETE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.relacl, makeaclitem(u.usesysid,0,g.usesysid,'INSERT',false))
            UNION ALL
            select 1 WHERE aclcontains(c.relacl, makeaclitem(u.usesysid,0,g.usesysid,'UPDATE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.relacl, makeaclitem(u.usesysid,0,g.usesysid,'REFERENCES',false))
        )
UNION ALL
--Tables granted by this user
select sc.nspname,c.relname,case c.relkind when 'r' then 'Table' when 'v' then 'View' end,u.usename,g.usename
from pg_class c join sc on c.relnamespace=sc.oid, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.relacl, makeaclitem(g.usesysid,0,u.usesysid,'SELECT',false))
            UNION ALL
            select 1 WHERE aclcontains(c.relacl, makeaclitem(g.usesysid,0,u.usesysid,'DELETE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.relacl, makeaclitem(g.usesysid,0,u.usesysid,'INSERT',false))
            UNION ALL
            select 1 WHERE aclcontains(c.relacl, makeaclitem(g.usesysid,0,u.usesysid,'UPDATE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.relacl, makeaclitem(g.usesysid,0,u.usesysid,'REFERENCES',false))
        )
UNION ALL
--Tables granted by this user to a group
select sc.nspname,c.relname,case c.relkind when 'r' then 'Table' when 'v' then 'View' end,u.usename,g.groname
from pg_class c join sc on c.relnamespace=sc.oid, pg_group g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.relacl, makeaclitem(0,g.grosysid,u.usesysid,'SELECT',false))
            UNION ALL
            select 1 WHERE aclcontains(c.relacl, makeaclitem(0,g.grosysid,u.usesysid,'DELETE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.relacl, makeaclitem(0,g.grosysid,u.usesysid,'INSERT',false))
            UNION ALL
            select 1 WHERE aclcontains(c.relacl, makeaclitem(0,g.grosysid,u.usesysid,'UPDATE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.relacl, makeaclitem(0,g.grosysid,u.usesysid,'REFERENCES',false))
        )
UNION ALL
-- Schemas granted to this user
select null,c.nspname,'Schema',g.usename,u.usename
from sc c, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.nspacl, makeaclitem(u.usesysid,0,g.usesysid,'USAGE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.nspacl, makeaclitem(u.usesysid,0,g.usesysid,'CREATE',false))
    )
UNION ALL
-- Schemas granted by this user
select null,c.nspname,'Schema',u.usename,g.usename
from sc c, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.nspacl, makeaclitem(g.usesysid,0,u.usesysid,'USAGE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.nspacl, makeaclitem(g.usesysid,0,u.usesysid,'CREATE',false))
    )
UNION ALL
-- Schemas granted by this user to a group
select null,c.nspname,'Schema',u.usename,g.groname
from sc c, pg_group g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.nspacl, makeaclitem(0,g.grosysid,u.usesysid,'USAGE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.nspacl, makeaclitem(0,g.grosysid,u.usesysid,'CREATE',false))
    )
UNION ALL
-- Databases granted to this user
select null,c.datname,'Database',g.usename,u.usename
from pg_database c, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.datacl, makeaclitem(u.usesysid,0,g.usesysid,'CREATE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.datacl, makeaclitem(u.usesysid,0,g.usesysid,'TEMP',false))
    )
UNION ALL
-- Databases granted by this user
select null,c.datname,'Database',u.usename,g.usename
from pg_database c, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.datacl, makeaclitem(g.usesysid,0,u.usesysid,'CREATE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.datacl, makeaclitem(g.usesysid,0,u.usesysid,'TEMP',false))
    )
UNION ALL
-- Databases granted by this user to a group
select null,c.datname,'Database',u.usename,g.groname
from pg_database c, pg_group g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.datacl, makeaclitem(0,g.grosysid,u.usesysid,'CREATE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.datacl, makeaclitem(0,g.grosysid,u.usesysid,'TEMP',false))
    )
UNION ALL
--Default ACL granted to this user
select sc.nspname,case c.defaclobjtype when 'r' then 'Table/View' when 'f' then 'Function' end,'Default ACL',g.usename, u.usename
    from pg_default_acl c join sc on c.defaclnamespace=sc.oid, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(u.usesysid,0,g.usesysid,'SELECT',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(u.usesysid,0,g.usesysid,'DELETE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(u.usesysid,0,g.usesysid,'INSERT',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(u.usesysid,0,g.usesysid,'UPDATE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(u.usesysid,0,g.usesysid,'REFERENCES',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(u.usesysid,0,g.usesysid,'EXECUTE',false))) 
UNION ALL
--Default ACL granted by this user
select sc.nspname,case c.defaclobjtype when 'r' then 'Table/View' when 'f' then 'Function' end,'Default ACL',u.usename, g.usename
    from pg_default_acl c join sc on c.defaclnamespace=sc.oid, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(g.usesysid,0,u.usesysid,'SELECT',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(g.usesysid,0,u.usesysid,'DELETE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(g.usesysid,0,u.usesysid,'INSERT',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(g.usesysid,0,u.usesysid,'UPDATE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(g.usesysid,0,u.usesysid,'REFERENCES',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(g.usesysid,0,u.usesysid,'EXECUTE',false)))
UNION ALL
--Default ACL granted by this user to a group
select sc.nspname,case c.defaclobjtype when 'r' then 'Table/View' when 'f' then 'Function' end,'Default ACL',u.usename, g.groname
    from pg_default_acl c join sc on c.defaclnamespace=sc.oid, pg_group g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(0,g.grosysid,u.usesysid,'SELECT',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(0,g.grosysid,u.usesysid,'DELETE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(0,g.grosysid,u.usesysid,'INSERT',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(0,g.grosysid,u.usesysid,'UPDATE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(0,g.grosysid,u.usesysid,'REFERENCES',false))
            UNION ALL
            select 1 WHERE aclcontains(c.defaclacl, makeaclitem(0,g.grosysid,u.usesysid,'EXECUTE',false))))
privs("schemaname","objname","objtype","grantor","grantee");