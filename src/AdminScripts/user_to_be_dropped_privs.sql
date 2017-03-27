/**********************************************************************************************
Purpose:        Find all objects where the user is the grantor or grantee

Columns -
schemaname:     Schema for the object
objname:        Name of the object
objtype:        Type of object user has privilege on. Object types are Function,Schema,
                Table or View, Database, Language or Default ACL
objowner:       Object owner 
grantor:        User that granted the privilege
grantee:        User/Group the privilege is granted to


Notes:          Create prepared statement. Run it i.e execute find_drop_userprivs('<username>'). This
                will return all the privileges granted to or by the user. You can then revoke these 
                privileges
                
History:
2017-03-24 adedotua created
2017-03-27 adedotua updated prepared statement name
**********************************************************************************************/

prepare find_drop_userprivs(varchar) as WITH 
u as (select usesysid,usename from pg_user where usename= $1),
sc as (select oid,nspname,nspacl,nspowner from pg_namespace),
lang as (select lanname,lanacl from pg_language)
select privs.schemaname,privs.objname,privs.objtype,privs.objowner,privs.grantor,privs.grantee from (
-- Functions granted to this user
select sc.nspname,textin(regprocedureout(c.oid::regprocedure)),'Function',pg_get_userbyid(c.proowner),g.usename,u.usename
from pg_proc c join sc on c.pronamespace=sc.oid,pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.proacl, makeaclitem(u.usesysid,0,g.usesysid,'EXECUTE',false))
    )
UNION ALL
-- Functions granted by this user
select sc.nspname,textin(regprocedureout(c.oid::regprocedure)),'Function',pg_get_userbyid(c.proowner),u.usename,g.usename
from pg_proc c join sc on c.pronamespace=sc.oid,pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.proacl, makeaclitem(g.usesysid,0,u.usesysid,'EXECUTE',false))
    )
UNION ALL
-- Functions granted by this user to a group
select sc.nspname,textin(regprocedureout(c.oid::regprocedure)),'Function',pg_get_userbyid(c.proowner),u.usename,g.groname
from pg_proc c join sc on c.pronamespace=sc.oid,pg_group g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.proacl, makeaclitem(0,g.grosysid,u.usesysid,'EXECUTE',false))
    )
UNION ALL
-- Language granted to this user
select null,c.lanname,'Language',null,g.usename,u.usename
from lang c, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.lanacl, makeaclitem(u.usesysid,0,g.usesysid,'USAGE',false))
    )
UNION ALL
-- Language granted by this user
select null,c.lanname,'Language',null,u.usename,g.usename
from lang c, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.lanacl, makeaclitem(g.usesysid,0,u.usesysid,'USAGE',false))
    )
UNION ALL
-- Language granted by this user to a group
select null,c.lanname,'Language',null,u.usename,g.groname
from lang c, pg_group g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.lanacl, makeaclitem(0,g.grosysid,u.usesysid,'USAGE',false))
    )
UNION ALL
--Tables granted to this user
select sc.nspname,c.relname,case c.relkind when 'r' then 'Table' when 'v' then 'View' end,pg_get_userbyid(c.relowner),g.usename,u.usename
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
select sc.nspname,c.relname,case c.relkind when 'r' then 'Table' when 'v' then 'View' end,pg_get_userbyid(c.relowner),u.usename,g.usename
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
select sc.nspname,c.relname,case c.relkind when 'r' then 'Table' when 'v' then 'View' end,pg_get_userbyid(c.relowner),u.usename,g.groname
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
select null,c.nspname,'Schema',pg_get_userbyid(c.nspowner),g.usename,u.usename
from sc c, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.nspacl, makeaclitem(u.usesysid,0,g.usesysid,'USAGE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.nspacl, makeaclitem(u.usesysid,0,g.usesysid,'CREATE',false))
    )
UNION ALL
-- Schemas granted by this user
select null,c.nspname,'Schema',pg_get_userbyid(c.nspowner),u.usename,g.usename
from sc c, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.nspacl, makeaclitem(g.usesysid,0,u.usesysid,'USAGE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.nspacl, makeaclitem(g.usesysid,0,u.usesysid,'CREATE',false))
    )
UNION ALL
-- Schemas granted by this user to a group
select null,c.nspname,'Schema',pg_get_userbyid(c.nspowner),u.usename,g.groname
from sc c, pg_group g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.nspacl, makeaclitem(0,g.grosysid,u.usesysid,'USAGE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.nspacl, makeaclitem(0,g.grosysid,u.usesysid,'CREATE',false))
    )
UNION ALL
-- Databases granted to this user
select null,c.datname,'Database',pg_get_userbyid(c.datdba),g.usename,u.usename
from pg_database c, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.datacl, makeaclitem(u.usesysid,0,g.usesysid,'CREATE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.datacl, makeaclitem(u.usesysid,0,g.usesysid,'TEMP',false))
    )
UNION ALL
-- Databases granted by this user
select null,c.datname,'Database',pg_get_userbyid(c.datdba),u.usename,g.usename
from pg_database c, pg_user g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.datacl, makeaclitem(g.usesysid,0,u.usesysid,'CREATE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.datacl, makeaclitem(g.usesysid,0,u.usesysid,'TEMP',false))
    )
UNION ALL
-- Databases granted by this user to a group
select null,c.datname,'Database',pg_get_userbyid(c.datdba),u.usename,g.groname
from pg_database c, pg_group g,u
    where EXISTS (
            select 1 WHERE aclcontains(c.datacl, makeaclitem(0,g.grosysid,u.usesysid,'CREATE',false))
            UNION ALL
            select 1 WHERE aclcontains(c.datacl, makeaclitem(0,g.grosysid,u.usesysid,'TEMP',false))
    )
UNION ALL
--Default ACL granted to this user
select sc.nspname,case c.defaclobjtype when 'r' then 'Table/View' when 'f' then 'Function' end,'Default ACL',null,g.usename, u.usename
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
select sc.nspname,case c.defaclobjtype when 'r' then 'Table/View' when 'f' then 'Function' end,'Default ACL',null,u.usename, g.usename
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
select sc.nspname,case c.defaclobjtype when 'r' then 'Table/View' when 'f' then 'Function' end,'Default ACL',null,u.usename, g.groname
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
privs("schemaname","objname","objtype","objowner","grantor","grantee");
