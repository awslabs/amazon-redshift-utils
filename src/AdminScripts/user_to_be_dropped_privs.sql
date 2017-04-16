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
2017-04-06 adedotua combined grantee,grantor and added a select to find empty default acls. 
2017-04-06 adedotua significant reduction in statement from 197 to 77 lines. 
**********************************************************************************************/

prepare find_drop_userprivs(varchar) as WITH 
grantor as (select usesysid,usename from pg_user),
schemas as (select oid,nspname,nspacl,nspowner from pg_namespace),
grantee as ((SELECT pg_user.usesysid as usesysid, 0 as grosysid, pg_user.usename as usename FROM pg_user
UNION ALL 
SELECT 0 as usesysid, pg_group.grosysid as grosysid, pg_group.groname as usename FROM pg_group)
UNION ALL 
SELECT 0 as usesysid, 0 as grosysid, 'PUBLIC'::name as usename)
select privs.schemaname,privs.objname,privs.objtype,privs.objowner,privs.grantor,privs.grantee from (
-- Functions grants
select sc.nspname,textin(regprocedureout(c.oid::regprocedure)),'Function',pg_get_userbyid(c.proowner),g.usename,u.usename
from pg_proc c join schemas sc on c.pronamespace=sc.oid,grantor g,grantee u
where EXISTS (select 1 WHERE aclcontains(c.proacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'EXECUTE',false)))
UNION ALL
-- Language grants
select null,c.lanname,'Language',null,g.usename,u.usename
from pg_language c,grantor g,grantee u
where EXISTS (select 1 WHERE aclcontains(c.lanacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'USAGE',false)))
UNION ALL 
--Tables grants
select sc.nspname,c.relname,case c.relkind when 'r' then 'Table' when 'v' then 'View' end,pg_get_userbyid(c.relowner),g.usename,u.usename
from pg_class c join schemas sc on c.relnamespace=sc.oid, grantor g,grantee u
where EXISTS (
select 1 WHERE aclcontains(c.relacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'SELECT',false))
UNION ALL
select 1 WHERE aclcontains(c.relacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'DELETE',false))
UNION ALL
select 1 WHERE aclcontains(c.relacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'INSERT',false))
UNION ALL
select 1 WHERE aclcontains(c.relacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'UPDATE',false))
UNION ALL
select 1 WHERE aclcontains(c.relacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'REFERENCES',false)))
UNION ALL
-- Schema grants
select null,c.nspname,'Schema',pg_get_userbyid(c.nspowner),g.usename,u.usename
from pg_namespace c, grantor g,grantee u
where EXISTS (
select 1 WHERE aclcontains(c.nspacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'USAGE',false))
UNION ALL
select 1 WHERE aclcontains(c.nspacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'CREATE',false)))
UNION ALL
-- Database grants
select null,c.datname,'Database',pg_get_userbyid(c.datdba),g.usename,u.usename
from pg_database c, grantor g,grantee u
where EXISTS (
select 1 WHERE aclcontains(c.datacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'CREATE',false))
UNION ALL
select 1 WHERE aclcontains(c.datacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'TEMP',false)))
UNION ALL
--Default ACL grants
select sc.nspname,decode(c.defaclobjtype,'r','Tables','f','Functions'),
'Default ACL '||decode(c.defaclnamespace,0,'User','Schema'),pg_get_userbyid(c.defacluser),g.usename, u.usename
from pg_default_acl c left join schemas sc on c.defaclnamespace=sc.oid, grantor g,grantee u
where EXISTS (
select 1 WHERE aclcontains(c.defaclacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'SELECT',false))
UNION ALL
select 1 WHERE aclcontains(c.defaclacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'DELETE',false))
UNION ALL
select 1 WHERE aclcontains(c.defaclacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'INSERT',false))
UNION ALL
select 1 WHERE aclcontains(c.defaclacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'UPDATE',false))
UNION ALL
select 1 WHERE aclcontains(c.defaclacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'REFERENCES',false))
UNION ALL
select 1 WHERE aclcontains(c.defaclacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'EXECUTE',false))) 
UNION ALL
--Default ACL grants with empty acl
select sc.nspname,decode(c.defaclobjtype,'r','Tables','f','Functions'),
'Default ACL '||decode(c.defaclnamespace,0,'User','Schema'),pg_get_userbyid(c.defacluser),null,
decode(c.defaclobjtype,'r','Regrant privileges on tables to owner','f','Regrant privileges on Functions to owner and PUBLIC')
from pg_default_acl c left join schemas sc on c.defaclnamespace=sc.oid
where EXISTS (select 1 where defaclacl='{}'::aclitem[])
) privs("schemaname","objname","objtype","objowner","grantor","grantee") where privs.grantor = $1 or privs.grantee = $1 or privs.objowner = $1;
