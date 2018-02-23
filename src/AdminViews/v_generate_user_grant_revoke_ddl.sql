/*************************************************************************************************************************
Purpose:      View to generate grant or revoke ddl for users and groups. This is useful for 
              recreating users or group privileges or for revoking privileges before dropping 
              a user or group
              
Columns -
objowner:     Object owner 
schemaname:   Object schema if applicable
objname:      Name of the object the privilege is granted on
grantor:      User that granted the privilege
username:     User/Group the privilege is granted to
objtype:      Type of object user has privilege on. Object types are Function,Schema,
              Table or View, Database, Language or Default ACL
ddltype:      Type of ddl generated i.e grant or revoke
sequence:     Sequence number to order the DDLs by hierarchy
ddl:          DDL text
Notes:           
                
History:
2017-07-11    adedotua created
2017-07-17    adedotua added comments to account for difference between grantor and owner of 
              object. Also added schemaname and objname columns.
2017-12-17    adedotua added sequence column to order grant or revoke DDLs in order of heirarchy.
2017-12-17    adedotua added schema name admin
2017-12-18    adedotua added case statement for revoke DDLs to account for situations where the grantor is not object owner 
2018-01-15    pvbouwel Add QUOTE_IDENT for identifiers (schema,table and column names)
              
              
              
Steps to revoking grants before dropping a user:
1. Find all grants by granted by user to drop and regrant them as another user (superuser preferably).
select ddl from v_generate_user_grant_revoke_ddl where grantor='<username>' and ddltype='grant' and objtype <>'Default ACL' order by sequence;
2. Find all grants granted to user to drop and revoke them.
select ddl from v_generate_user_grant_revoke_ddl where ddltype='revoke' and (username='<username>' or grantor='<username>') order by sequence;              
************************************************************************************************************************/

CREATE OR REPLACE VIEW admin.v_generate_user_grant_revoke_ddl as
WITH 
grantor as (select usesysid,usename from pg_user),
schemas as (select oid,nspname,nspacl,nspowner from pg_namespace),
grantee as ((SELECT pg_user.usesysid as usesysid, 0 as grosysid, pg_user.usename as usename FROM pg_user
UNION ALL 
SELECT 0 as usesysid, pg_group.grosysid as grosysid, pg_group.groname as usename FROM pg_group)
UNION ALL 
SELECT 0 as usesysid, 0 as grosysid, 'PUBLIC'::name as usename),
tabprivs as (SELECT 'SELECT'::character varying as type
UNION ALL 
SELECT 'DELETE'::character varying as type
UNION ALL 
SELECT 'INSERT'::character varying as type
UNION ALL 
SELECT 'UPDATE'::character varying as type
UNION ALL 
SELECT 'EXECUTE'::character varying as type
UNION ALL 
SELECT 'REFERENCES'::character varying as type),
dbprivs as (SELECT 'CREATE'::character varying as type
UNION ALL 
SELECT 'TEMP'::character varying as type),
schemaprivs as (SELECT 'CREATE'::character varying as type
UNION ALL 
SELECT 'USAGE'::character varying as type)
select * from (
-- Functions grants
select pg_get_userbyid(c.proowner),sc.nspname,textin(regprocedureout(c.oid::regprocedure)),g.usename,u.usename,'Function','grant',2,
'grant execute on function '|| QUOTE_IDENT(sc.nspname)||'.'||textin(regprocedureout(c.oid::regprocedure))||
CASE WHEN u.usesysid>1 THEN ' to ' ELSE ' to group ' END||QUOTE_IDENT(u.usename)||
CASE WHEN aclcontains(c.proacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'EXECUTE',true)) THEN ' with grant option;' ELSE ';' END::text
from pg_proc c join schemas sc on c.pronamespace=sc.oid,grantor g,grantee u
where  aclcontains(c.proacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'EXECUTE',false))
UNION ALL
-- Functions revokes
select pg_get_userbyid(c.proowner),sc.nspname,textin(regprocedureout(c.oid::regprocedure)),g.usename,u.usename,'Function','revoke',
CASE WHEN (g.usename <> current_user and g.usename <> pg_get_userbyid(c.proowner) and g.usename <> 'rdsdb') THEN 1::int ELSE 2::int END,
CASE WHEN (g.usename <> current_user and g.usename <> pg_get_userbyid(c.proowner) and g.usename <> 'rdsdb') THEN 'set session authorization '||QUOTE_IDENT(g.usename)||'; ' ELSE '' END::text||
'revoke execute on function '||QUOTE_IDENT(sc.nspname)||'.'||textin(regprocedureout(c.oid::regprocedure))||
CASE WHEN u.usesysid>1 THEN ' from ' ELSE ' from group ' END||QUOTE_IDENT(u.usename)||';'||
CASE WHEN (g.usename <> current_user and g.usename <> pg_get_userbyid(c.proowner) and g.usename <> 'rdsdb') THEN 'reset session authorization;' ELSE '' END::text 
from pg_proc c join schemas sc on c.pronamespace=sc.oid,grantor g,grantee u
where  aclcontains(c.proacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'EXECUTE',false)) 
UNION ALL
-- Language grants
select null,null,c.lanname,g.usename,u.usename,'Language','grant',1,
'grant usage on language '||c.lanname||
CASE WHEN u.usesysid>1 THEN ' to ' ELSE ' to group ' END||QUOTE_IDENT(u.usename)||
CASE WHEN aclcontains(c.lanacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'USAGE',true)) THEN ' with grant option;' ELSE ';' END::text
from pg_language c,grantor g,grantee u
where aclcontains(c.lanacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'USAGE',false)) 
UNION ALL
-- Language revokes
select null,null,c.lanname,g.usename,u.usename,'Language','revoke',
CASE WHEN (g.usename <> current_user and g.usename <> 'rdsdb') THEN 2::int ELSE 3::int END,
CASE WHEN (g.usename <> current_user and g.usename <> 'rdsdb') THEN 'set session authorization '||QUOTE_IDENT(g.usename)||'; ' ELSE '' END::text||
'revoke usage on language '||c.lanname||
CASE WHEN u.usesysid>1 THEN ' from ' ELSE ' from group ' END||QUOTE_IDENT(u.usename)||';'||
CASE WHEN (g.usename <> current_user and g.usename <> 'rdsdb') THEN 'reset session authorization;' ELSE '' END::text 
from pg_language c,grantor g,grantee u
where aclcontains(c.lanacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'USAGE',false)) 
UNION ALL
--Tables grants
select pg_get_userbyid(c.relowner),sc.nspname,c.relname,g.usename,u.usename,case c.relkind WHEN 'r' THEN 'Table' WHEN 'v' THEN 'View' END,'grant',2,
'grant '||t.type||' on '||QUOTE_IDENT(sc.nspname)||'.'||QUOTE_IDENT(c.relname)||
CASE WHEN u.usesysid>1 THEN ' to ' ELSE ' to group ' END||QUOTE_IDENT(u.usename)||
CASE WHEN aclcontains(c.relacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,t.type,true)) THEN ' with grant option;' ELSE ';' END::text
from pg_class c join schemas sc on c.relnamespace=sc.oid, grantor g,grantee u,tabprivs t
where aclcontains(c.relacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,t.type,false)) 
UNION ALL
--Tables revokes
select distinct pg_get_userbyid(c.relowner),sc.nspname,c.relname,g.usename,u.usename,'Table/View','revoke',
CASE WHEN (g.usename <> current_user and g.usename <> pg_get_userbyid(c.relowner) and g.usename <> 'rdsdb') THEN 1::int ELSE 2::int END,
CASE WHEN (g.usename <> current_user and g.usename <> pg_get_userbyid(c.relowner) and g.usename <> 'rdsdb') THEN 'set session authorization '||QUOTE_IDENT(g.usename)||'; ' ELSE '' END::text||
'revoke all on '||QUOTE_IDENT(sc.nspname)||'.'||QUOTE_IDENT(c.relname)||
CASE WHEN u.usesysid>1 THEN ' from ' ELSE ' from group ' END||QUOTE_IDENT(u.usename)||';'||
CASE WHEN (g.usename <> current_user and g.usename <> pg_get_userbyid(c.relowner) and g.usename <> 'rdsdb') THEN 'reset session authorization;' ELSE '' END::text 
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
select pg_get_userbyid(c.nspowner),null,c.nspname,g.usename,u.usename,'Schema','grant',1,
'grant '||s.type||' on schema '||QUOTE_IDENT(c.nspname)||
CASE WHEN u.usesysid>1 THEN ' to ' ELSE ' to group ' END||QUOTE_IDENT(u.usename)||
CASE WHEN aclcontains(c.nspacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,s.type,true)) THEN ' with grant option;' ELSE ';' END::text
from pg_namespace c, grantor g,grantee u,schemaprivs s
where  aclcontains(c.nspacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,s.type,false))
UNION ALL
-- Schema revokes
select pg_get_userbyid(c.nspowner),null,c.nspname,g.usename,u.usename,'Schema','revoke',
CASE WHEN (g.usename <> current_user and g.usename <> pg_get_userbyid(c.nspowner) and g.usename <> 'rdsdb') THEN 2::int ELSE 3::int END,
CASE WHEN (g.usename <> current_user and g.usename <> pg_get_userbyid(c.nspowner) and g.usename <> 'rdsdb') THEN 'set session authorization '||QUOTE_IDENT(g.usename)||'; ' ELSE '' END::text||
'revoke all on schema '||QUOTE_IDENT(c.nspname)||
CASE WHEN u.usesysid>1 THEN ' from ' ELSE ' from group ' END||QUOTE_IDENT(u.usename)||';'||
CASE WHEN (g.usename <> current_user and g.usename <> pg_get_userbyid(c.nspowner) and g.usename <> 'rdsdb') THEN 'reset session authorization;' ELSE '' END::text 
from pg_namespace c, grantor g,grantee u
where  exists 
(select 1 where aclcontains(c.nspacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'CREATE',false))
UNION ALL
select 1 where aclcontains(c.nspacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'USAGE',false)))
UNION ALL
-- Database grants
select pg_get_userbyid(c.datdba),null,c.datname,g.usename,u.usename,'Database','grant',0,
'grant '||d.type||' on database '||QUOTE_IDENT(c.datname)||
CASE WHEN u.usesysid>1 THEN ' to ' ELSE ' to group ' END||QUOTE_IDENT(u.usename)||
CASE WHEN aclcontains(c.datacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,d.type,true)) THEN ' with grant option;' ELSE ';' END::text 
from pg_database c, grantor g,grantee u,dbprivs d
where  aclcontains(c.datacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,d.type,false))
UNION ALL
-- Database revokes
select pg_get_userbyid(c.datdba),null,c.datname,g.usename,u.usename,'Database','revoke',
CASE WHEN (g.usename <> current_user and g.usename <> pg_get_userbyid(c.datdba) and g.usename <> 'rdsdb') THEN 3::int ELSE 4::int END,
CASE WHEN (g.usename <> current_user and g.usename <> pg_get_userbyid(c.datdba) and g.usename <> 'rdsdb') THEN 'set session authorization '||QUOTE_IDENT(g.usename)||'; ' ELSE '' END::text||
'revoke all on database '||QUOTE_IDENT(c.datname)||
CASE WHEN u.usesysid>1 THEN ' from ' ELSE ' from group ' END||QUOTE_IDENT(u.usename)||';'||
CASE WHEN (g.usename <> current_user and g.usename <> pg_get_userbyid(c.datdba) and g.usename <> 'rdsdb') THEN 'reset session authorization;' ELSE '' END::text 
from pg_database c, grantor g,grantee u 
where  exists 
(select 1 where aclcontains(c.datacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'CREATE',false))
UNION ALL
select 1 where aclcontains(c.datacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'TEMP',false)))
UNION ALL
-- Default ACL grants
select pg_get_userbyid(c.defacluser),sc.nspname,decode(c.defaclobjtype,'r','Tables','f','Functions'),g.usename,u.usename,'Default ACL','grant',3,
'alter default privileges for user '||QUOTE_IDENT(pg_get_userbyid(c.defacluser))||
CASE WHEN c.defaclnamespace >0 THEN ' in schema '||QUOTE_IDENT(sc.nspname) ELSE '' END||
' grant '||t.type||' on '||decode(c.defaclobjtype,'r','tables','f','functions')||
CASE WHEN u.usesysid>1 THEN ' to ' ELSE ' to group ' END||QUOTE_IDENT(u.usename)||
CASE WHEN aclcontains(c.defaclacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,t.type,true)) THEN ' with grant option;' ELSE ';' END::text
from pg_default_acl c left join schemas sc on c.defaclnamespace=sc.oid, grantor g,grantee u,tabprivs t
where  aclcontains(c.defaclacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,t.type,false))
UNION ALL
-- Default ACL revokes
select pg_get_userbyid(c.defacluser),sc.nspname,decode(c.defaclobjtype,'r','Tables','f','Functions'),g.usename,u.usename,'Default ACL','revoke',0,
'alter default privileges for user '||QUOTE_IDENT(pg_get_userbyid(c.defacluser))||
CASE WHEN c.defaclnamespace >0 THEN ' in schema '||QUOTE_IDENT(sc.nspname) ELSE '' END||
' revoke all on '||decode(c.defaclobjtype,'r','tables','f','functions')||
CASE WHEN u.usesysid>1 THEN ' from ' ELSE ' from group ' END||QUOTE_IDENT(u.usename)||';'::text 
from pg_default_acl c left join schemas sc on c.defaclnamespace=sc.oid, grantor g,grantee u 
where  EXISTS (
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
and pg_get_userbyid(c.defacluser)<>u.usename
and array_to_string(c.defaclacl,'')<>('=X/'||QUOTE_IDENT(pg_get_userbyid(c.defacluser)))
UNION ALL
--Default ACL grants with empty acl
select pg_get_userbyid(c.defacluser),sc.nspname,decode(c.defaclobjtype,'r','Tables','f','Functions'),null,pg_get_userbyid(c.defacluser),'Default ACL','revoke',0,
'alter default privileges for user '||QUOTE_IDENT(pg_get_userbyid(c.defacluser))||
CASE WHEN c.defaclnamespace >0 THEN ' in schema '||QUOTE_IDENT(sc.nspname) ELSE '' END||
' grant all on '||
CASE WHEN c.defaclobjtype='r' THEN 'tables to '||QUOTE_IDENT(pg_get_userbyid(c.defacluser))||';'
WHEN c.defaclobjtype='f' THEN 'functions to '||QUOTE_IDENT(pg_get_userbyid(c.defacluser))||',public;' END::text
from pg_default_acl c left join schemas sc on c.defaclnamespace=sc.oid
where EXISTS (select 1 where c.defaclacl='{}'::aclitem[]
UNION ALL
select 1 WHERE array_to_string(c.defaclacl,'')=('=X/'||QUOTE_IDENT(pg_get_userbyid(c.defacluser))))
)userddl("objowner","schemaname","objname","grantor","username","objtype","ddltype","sequence","ddl") where (username<>objowner or objtype='Default ACL' or objowner is null or grantor is null) and username<>'rdsdb';
