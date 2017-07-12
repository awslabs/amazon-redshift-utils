create or replace view v_generate_user_grant_revoke_ddl as
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
select pg_get_userbyid(c.proowner),g.usename,u.usename,'Function','grant',
'grant execute on function '||sc.nspname||'.'||textin(regprocedureout(c.oid::regprocedure))||
case when u.usesysid>1 then ' to ' else ' to group ' end||u.usename||
CASE WHEN aclcontains(c.proacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'EXECUTE',true)) THEN ' with grant option;'::text 
ELSE ';'::text END
from pg_proc c join schemas sc on c.pronamespace=sc.oid,grantor g,grantee u
where  aclcontains(c.proacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'EXECUTE',false))

UNION ALL
-- Functions revokes
select pg_get_userbyid(c.proowner),g.usename,u.usename,'Function','revoke',
'revoke execute on function '||sc.nspname||'.'||textin(regprocedureout(c.oid::regprocedure))||
case when u.usesysid>1 then ' from ' else ' from group ' end||u.usename||';'
from pg_proc c join schemas sc on c.pronamespace=sc.oid,grantor g,grantee u
where  aclcontains(c.proacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'EXECUTE',false)) 
UNION ALL
-- Language grants
select null,g.usename,u.usename,'Language','grant',
'grant usage on language '||c.lanname||
case when u.usesysid>1 then ' to ' else ' to group ' end||u.usename||
CASE WHEN aclcontains(c.lanacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'USAGE',true)) THEN ' with grant option;'::text 
ELSE ';'::text END
from pg_language c,grantor g,grantee u
where aclcontains(c.lanacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'USAGE',false)) 
UNION ALL
-- Language revokes
select null,g.usename,u.usename,'Language','revoke',
'revoke usage on language '||c.lanname||
case when u.usesysid>1 then ' from ' else ' from group ' end||u.usename||';'
from pg_language c,grantor g,grantee u
where aclcontains(c.lanacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'USAGE',false)) 
UNION ALL
--Tables grants
select pg_get_userbyid(c.relowner),g.usename,u.usename,case c.relkind when 'r' then 'Table' when 'v' then 'View' end,'grant',
'grant '||t.type||' on '||sc.nspname||'.'||c.relname||
case when u.usesysid>1 then ' to ' else ' to group ' end||u.usename||
CASE WHEN aclcontains(c.relacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,t.type,true)) THEN ' with grant option;'::text 
ELSE ';'::text END
from pg_class c join schemas sc on c.relnamespace=sc.oid, grantor g,grantee u,tabprivs t
where aclcontains(c.relacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,t.type,false)) 
UNION ALL
--Tables revokes
select distinct pg_get_userbyid(c.relowner),g.usename,u.usename,'Table/View','revoke',
'revoke all on all tables in schema '||sc.nspname||
case when u.usesysid>1 then ' from ' else ' from group ' end||u.usename||';'
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
select pg_get_userbyid(c.nspowner),g.usename,u.usename,'Schema','grant',
'grant '||s.type||' on schema '||c.nspname||
case when u.usesysid>1 then ' to ' else ' to group ' end||u.usename||
CASE WHEN aclcontains(c.nspacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,s.type,true)) THEN ' with grant option;'::text 
ELSE ';'::text END
from pg_namespace c, grantor g,grantee u,schemaprivs s
where  aclcontains(c.nspacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,s.type,false))
UNION ALL
-- Schema revokes
select pg_get_userbyid(c.nspowner),g.usename,u.usename,'Schema','revoke',
'revoke all on schema '||c.nspname||
case when u.usesysid>1 then ' from ' else ' from group ' end||u.usename||';'
from pg_namespace c, grantor g,grantee u
where  exists 
(select 1 where aclcontains(c.nspacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'CREATE',false))
UNION ALL
select 1 where aclcontains(c.nspacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'USAGE',false)))
UNION ALL
-- Database grants
select pg_get_userbyid(c.datdba),g.usename,u.usename,'Database','grant',
'grant '||d.type||' on database '||c.datname||
case when u.usesysid>1 then ' to ' else ' to group ' end||u.usename||
CASE WHEN aclcontains(c.datacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,d.type,true)) THEN ' with grant option;'::text 
ELSE ';'::text END
from pg_database c, grantor g,grantee u,dbprivs d
where  aclcontains(c.datacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,d.type,false))
UNION ALL
-- Database revokes
select pg_get_userbyid(c.datdba),g.usename,u.usename,'Database','revoke',
'revoke all on database '||c.datname||
case when u.usesysid>1 then ' from ' else ' from group ' end||u.usename||';'
from pg_database c, grantor g,grantee u 
where  exists 
(select 1 where aclcontains(c.datacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'CREATE',false))
UNION ALL
select 1 where aclcontains(c.datacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,'TEMP',false)))
UNION ALL
-- Default ACL grants
select pg_get_userbyid(c.defacluser),g.usename,u.usename,'Default ACL','grant',
'alter default privileges for user '||pg_get_userbyid(c.defacluser)||
case when c.defaclnamespace >0 then ' in schema '||sc.nspname else '' end||
' grant '||t.type||' on '||decode(c.defaclobjtype,'r','tables','f','functions')||
case when u.usesysid>1 then ' to ' else ' to group ' end||u.usename||
CASE WHEN aclcontains(c.defaclacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,t.type,true)) THEN ' with grant option;'::text 
ELSE ';'::text END
from pg_default_acl c left join schemas sc on c.defaclnamespace=sc.oid, grantor g,grantee u,tabprivs t
where  aclcontains(c.defaclacl, makeaclitem(u.usesysid,u.grosysid,g.usesysid,t.type,false))
UNION ALL
-- Default ACL revokes
select pg_get_userbyid(c.defacluser),g.usename,u.usename,'Default ACL','revoke',
'alter default privileges for user '||pg_get_userbyid(c.defacluser)||
case when c.defaclnamespace >0 then ' in schema '||sc.nspname else '' end||
' revoke all on '||decode(c.defaclobjtype,'r','tables','f','functions')||
case when u.usesysid>1 then ' from ' else ' from group ' end||u.usename||';'
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
and array_to_string(c.defaclacl,'')<>('=X/'||pg_get_userbyid(c.defacluser))
UNION ALL
--Default ACL grants with empty acl
select pg_get_userbyid(c.defacluser),null,pg_get_userbyid(c.defacluser),'Default ACL','revoke',
'alter default privileges for user '||pg_get_userbyid(c.defacluser)||
case when c.defaclnamespace >0 then ' in schema '||sc.nspname else '' end||
' grant all on '||
case when c.defaclobjtype='r' then 'tables to '||pg_get_userbyid(c.defacluser)||';'
when c.defaclobjtype='f' then 'functions to '||pg_get_userbyid(c.defacluser)||',public;' end
from pg_default_acl c left join schemas sc on c.defaclnamespace=sc.oid
where EXISTS (select 1 where c.defaclacl='{}'::aclitem[]
UNION ALL
select 1 WHERE array_to_string(c.defaclacl,'')=('=X/'||pg_get_userbyid(c.defacluser)))
)userddl("objowner","grantor","username","objtype","ddltype","ddl") where (username<>objowner or objtype='Default ACL' or objowner is null or grantor is null) and username<>'rdsdb';