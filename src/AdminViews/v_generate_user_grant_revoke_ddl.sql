/*************************************************************************************************************************
Purpose:        View to generate grant or revoke ddl for users and groups. This is useful for 
                recreating users or group privileges or for revoking privileges before dropping 
                a user or group. 
		
Current Version:        1.06
Columns -
objowner:       Object owner 
schemaname:     Object schema if applicable
objname:        Name of the object the privilege is granted on
grantor:        User that granted the privilege
grantee:        User/Group the privilege is granted to
objtype:        Type of object user has privilege on. Object types are Function,Schema,
		        Table or View, Database, Language or Default ACL
ddltype:        Type of ddl generated i.e grant or revoke
grantseq:       Sequence number to order the DDLs by hierarchy
objseq:         Sequence number to order the objects by hierarchy
ddl:            DDL text
Notes:           
                
History:
Version 1.01
	2017-03-01      adedotua created
	2018-03-04      adedotua completely refactored the view to minimize nested loop joins. View is now significantly faster on clusters
			with a large number of users and privileges
	2018-03-04      adedotua added column grantseq to help return the DDLs in the order they need to be granted or revoked
	2018-03-04      adedotua renamed column sequence to objseq and username to grantee
Version 1.02
	2018-03-09      adedotua added logic to handle function name generation when there are non-alphabets in the function schemaname
Version 1.03
	2018-04-26	adedotua added missing filter for handling empty default acls
	2018-04-26	adedotua fixed one more edge case where default privilege is granted on schema to user other than schema owner
Version 1.04
	2018-05-02	adedotua added support for privileges granted on pg_catalog tables and other system owned objects
Version 1.05
	2018-06-22	adedotua fixed issue with generation of default privileges grants.
Version 1.06
	2018-11-12	adedotua fixed issue with generation of default privileges revokes for user who granted the privileges (defacluser).	
Version 1.07
	2019-10-10	adedotua added handling for privileges granted on procedures. Increased ddl column to varchar(4000) to incorporate 
			CrisFavero #460 pull request. Fixed handling for empty default aclitem

Steps to revoking grants before dropping a user:
1. Find all grants by granted by user to drop and regrant them as another user (superuser preferably).
select regexp_replace(ddl,grantor,'<superuser>') from v_generate_user_grant_revoke_ddl where grantor='<username>' and ddltype='grant' and objtype <>'default acl' order by objseq,grantseq;
2. Find all grants granted to user to drop and revoke them.
select ddl from v_generate_user_grant_revoke_ddl where ddltype='revoke' and (grantee='<username>' or grantor='<username>') order by objseq, grantseq desc;              
************************************************************************************************************************/

--DROP view admin.v_generate_user_grant_revoke_ddl;
CREATE OR REPLACE VIEW admin.v_generate_user_grant_revoke_ddl AS 
WITH objprivs AS ( 
	SELECT objowner, 
	schemaname, 
	objname, 
	objtype,
	CASE WHEN split_part(aclstring,'=',1)='' THEN 'PUBLIC' ELSE translate(trim(split_part(aclstring,'=',1)),'"','') END::text AS grantee,
	translate(trim(split_part(aclstring,'/',2)),'"','')::text AS grantor, 
	trim(split_part(split_part(aclstring,'=',2),'/',1))::text AS privilege, 
	CASE WHEN objtype = 'default acl' THEN objname 
	WHEN objtype in ('procedure','function') AND regexp_instr(schemaname,'[^a-z]') > 0 THEN objname
	WHEN objtype in ('procedure','function') THEN QUOTE_IDENT(schemaname)||'.'||objname 
	ELSE nvl(QUOTE_IDENT(schemaname)||'.'||QUOTE_IDENT(objname),QUOTE_IDENT(objname)) END::text as fullobjname,
	CASE WHEN split_part(aclstring,'=',1)='' THEN 'PUBLIC' 
	ELSE trim(split_part(aclstring,'=',1)) 
	END::text as splitgrantee,
	grantseq 
	FROM (
		-- TABLE AND VIEW privileges
		SELECT pg_get_userbyid(b.relowner)::text AS objowner, 
		trim(c.nspname)::text AS schemaname,  
		b.relname::text AS objname,
		CASE WHEN relkind='r' THEN 'table' ELSE 'view' END::text AS objtype, 
		TRIM(SPLIT_PART(array_to_string(b.relacl,','), ',', NS.n))::text AS aclstring, 
		NS.n as grantseq
		FROM 
		(SELECT oid,generate_series(1,array_upper(relacl,1))  AS n FROM pg_class) NS
		inner join pg_class B ON b.oid = ns.oid AND  NS.n <= array_upper(b.relacl,1)
		join pg_namespace c on b.relnamespace = c.oid
		where relkind in ('r','v')
		UNION ALL
		-- SCHEMA privileges
		SELECT pg_get_userbyid(b.nspowner)::text AS objowner,
		null::text AS schemaname,
		b.nspname::text AS objname,
		'schema'::text AS objtype,
		TRIM(SPLIT_PART(array_to_string(b.nspacl,','), ',', NS.n))::text AS aclstring,
		NS.n as grantseq
		FROM 
		(SELECT oid,generate_series(1,array_upper(nspacl,1)) AS n FROM pg_namespace) NS
		inner join pg_namespace B ON b.oid = ns.oid AND NS.n <= array_upper(b.nspacl,1)
		UNION ALL
		-- DATABASE privileges
		SELECT pg_get_userbyid(b.datdba)::text AS objowner,
		null::text AS schemaname,
		b.datname::text AS objname,
		'database'::text AS objtype,
		TRIM(SPLIT_PART(array_to_string(b.datacl,','), ',', NS.n))::text AS aclstring,
		NS.n as grantseq
		FROM 
		(SELECT oid,generate_series(1,array_upper(datacl,1)) AS n FROM pg_database) NS
		inner join pg_database B ON b.oid = ns.oid AND NS.n <= array_upper(b.datacl,1) 
		UNION ALL 
		SELECT pg_get_userbyid(b.proowner)::text AS objowner,
		trim(c.nspname)::text AS schemaname, 
		textin(regprocedureout(b.oid::regprocedure))::text AS objname,
		decode(prorettype,0,'procedure','function')::text AS objtype,
		TRIM(SPLIT_PART(array_to_string(b.proacl,','), ',', NS.n))::text AS aclstring,
		NS.n as grantseq  
		FROM 
		(SELECT oid,generate_series(1,array_upper(proacl,1)) AS n FROM pg_proc) NS
		inner join pg_proc B ON b.oid = ns.oid and NS.n <= array_upper(b.proacl,1)
		join pg_namespace c on b.pronamespace=c.oid 
		UNION ALL
		-- LANGUAGE privileges
		SELECT null::text AS objowner,
		null::text AS schemaname,
		lanname::text AS objname,
		'language'::text AS objtype,
		TRIM(SPLIT_PART(array_to_string(b.lanacl,','), ',', NS.n))::text AS aclstring,
		NS.n as grantseq 
		FROM 
		(SELECT oid,generate_series(1,array_upper(lanacl,1)) AS n FROM pg_language) NS
		inner join pg_language B ON b.oid = ns.oid and NS.n <= array_upper(b.lanacl,1)
		UNION ALL
		-- DEFAULT ACL privileges
		SELECT pg_get_userbyid(b.defacluser)::text AS objowner,
		trim(c.nspname)::text AS schemaname,
		decode(b.defaclobjtype,'r','tables','f','functions','p','procedures')::text AS objname,
		'default acl'::text AS objtype,
		TRIM(SPLIT_PART(array_to_string(b.defaclacl,','), ',', NS.n))::text AS aclstring,
		NS.n as grantseq 
		FROM 
		(SELECT oid,generate_series(1,array_upper(defaclacl,1)) AS n FROM pg_default_acl) NS
		join pg_default_acl b ON b.oid = ns.oid and NS.n <= array_upper(b.defaclacl,1) 
		left join  pg_namespace c on b.defaclnamespace=c.oid
	) 
	where  (split_part(aclstring,'=',1) <> split_part(aclstring,'/',2) 
	and split_part(aclstring,'=',1) <> 'rdsdb'
	and NOT (split_part(aclstring,'=',1)='' AND split_part(aclstring,'/',2) = 'rdsdb')) 
)
-- Extract object GRANTS
SELECT objowner::text, schemaname::text, objname::text, objtype::text, grantor::text, grantee::text, 'grant'::text AS ddltype, grantseq,
decode(objtype,'database',0,'schema',1,'language',1,'table',2,'view',2,'function',2,'procedure',2,'default acl',3) AS objseq,
CASE WHEN (grantor <> current_user AND grantor <> 'rdsdb' AND objtype <> 'default acl') 
THEN 'SET SESSION AUTHORIZATION '||QUOTE_IDENT(grantor)||';' ELSE '' END::varchar(4000)||
(CASE WHEN privilege = 'arwdRxt' OR privilege = 'a*r*w*d*R*x*t*' THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT ALL on '||fullobjname||' to '||splitgrantee||
(CASE WHEN privilege = 'a*r*w*d*R*x*t*' THEN ' with grant option;' ELSE ';' END::varchar(4000)) 
when privilege = 'UC' OR privilege = 'U*C*' THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT ALL on '||objtype||' '||fullobjname||' to '||splitgrantee||
(CASE WHEN privilege = 'U*C*' THEN ' with grant option;' ELSE ';' END::varchar(4000)) 
when privilege = 'CT' OR privilege = 'U*C*' THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT ALL on '||objtype||' '||fullobjname||' to '||splitgrantee||
(CASE WHEN privilege = 'C*T*' THEN ' with grant option;' ELSE ';' END::varchar(4000))
ELSE  
(
CASE WHEN charindex('a',privilege) > 0 THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT INSERT on '||fullobjname||' to '||splitgrantee|| 
(CASE WHEN charindex('a*',privilege) > 0 THEN ' with grant option;' ELSE ';' END::varchar(4000)) ELSE '' END::varchar(4000)||
CASE WHEN charindex('r',privilege) > 0 THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT SELECT on '||fullobjname||' to '||splitgrantee||
(CASE WHEN charindex('r*',privilege) > 0 THEN ' with grant option;' ELSE ';' END::varchar(4000)) ELSE '' END::varchar(4000)||
CASE WHEN charindex('w',privilege) > 0 THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT UPDATE on '||fullobjname||' to '||splitgrantee||
(CASE WHEN charindex('w*',privilege) > 0 THEN ' with grant option;' ELSE ';' END::varchar(4000)) ELSE '' END::varchar(4000)||
CASE WHEN charindex('d',privilege) > 0 THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT DELETE on '||fullobjname||' to '||splitgrantee||
(CASE WHEN charindex('d*',privilege) > 0 THEN ' with grant option;' ELSE ';' END::varchar(4000)) ELSE '' END::varchar(4000)||
CASE WHEN charindex('R',privilege) > 0 THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT RULE on '||fullobjname||' to '||splitgrantee||
(CASE WHEN charindex('R*',privilege) > 0 THEN ' with grant option;' ELSE ';' END::varchar(4000)) ELSE '' END::varchar(4000)||
CASE WHEN charindex('x',privilege) > 0 THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT REFERENCES on '||fullobjname||' to '||splitgrantee||
(CASE WHEN charindex('x*',privilege) > 0 THEN ' with grant option;' ELSE ';' END::varchar(4000)) ELSE '' END::varchar(4000)||
CASE WHEN charindex('t',privilege) > 0 THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT TRIGGER on '||fullobjname||' to '||splitgrantee||
(CASE WHEN charindex('t*',privilege) > 0 THEN ' with grant option;' ELSE ';' END::varchar(4000)) ELSE '' END::varchar(4000)||
CASE WHEN charindex('U',privilege) > 0 THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT USAGE on '||objtype||' '||fullobjname||' to '||splitgrantee||
(CASE WHEN charindex('U*',privilege) > 0 THEN ' with grant option;' ELSE ';' END::varchar(4000)) ELSE '' END::varchar(4000)||
CASE WHEN charindex('C',privilege) > 0 THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT CREATE on '||objtype||' '||fullobjname||' to '||splitgrantee||
(CASE WHEN charindex('C*',privilege) > 0 THEN ' with grant option;' ELSE ';' END::varchar(4000)) ELSE '' END::varchar(4000)||
CASE WHEN charindex('T',privilege) > 0 THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT TEMP on '||objtype||' '||fullobjname||' to '||splitgrantee||
(CASE WHEN charindex('T*',privilege) > 0 THEN ' with grant option;' ELSE ';' END::varchar(4000)) ELSE '' END::varchar(4000)||
CASE WHEN charindex('X',privilege) > 0 THEN (CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
ELSE '' END::varchar(4000))||'GRANT EXECUTE on '||
(CASE WHEN objtype = 'default acl' THEN '' ELSE objtype||' ' END::varchar(4000))||fullobjname||' to '||splitgrantee||
(CASE WHEN charindex('X*',privilege) > 0 THEN ' with grant option;' ELSE ';' END::varchar(4000)) ELSE '' END::varchar(4000)
) END::varchar(4000))|| 
CASE WHEN (grantor <> current_user AND grantor <> 'rdsdb' AND objtype <> 'default acl') THEN 'RESET SESSION AUTHORIZATION;' ELSE '' END::varchar(4000) AS ddl
FROM objprivs
UNION ALL
-- Extract object REVOKES
SELECT objowner::text, schemaname::text, objname::text, objtype::text, grantor::text, grantee::text, 'revoke'::text AS ddltype, grantseq,
decode(objtype,'default acl',0,'function',0,'procedure',1,'table',1,'view',1,'schema',2,'language',2,'database',3) AS objseq,
CASE WHEN (grantor <> current_user AND grantor <> 'rdsdb' AND objtype <> 'default acl' AND grantor <> objowner) THEN 'SET SESSION AUTHORIZATION '||QUOTE_IDENT(grantor)||';' ELSE '' END::varchar(4000)||
(CASE WHEN objtype = 'default acl' THEN 'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(grantor)||nvl(' in schema '||QUOTE_IDENT(schemaname)||' ',' ')
||'REVOKE ALL on '||fullobjname||' FROM '||splitgrantee||';'
ELSE 'REVOKE ALL on '||(CASE WHEN objtype = 'table' OR objtype = 'view' THEN '' ELSE objtype||' ' END::varchar(4000))||fullobjname||' FROM '||splitgrantee||';' END::varchar(4000))||
CASE WHEN (grantor <> current_user AND grantor <> 'rdsdb' AND objtype <> 'default acl' AND grantor <> objowner) THEN 'RESET SESSION AUTHORIZATION;' ELSE '' END::varchar(4000) AS ddl
FROM objprivs
WHERE NOT (objtype = 'default acl' AND grantee = 'PUBLIC' and objname in ('functions'))
UNION ALL
-- Eliminate empty default ACLs
SELECT null::text AS objowner, null::text AS schemaname, decode(b.defaclobjtype,'r','tables','f','functions','p','procedures')::text AS objname,
		'default acl'::text AS objtype,  pg_get_userbyid(b.defacluser)::text AS grantor, null::text AS grantee, 'revoke'::text AS ddltype, 5 as grantseq, 5 AS objseq,
  'ALTER DEFAULT PRIVILEGES for user '||QUOTE_IDENT(pg_get_userbyid(b.defacluser))||' GRANT ALL on '||decode(b.defaclobjtype,'r','tables','f','functions','p','procedures')||' TO '||QUOTE_IDENT(pg_get_userbyid(b.defacluser))||
CASE WHEN b.defaclobjtype = 'f' then ', PUBLIC;' ELSE ';' END::varchar(4000) AS ddl FROM pg_default_acl b where b.defaclacl = '{}'::aclitem[] or (defaclnamespace=0 and defaclobjtype='f'); 
