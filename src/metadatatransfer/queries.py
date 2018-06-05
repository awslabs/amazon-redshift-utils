tablelist = """select trim(pgn.nspname),trim(pgc.relname),decode(stvb.size_mb,null,'0',trim(stvb.size_mb)) 
 from pg_class pgc join pg_namespace pgn on pgc.relnamespace=pgn.oid 
 left join (select tbl,count(blocknum) size_mb from stv_blocklist group by tbl) stvb on pgc.oid=stvb.tbl 
 join (select id from stv_tbl_perm where backup=1 and slice=0) stvp on pgc.oid=stvp.id
 where pgc.relowner>1"""
# DATABASE privileges
databaseprivs = """SELECT  
QUOTE_IDENT(pg_get_userbyid(b.datdba))::text AS objowner,
null::text AS schemaname,
QUOTE_IDENT(b.datname)::text AS objname,
'database'::text AS objtype,
array_to_string(b.datacl,',')::text AS aclstring
FROM 
pg_database b"""
# LANGUAGE privileges
languageprivs = """SELECT
null::text AS objowner,
null::text AS schemaname,
lanname::text AS objname,
'language'::text AS objtype,
array_to_string(b.lanacl,',') AS aclstring 
FROM pg_language b where lanacl is not null"""
# SCHEMA privileges
schemaprivs = """SELECT
QUOTE_IDENT(pg_get_userbyid(b.nspowner))::text AS objowner,
null::text AS schemaname,
QUOTE_IDENT(b.nspname)::text AS objname,
'schema'::text AS objtype,
array_to_string(b.nspacl,',')::text AS aclstring
FROM
pg_namespace b where QUOTE_IDENT(b.nspname) not ilike 'pg\_temp\_%'"""
# TABLE AND VIEW privileges
tableprivs = """SELECT 
QUOTE_IDENT(pg_get_userbyid(b.relowner))::text AS objowner, 
QUOTE_IDENT(trim(c.nspname))::text AS schemaname,  
QUOTE_IDENT(b.relname)::text AS objname,
'table'::text AS objtype, 
array_to_string(b.relacl,',')::text AS aclstring 
FROM 
pg_class b
join pg_namespace c on b.relnamespace = c.oid
where relkind in ('r','v')"""
# FUNCTION privileges
functionprivs = """SELECT
QUOTE_IDENT(pg_get_userbyid(b.proowner))::text AS objowner,
QUOTE_IDENT(trim(c.nspname))::text AS schemaname, 
QUOTE_IDENT(proname)||'('||oidvectortypes(proargtypes)||')'::text AS objname,
'function'::text AS objtype,
array_to_string(b.proacl,',') AS aclstring
FROM 
pg_proc b
join pg_namespace c on b.pronamespace=c.oid"""
# DEFAULT ACL privileges
defaclprivs = """SELECT
QUOTE_IDENT(pg_get_userbyid(b.defacluser))::text AS objowner,
QUOTE_IDENT(trim(c.nspname))::text AS schemaname,
decode(b.defaclobjtype,'r','tables','f','functions')::text AS objname,
'default acl'::text AS objtype,
array_to_string(b.defaclacl,',')::text AS aclstring 
FROM 
pg_default_acl b  
left join  pg_namespace c on b.defaclnamespace=c.oid"""
dbobjs = """select QUOTE_IDENT(datname) from pg_database"""
dblist = """select datname,'create database '||QUOTE_IDENT(datname)||';' from pg_database where datdba>1"""
schemalist = """select QUOTE_IDENT(nspname),'create schema if not exists '||QUOTE_IDENT(nspname)||';' 
from pg_namespace where nspowner>1"""
grouplist = """select groname,'create group '||quote_ident(groname)||';' from pg_group;"""
userlist = """select usename,'create user '||quote_ident(usename)||' password disable;' FROM pg_user_info u
where usename <>'rdsdb' and usesuper='f'"""
addusrtogrp = """select pg_get_userbyid(grolist[i]),groname,
'alter group '||groname||' add user '||quote_ident(pg_get_userbyid(grolist[i]))||';' from 
(SELECT generate_series(1, array_upper(grolist, 1)) AS i, grolist,groname FROM pg_group)
where grolist[i] not in (select usesysid from pg_user where usename ='rdsdb' or usesuper='t');"""
usrconfig = """select usename,useconfig[i], 'alter user '||usename||' set '||
case when split_part(useconfig[i],'=',1)='TimeZone' then split_part(useconfig[i],'=',1)||'='''||
split_part(useconfig[i],'=',2)||'''' else useconfig[i] end||';' from 
(SELECT generate_series(1, array_upper(useconfig, 1)) AS i, useconfig,usename FROM pg_user where usename<>'rdsdb'  
and usesuper='f');"""
usrprofile = """select usename,'alter user '||usename||decode(usecreatedb,'t',' createdb','')||
decode(usesuper,'t',' createuser','')||nvl(' connection limit '||useconnlimit,'')||
nvl(' valid until '''||valuntil||'''','')||';' 
from 
(SELECT u.usename,
  u.usesysid,
  u.usecreatedb,
  u.usesuper,
  u.useconnlimit,
  decode(valuntil,'infinity'::abstime,'infinity','invalid'::abstime,null,
  to_char(valuntil,'YYYY-MM-DD HH24:MI:SS')) AS valuntil 
FROM pg_user_info u
where usename <>'rdsdb' and usesuper='f')"""
sourcetables = """select nspname||'.'||relname from pg_class a join pg_namespace b on a.relnamespace=b.oid 
where relowner > 1"""
viewddl = """SELECT 
n.nspname||'.'||c.relname AS viewname
,'CREATE OR REPLACE VIEW ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + ' AS ' + 
COALESCE(pg_get_viewdef(c.oid), '') AS ddl
FROM 
    pg_catalog.pg_class AS c
INNER JOIN
    pg_catalog.pg_namespace AS n
    ON c.relnamespace = n.oid
WHERE relkind = 'v'
and relowner > 1;"""
tblddl = """select schemaname||'.'||tablename, ddl from (SELECT
  tableowner
 ,REGEXP_REPLACE (schemaname, '^zzzzzzzz', '') AS schemaname
 ,REGEXP_REPLACE (tablename, '^zzzzzzzz', '') AS tablename
 ,seq
 ,ddl
FROM
 (
 SELECT
   tableowner
  ,schemaname 
  ,tablename
  ,seq
  ,ddl
 FROM
  (
  --DROP TABLE
  SELECT
    c.relowner as tableowner
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,0 AS seq
   ,'--DROP TABLE ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + ';' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --CREATE TABLE
  UNION SELECT
    c.relowner as tableowner  
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,2 AS seq
   ,'CREATE TABLE IF NOT EXISTS ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + '' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --OPEN PAREN COLUMN LIST
  UNION SELECT c.relowner as tableowner, n.nspname AS schemaname, c.relname AS tablename, 5 AS seq, '(' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --COLUMN LIST
  UNION SELECT
   tableowner
   ,schemaname
   ,tablename
   ,seq
   ,'\t' + col_delim + col_name + ' ' + col_datatype + ' ' + col_nullable + ' ' + col_default + ' ' + col_encoding AS ddl
  FROM
   (
   SELECT
    c.relowner as tableowner   
    ,n.nspname AS schemaname
    ,c.relname AS tablename
    ,100000000 + a.attnum AS seq
    ,CASE WHEN a.attnum > 1 THEN ',' ELSE '' END AS col_delim
    ,QUOTE_IDENT(a.attname) AS col_name
    ,CASE WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING') > 0
      THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING', 'VARCHAR')
     WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER') > 0
      THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER', 'CHAR')
     ELSE UPPER(format_type(a.atttypid, a.atttypmod))
     END AS col_datatype
    ,CASE WHEN format_encoding((a.attencodingtype)::integer) = 'none'
     THEN ''
     ELSE 'ENCODE ' + format_encoding((a.attencodingtype)::integer)
     END AS col_encoding
    ,CASE WHEN a.atthasdef IS TRUE THEN 'DEFAULT ' + adef.adsrc ELSE '' END AS col_default
    ,CASE WHEN a.attnotnull IS TRUE THEN 'NOT NULL' ELSE '' END AS col_nullable
   FROM pg_namespace AS n
   INNER JOIN pg_class AS c ON n.oid = c.relnamespace
   INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
   LEFT OUTER JOIN pg_attrdef AS adef ON a.attrelid = adef.adrelid AND a.attnum = adef.adnum
   WHERE c.relkind = 'r'
     AND a.attnum > 0
   ORDER BY a.attnum
   )
  --CONSTRAINT LIST
  UNION (SELECT
    c.relowner as tableowner  	
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,200000000 + CAST(con.oid AS INT) AS seq
   ,'\t,' + pg_get_constraintdef(con.oid) AS ddl
  FROM pg_constraint AS con
  INNER JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.oid = con.conrelid
  INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' AND pg_get_constraintdef(con.oid) NOT LIKE 'FOREIGN KEY%'
  ORDER BY seq)
  --CLOSE PAREN COLUMN LIST
  UNION SELECT c.relowner as tableowner,n.nspname AS schemaname, c.relname AS tablename, 299999999 AS seq, ')' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --BACKUP
  UNION SELECT
    c.relowner as tableowner
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,300000000 AS seq
   ,'BACKUP NO' as ddl
FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN (SELECT
    SPLIT_PART(key,'_',5) id
    FROM pg_conf
    WHERE key LIKE 'pg_class_backup_%'
    AND SPLIT_PART(key,'_',4) = (SELECT
      oid
      FROM pg_database
      WHERE datname = current_database())) t ON t.id=c.oid
  WHERE c.relkind = 'r'
  --BACKUP WARNING
  UNION SELECT
    c.relowner as tableowner  
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,1 AS seq
   ,'--WARNING: This DDL inherited the BACKUP NO property from the source table' as ddl
FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN (SELECT
    SPLIT_PART(key,'_',5) id
    FROM pg_conf
    WHERE key LIKE 'pg_class_backup_%'
    AND SPLIT_PART(key,'_',4) = (SELECT
      oid
      FROM pg_database
      WHERE datname = current_database())) t ON t.id=c.oid
  WHERE c.relkind = 'r'
  --DISTSTYLE
  UNION SELECT
    c.relowner as tableowner
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,300000001 AS seq
   ,CASE WHEN c.reldiststyle = 0 THEN 'DISTSTYLE EVEN'
    WHEN c.reldiststyle = 1 THEN 'DISTSTYLE KEY'
    WHEN c.reldiststyle = 8 THEN 'DISTSTYLE ALL'
    ELSE '<<Error - UNKNOWN DISTSTYLE>>'
    END AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --DISTKEY COLUMNS
  UNION SELECT
    c.relowner as tableowner
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,400000000 + a.attnum AS seq
   ,'DISTKEY (' + QUOTE_IDENT(a.attname) + ')' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND a.attisdistkey IS TRUE
    AND a.attnum > 0
  --SORTKEY COLUMNS 
  UNION select tableowner,schemaname, tablename, seq,
       case when min_sort <0 then 'INTERLEAVED SORTKEY (' else 'SORTKEY (' end as ddl
from (SELECT
	c.relowner as tableowner
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,499999999 AS seq
   ,min(attsortkeyord) min_sort FROM pg_namespace AS n
  INNER JOIN  pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
  AND abs(a.attsortkeyord) > 0
  AND a.attnum > 0
  group by 1,2,3 )
  UNION (SELECT
  	c.relowner as tableowner
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,500000000 + abs(a.attsortkeyord) AS seq
   ,CASE WHEN abs(a.attsortkeyord) = 1
    THEN '\t' + QUOTE_IDENT(a.attname)
    ELSE '\t, ' + QUOTE_IDENT(a.attname)
    END AS ddl
  FROM  pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND abs(a.attsortkeyord) > 0
    AND a.attnum > 0
  ORDER BY abs(a.attsortkeyord))
  UNION SELECT
   c.relowner as tableowner
   ,n.nspname AS schemaname
   ,c.relname AS tablename
   ,599999999 AS seq
   ,'\t)' AS ddl
  FROM pg_namespace AS n
  INNER JOIN  pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN  pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND abs(a.attsortkeyord) > 0
    AND a.attnum > 0
  --END SEMICOLON
  UNION SELECT c.relowner as tableowner,n.nspname AS schemaname, c.relname AS tablename, 600000000 AS seq, ';' AS ddl
  FROM  pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' )
UNION (
    SELECT c.relowner as tableowner,'zzzzzzzz' || n.nspname AS schemaname,
       'zzzzzzzz' || c.relname AS tablename,
       700000000 + CAST(con.oid AS INT) AS seq,
       'ALTER TABLE ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + ' ADD ' + pg_get_constraintdef(con.oid)::VARCHAR(1024) + ';' AS ddl
    FROM pg_constraint AS con
      INNER JOIN pg_class AS c
             ON c.relnamespace = con.connamespace
             AND c.oid = con.conrelid
      INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
    AND con.contype = 'f'
    ORDER BY seq
  )
 ORDER BY schemaname, tablename, seq
 ) where seq > 0 and tableowner > 1);"""