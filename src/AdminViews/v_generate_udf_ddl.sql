--DROP VIEW admin.v_generate_udf_ddl;
/**********************************************************************************************
Purpose: View to get the DDL for a UDF. 
History:
2016-04-20 chriz-bigdata Created
2018-01-15 pvbouwel      Add QUOTE_IDENT for identifiers (function name)
2018-01-24 joeharris76   Support for SQL functions
2019-04-03 adedotua      Added schemaname, ending semi-colon and 'OR REPLACE' 
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_generate_udf_ddl
AS
WITH arguments AS (SELECT oid, i, arg_name[i] as argument_name, arg_types[i-1] argument_type
FROM (
  SELECT generate_series(1, arg_count) AS i, arg_name, arg_types,oid
  FROM (SELECT oid, proargnames arg_name, proargtypes arg_types, pronargs arg_count from pg_proc where proowner != 1) t
) t)
SELECT 
	schemaname,
	udfname,
	seq,
	trim(ddl) ddl FROM (
SELECT 
   n.nspname AS schemaname,
   p.proname AS udfname,
   p.oid AS udfoid,
1000 as seq, ('CREATE OR REPLACE FUNCTION ' || QUOTE_IDENT(n.nspname) ||'.'|| QUOTE_IDENT(p.proname) || ' \(')::varchar(max) as ddl
FROM pg_proc p
LEFT JOIN pg_namespace n on n.oid = p.pronamespace
WHERE p.proowner != 1
UNION ALL
SELECT 
   n.nspname AS schemaname,
   p.proname AS udfname,
   p.oid AS udfoid,
2000+nvl(i,0) as seq, case when i = 1 then NVL(argument_name,'') || ' ' || format_type(argument_type,null) else ',' || NVL(argument_name,'') || ' ' || format_type(argument_type,null) end as ddl
FROM pg_proc p
LEFT JOIN pg_namespace n on n.oid = p.pronamespace
LEFT JOIN arguments a on a.oid = p.oid
WHERE p.proowner != 1
UNION ALL
SELECT 
   n.nspname AS schemaname,
   p.proname AS udfname,
   p.oid AS udfoid,
3000 as seq, '\)' as ddl
FROM pg_proc p
LEFT JOIN pg_namespace n on n.oid = p.pronamespace
WHERE p.proowner != 1
UNION ALL
SELECT 
   n.nspname AS schemaname,
   p.proname AS udfname,
   p.oid AS udfoid,
 4000 as seq, '  RETURNS ' || pg_catalog.format_type(p.prorettype, NULL) as ddl
FROM pg_proc p
LEFT JOIN pg_namespace n on n.oid = p.pronamespace
WHERE p.proowner != 1
UNION ALL
SELECT 
   n.nspname AS schemaname,
   p.proname AS udfname,
      p.oid AS udfoid,
5000 AS seq, CASE WHEN p.provolatile = 'v' THEN 'VOLATILE' WHEN p.provolatile = 's' THEN 'STABLE' WHEN p.provolatile = 'i' THEN 'IMMUTABLE' ELSE '' END as ddl
FROM pg_proc p
LEFT JOIN pg_namespace n on n.oid = p.pronamespace
WHERE p.proowner != 1
UNION ALL
SELECT 
   n.nspname AS schemaname,
   p.proname AS udfname,
      p.oid AS udfoid,
6000 AS seq, 'AS $$' as ddl
FROM pg_proc p
LEFT JOIN pg_namespace n on n.oid = p.pronamespace
WHERE p.proowner != 1
UNION ALL
SELECT 
   n.nspname AS schemaname,
   p.proname AS udfname,
      p.oid AS udfoid,
7000 AS seq, p.prosrc as DDL
FROM pg_proc p
LEFT JOIN pg_namespace n on n.oid = p.pronamespace
WHERE p.proowner != 1
UNION ALL
SELECT 
   n.nspname AS schemaname,
   p.proname AS udfname,
      p.oid AS udfoid,
8000 as seq, '$$ LANGUAGE ' + lang.lanname + ';' as ddl
FROM pg_proc p
LEFT JOIN pg_namespace n on n.oid = p.pronamespace
LEFT JOIN (select oid, lanname FROM pg_language) lang on p.prolang = lang.oid
WHERE p.proowner != 1
)
ORDER BY udfoid,seq;
