--DROP VIEW admin.v_generate_tbl_ddl;
/**********************************************************************************************
Purpose: View to get the DDL for a table.  This will contain the distkey, sortkey, constraints,
         not null, defaults, etc.

Notes:   Default view ordering causes foreign keys to be created at the end.
         This is needed due to dependencies of the foreign key constraint and the tables it
         links.  Due to this one should not manually order the output if you are expecting to
         be able to replay the SQL directly from the VIEW query result. It is still possible to
         order if you filter out the FOREIGN KEYS and then apply them later.

         The following filters are useful:
           where ddl not like 'ALTER TABLE %'  -- do not return FOREIGN KEY CONSTRAINTS
           where ddl like 'ALTER TABLE %'      -- only get FOREIGN KEY CONSTRAINTS
           where tablename in ('t1', 't2')     -- only get DDL for specific tables
           where schemaname in ('s1', 's2')    -- only get DDL for specific schemas

         So for example if you want to order DDL on tablename and only want the tables 't1', 't2'
         and 't4' you can do so by using a query like:
           select ddl from (
             (
               select
                 *
               from admin.v_generate_tbl_ddl
               where ddl not like 'ALTER TABLE %'
               order by tablename
             )
             UNION ALL
             (
               select
                 *
               from admin.v_generate_tbl_ddl
               where ddl like 'ALTER TABLE %'
               order by tablename
             )
           ) where tablename in ('t1', 't2', 't4');

History:
2014-02-10 jjschmit Created
2015-05-18 ericfe Added support for Interleaved sortkey
2015-10-31 ericfe Added cast tp increase size of returning constraint name
2016-05-24 chriz-bigdata Added support for BACKUP NO tables
2017-05-03 pvbouwel Change table & schemaname of Foreign key constraints to allow for filters
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_generate_tbl_ddl
AS
SELECT
 REGEXP_REPLACE (schemaname, '^zzzzzzzz', '') AS schemaname
 ,REGEXP_REPLACE (tablename, '^zzzzzzzz', '') AS tablename
 ,seq
 ,ddl
FROM
 (
 SELECT
  schemaname
  ,tablename
  ,seq
  ,ddl
 FROM
  (
  --DROP TABLE
  SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,0 AS seq
   ,'--DROP TABLE "' + n.nspname + '"."' + c.relname + '";' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --CREATE TABLE
  UNION SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,2 AS seq
   ,'CREATE TABLE IF NOT EXISTS "' + n.nspname + '"."' + c.relname + '"' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --OPEN PAREN COLUMN LIST
  UNION SELECT n.nspname AS schemaname, c.relname AS tablename, 5 AS seq, '(' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --COLUMN LIST
  UNION SELECT
   schemaname
   ,tablename
   ,seq
   ,'\t' + col_delim + col_name + ' ' + col_datatype + ' ' + col_nullable + ' ' + col_default + ' ' + col_encoding AS ddl
  FROM
   (
   SELECT
    n.nspname AS schemaname
    ,c.relname AS tablename
    ,100000000 + a.attnum AS seq
    ,CASE WHEN a.attnum > 1 THEN ',' ELSE '' END AS col_delim
    ,'"' + a.attname + '"' AS col_name
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
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,200000000 + CAST(con.oid AS INT) AS seq
   ,'\t,' + pg_get_constraintdef(con.oid) AS ddl
  FROM pg_constraint AS con
  INNER JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.oid = con.conrelid
  INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' AND pg_get_constraintdef(con.oid) NOT LIKE 'FOREIGN KEY%'
  ORDER BY seq)
  --CLOSE PAREN COLUMN LIST
  UNION SELECT n.nspname AS schemaname, c.relname AS tablename, 299999999 AS seq, ')' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --BACKUP
  UNION SELECT
  n.nspname AS schemaname
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
  n.nspname AS schemaname
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
   n.nspname AS schemaname
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
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,400000000 + a.attnum AS seq
   ,'DISTKEY ("' + a.attname + '")' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND a.attisdistkey IS TRUE
    AND a.attnum > 0
  --SORTKEY COLUMNS
  UNION select schemaname, tablename, seq,
       case when min_sort <0 then 'INTERLEAVED SORTKEY (' else 'SORTKEY (' end as ddl
from (SELECT
   n.nspname AS schemaname
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
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,500000000 + abs(a.attsortkeyord) AS seq
   ,CASE WHEN abs(a.attsortkeyord) = 1
    THEN '\t"' + a.attname + '"'
    ELSE '\t, "' + a.attname + '"'
    END AS ddl
  FROM  pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND abs(a.attsortkeyord) > 0
    AND a.attnum > 0
  ORDER BY abs(a.attsortkeyord))
  UNION SELECT
   n.nspname AS schemaname
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
  UNION SELECT n.nspname AS schemaname, c.relname AS tablename, 600000000 AS seq, ';' AS ddl
  FROM  pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' )
  UNION (
    SELECT 'zzzzzzzz' || n.nspname AS schemaname,
       'zzzzzzzz' || c.relname AS tablename,
       700000000 + CAST(con.oid AS INT) AS seq,
       'ALTER TABLE ' + n.nspname + '.' + c.relname + ' ADD ' + pg_get_constraintdef(con.oid)::VARCHAR(1024) + ';' AS ddl
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
 )
;
