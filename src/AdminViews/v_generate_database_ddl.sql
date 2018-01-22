--DROP VIEW admin.v_generate_database_ddl;
/**********************************************************************************************
Purpose: View to get the DDL for a database 
History:
2018-01-20 pvbouwel Create the view
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_generate_database_ddl
AS
SELECT
  datname as datname,
  'CREATE DATABASE ' + QUOTE_IDENT(datname) + ' WITH CONNECTION LIMIT ' + datconnlimit + ';' AS ddl
FROM pg_catalog.pg_database_info
WHERE datdba >= 100
ORDER BY datname
;

