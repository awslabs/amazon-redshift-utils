--DROP VIEW admin.v_generate_schema_ddl;
/**********************************************************************************************
Purpose: View to get the DDL for schemas.  
History:
2014-02-11 jjschmit Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_generate_schema_ddl
AS
SELECT nspname AS schemaname, 'CREATE SCHEMA "' + nspname + '";' AS ddl FROM pg_catalog.pg_namespace WHERE nspowner >= 100 ORDER BY nspname
;


