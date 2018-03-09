--DROP VIEW admin.v_generate_schema_ddl;
/**********************************************************************************************
Purpose: View to get the DDL for schemas.  
History:
2014-02-11 jjschmit Created
2018-01-15 pvbouwel Add QUOTE_IDENT for namespace literal
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_generate_schema_ddl
AS
SELECT nspname AS schemaname, 'CREATE SCHEMA ' + QUOTE_IDENT(nspname) + ';' AS ddl FROM pg_catalog.pg_namespace WHERE nspowner >= 100 ORDER BY nspname
;


