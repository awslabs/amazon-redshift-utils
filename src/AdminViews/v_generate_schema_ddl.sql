--DROP VIEW admin.v_generate_schema_ddl;
/**********************************************************************************************
Purpose: View to get the DDL for schemas.  
History:
2014-02-11 jjschmit Created
2018-01-15 pvbouwel Add QUOTE_IDENT for namespace literal
2018-03-30 burck1 Add logic to add AUTHORIZATION clause
Notes:
If you receive the error
	[Amazon](500310) Invalid operation: cannot change data type of view column "ddl";
then you must drop the view and re-create it using
	DROP VIEW admin.v_generate_schema_ddl;
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_generate_schema_ddl
AS
SELECT
	nspname AS schemaname,
	'CREATE SCHEMA ' + QUOTE_IDENT(nspname) +
		CASE
		WHEN nspowner > 100
		THEN ' AUTHORIZATION ' + QUOTE_IDENT(pg_user.usename)
		ELSE ''
		END
		+ ';' AS ddl
FROM pg_catalog.pg_namespace as pg_namespace
LEFT OUTER JOIN pg_catalog.pg_user pg_user
ON pg_namespace.nspowner=pg_user.usesysid
WHERE nspowner >= 100
ORDER BY nspname
;
