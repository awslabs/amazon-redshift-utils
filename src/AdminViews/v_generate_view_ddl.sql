--DROP VIEW admin.v_generate_view_ddl;
/**********************************************************************************************
Purpose: View to get the DDL for a view.  
History:
2014-02-10 jjschmit Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_generate_view_ddl
AS
SELECT 
	n.nspname AS schemaname
	,c.relname AS viewname
	,'--DROP VIEW ' + n.nspname + '.' + c.relname + ';\nCREATE OR REPLACE VIEW ' + n.nspname + '.' + c.relname + ' AS\n' + COALESCE(pg_get_viewdef(c.oid, TRUE), '') AS ddl
FROM 
	pg_catalog.pg_class AS c
INNER JOIN
	pg_catalog.pg_namespace AS n
		ON c.relnamespace = n.oid
WHERE relkind = 'v'
;
