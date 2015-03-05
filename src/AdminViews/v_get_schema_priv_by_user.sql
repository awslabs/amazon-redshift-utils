/**********************************************************************************************
Purpose: View to get the schema that a user has access to
History:
2013-10-29 jjschmit Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_get_schema_priv_by_user
AS
SELECT
	* 
FROM 
	(
	SELECT 
		schemaname
		,usename
		,HAS_SCHEMA_PRIVILEGE(usrs.usename, schemaname, 'create') AS cre
		,HAS_SCHEMA_PRIVILEGE(usrs.usename, schemaname, 'usage') AS usg
	FROM
		(SELECT nspname AS schemaname FROM pg_namespace) AS objs
	INNER JOIN
		(SELECT * FROM pg_user) AS usrs
			ON 1 = 1
	ORDER BY schemaname
	)
WHERE cre = true OR usg = true
;
