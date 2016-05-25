/**********************************************************************************************
Purpose: View to get the views that a user has access to
History:
2013-10-29 jjschmit Created
2016-05-24 chriz-bigdata addressed edge case for objects with names containing '.'
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_get_view_priv_by_user
AS
SELECT
	* 
FROM 
	(
	SELECT 
		schemaname
		,viewname
		,usename
		,HAS_TABLE_PRIVILEGE(usrs.usename, obj, 'select') AS sel
		,HAS_TABLE_PRIVILEGE(usrs.usename, obj, 'insert') AS ins
		,HAS_TABLE_PRIVILEGE(usrs.usename, obj, 'update') AS upd
		,HAS_TABLE_PRIVILEGE(usrs.usename, obj, 'delete') AS del
		,HAS_TABLE_PRIVILEGE(usrs.usename, obj, 'references') AS ref
	FROM
		(SELECT schemaname, viewname, QUOTE_IDENT(schemaname) || '.' || QUOTE_IDENT(viewname) AS obj FROM pg_views ) AS objs
	INNER JOIN
		(SELECT * FROM pg_user) AS usrs
			ON 1 = 1
	ORDER BY obj
	)
WHERE sel = true or ins = true or upd = true or del = true or ref = true
;
