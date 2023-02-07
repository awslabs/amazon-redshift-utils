/**********************************************************************************************
Purpose: View to get the tables that a user has access to
History:
2013-10-29 jjschmit Created
2022-08-15 saeedma8 excluded system tables
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_get_tbl_priv_by_user
AS
SELECT
	* 
FROM 
	(
	SELECT 
		schemaname
		,tablename
		,usename
		,HAS_TABLE_PRIVILEGE(usrs.usename, obj, 'select') AS sel
		,HAS_TABLE_PRIVILEGE(usrs.usename, obj, 'insert') AS ins
		,HAS_TABLE_PRIVILEGE(usrs.usename, obj, 'update') AS upd
		,HAS_TABLE_PRIVILEGE(usrs.usename, obj, 'delete') AS del
		,HAS_TABLE_PRIVILEGE(usrs.usename, obj, 'references') AS ref
	FROM
		(SELECT schemaname, tablename, '\"' + schemaname + '\"' + '.' + '\"' + tablename + '\"' AS obj FROM pg_tables where schemaname !~ '^information_schema|catalog_history|pg_') AS objs
		,(SELECT * FROM pg_user) AS usrs
	ORDER BY obj
	)
WHERE sel = true or ins = true or upd = true or del = true or ref = true
;

