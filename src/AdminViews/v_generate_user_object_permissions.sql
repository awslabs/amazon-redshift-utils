--DROP VIEW admin.v_generate_user_object_permissions;
/**********************************************************************************************
Purpose: View to get the DDL for a users permissions to tables and views.  
History:
2014-02-12 jjschmit Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_generate_user_object_permissions
AS
SELECT 
	schemaname
	,objectname
	,usename
	,REVERSE(SUBSTRING(REVERSE(CASE WHEN sel IS TRUE THEN 'GRANT SELECT ON ' + QUOTE_IDENT(schemaname) + '.' + QUOTE_IDENT(objectname) + ' TO ' + usename + ';\n' ELSE '' END +
		CASE WHEN ins IS TRUE THEN 'GRANT INSERT ON ' + QUOTE_IDENT(schemaname) + '.' + QUOTE_IDENT(objectname) + ' TO ' + usename + ';\n' ELSE '' END +
		CASE WHEN upd IS TRUE THEN 'GRANT UPDATE ON ' + QUOTE_IDENT(schemaname) + '.' + QUOTE_IDENT(objectname) + ' TO ' + usename + ';\n' ELSE '' END +
		CASE WHEN del IS TRUE THEN 'GRANT DELETE ON ' + QUOTE_IDENT(schemaname) + '.' + QUOTE_IDENT(objectname) + ' TO ' + usename + ';\n' ELSE '' END +
		CASE WHEN ref IS TRUE THEN 'GRANT REFERENCES ON ' + QUOTE_IDENT(schemaname) + '.' + QUOTE_IDENT(objectname) + ' TO ' + usename + ';\n' ELSE '' END), 2)) AS ddl
FROM
	admin.v_get_obj_priv_by_user
;
