--DROP VIEW admin.v_generate_group_ddl;
/**********************************************************************************************
Purpose: View to get the DDL for a group.  
History:
2014-02-11 jjschmit Created
2018-01-15 pvbouwel Add QUOTE_IDENT for group names
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_generate_group_ddl
AS
SELECT groname AS groupname, 'CREATE GROUP ' + QUOTE_IDENT(groname) + ';' AS ddl FROM pg_catalog.pg_group ORDER BY groname
;


