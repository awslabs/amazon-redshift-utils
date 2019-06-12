--DROP VIEW admin.v_generate_view_ddl;
/**********************************************************************************************
Purpose: View to get the DDL for a view.  
History:
2014-02-10 jjschmit Created
2018-01-15 pvbouwel Replace tabs and add QUOTE_IDENT for identifiers (schema and view names)
2018-08-03 alexlsts Included CASE to check for late binding view 
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_generate_view_ddl
AS
SELECT 
    n.nspname AS schemaname
    ,c.relname AS viewname
    ,'--DROP VIEW ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + ';\n'
    + CASE 
     	WHEN c.relnatts > 0 then 'CREATE OR REPLACE VIEW ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + ' AS\n' + COALESCE(pg_get_viewdef(c.oid, TRUE), '')
     	ELSE  COALESCE(pg_get_viewdef(c.oid, TRUE), '') END AS ddl
FROM 
    pg_catalog.pg_class AS c
INNER JOIN
    pg_catalog.pg_namespace AS n
    ON c.relnamespace = n.oid
WHERE relkind = 'v';