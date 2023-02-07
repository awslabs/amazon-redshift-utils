--DROP VIEW admin.v_generate_view_ddl;
/**********************************************************************************************
Purpose: View to get the DDL for a view.  
History:
2014-02-10 jjschmit Created
2018-01-15 pvbouwel Replace tabs and add QUOTE_IDENT for identifiers (schema and view names)
2018-08-03 alexlsts Included CASE to check for late binding view 
2021-04-23 pvbouwel Replace logic to identify different cases to support materialized views.
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_generate_view_ddl
AS
SELECT 
    n.nspname AS schemaname
    ,c.relname AS viewname
    ,'--DROP '
    ||  
    CASE STRPOS(LOWER(pg_get_viewdef(c.oid, TRUE)), 'materialized')
      WHEN 8 THEN 'MATERIALIZED '::text --CREATE MATERIALIZED would be the start
      ELSE ''::text
    END 
    ||
    'VIEW ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + ';\n'
    + CASE STRPOS(LOWER(pg_get_viewdef(c.oid, TRUE)), 'create')
        WHEN 1 then '' -- CREATE statement already present
     	ELSE           --no CREATE statement present so no materialized view anyway
          'CREATE OR REPLACE VIEW ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + ' AS\n'
      END || COALESCE(pg_get_viewdef(c.oid, TRUE), '') AS ddl
FROM 
    pg_catalog.pg_class AS c
INNER JOIN
    pg_catalog.pg_namespace AS n
    ON c.relnamespace = n.oid
WHERE relkind = 'v';
