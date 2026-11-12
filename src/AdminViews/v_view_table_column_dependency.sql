--DROP VIEW admin.v_view_table_column_dependency;
/*************************************************************
Purpose: View to get the views and materialized views that
    depend on specific columns to migrate them away from using
    this column.

    Regular views record their dependency against their rewrite
    rule; materialized views record it directly against the MV,
    so each has its own branch. Late-binding views (WITH NO
    SCHEMA BINDING) record no dependencies and don't appear.
    Automated materialized views (AutoMV, schema pg_automv)
    are excluded.

Usage: To find views depending on 'my_col' of table 'my_tbl'
       SELECT * FROM admin.v_view_table_column_dependency
       WHERE tablename='my_tbl' AND columnname='my_col';

History:
2021-04-27 pvbouwel Created
2026-11-06 nickbarc Added materialized views, and the viewschema,
    tableschema and viewtype columns
*************************************************************/
CREATE OR REPLACE VIEW admin.v_view_table_column_dependency
AS
SELECT
  pc1.relname AS viewname
  , pc2.relname AS tablename
  , pa.attname AS columnname
  , pn1.nspname AS viewschema
  , pn2.nspname AS tableschema
  , 'view'::VARCHAR(17) AS viewtype
FROM pg_depend pd
JOIN pg_rewrite rw ON rw.oid = pd.objid
JOIN pg_class pc1 ON pc1.oid = rw.ev_class
JOIN pg_namespace pn1 ON pn1.oid = pc1.relnamespace
JOIN pg_class pc2 ON pc2.oid = pd.refobjid
JOIN pg_namespace pn2 ON pn2.oid = pc2.relnamespace
JOIN pg_attribute pa ON pa.attrelid = pd.refobjid
  AND pa.attnum = pd.refobjsubid
  AND pa.attnum>0 -- Only user columns
WHERE classid=(SELECT oid FROM pg_class WHERE relname='pg_rewrite') -- dependency from rewriter rule
  AND refclassid=(SELECT oid FROM pg_class WHERE relname='pg_class') -- dependency on a relation
  AND pc2.relowner > 1 -- Only dependencies on non-system relations
UNION ALL
SELECT
  pc1.relname AS viewname
  , pc2.relname AS tablename
  , pa.attname AS columnname
  , pn1.nspname AS viewschema
  , pn2.nspname AS tableschema
  , 'materialized view'::VARCHAR(17) AS viewtype
FROM pg_depend pd
JOIN pg_class pc1 ON pc1.oid = pd.objid
JOIN pg_namespace pn1 ON pn1.oid = pc1.relnamespace
JOIN pg_class pc2 ON pc2.oid = pd.refobjid
JOIN pg_namespace pn2 ON pn2.oid = pc2.relnamespace
JOIN pg_attribute pa ON pa.attrelid = pd.refobjid
  AND pa.attnum = pd.refobjsubid
  AND pa.attnum>0 -- Only user columns
WHERE classid=(SELECT oid FROM pg_class WHERE relname='pg_class') -- dependency from a relation (materialized view)
  AND refclassid=(SELECT oid FROM pg_class WHERE relname='pg_class') -- dependency on a relation
  AND pc1.relkind = 'v' -- Only materialized views
  AND pn1.nspname <> 'pg_automv' -- Skip automated materialized views (AutoMV)
  AND pc2.relowner > 1 -- Only dependencies on non-system relations
;
