--DROP VIEW admin.v_view_table_column_dependency;
/*************************************************************
Purpose: View to get the the views that depend on specific
    columns to migrate them away from using this column. 

Usage: To find views depending on 'my_col' of table 'my_tbl'
       SELECT * FROM admin.v_view_table_column_dependency
       WHERE tablename='my_tbl' AND columnname='my_col';

History:
2021-04-27 pvbouwel Created
*************************************************************/
CREATE OR REPLACE VIEW admin.v_view_table_column_dependency
AS
SELECT
  pc1.relname AS viewname
  , pc2.relname AS tablename
  , pa.attname AS columnname
FROM pg_depend pd
JOIN pg_rewrite rw ON rw.oid = pd.objid
JOIN pg_class pc1 ON pc1.oid = rw.ev_class
JOIN pg_class pc2 ON pc2.oid = pd.refobjid
JOIN pg_attribute pa ON pa.attrelid = pd.refobjid
  AND pa.attnum = pd.refobjsubid
  AND pa.attnum>0 -- Only user columns
WHERE classid=(SELECT oid FROM pg_class WHERE relname='pg_rewrite') -- dependency from rewriter rule
  AND refclassid=(SELECT oid FROM pg_class WHERE relname='pg_class') -- dependency on a relation
  AND pc2.relowner > 1 -- Only dependencies on non-system relations
;
