--DROP VIEW admin.v_object_owners;
/**********************************************************************************************
Purpose: View to get tables and views owners
History:
2016-10-06 Antoine Augusti Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_object_owners as
SELECT
'table' object_type,
schemaname,
tablename object_name,
tableowner object_owner
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_internal')
UNION
SELECT
'view' object_type,
schemaname,
viewname object_name,
viewowner object_owner
FROM pg_views
WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_internal')
