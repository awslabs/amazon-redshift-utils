--DROP VIEW admin.v_constraint_dependency;
/**********************************************************************************************
Purpose: View to get the the foreign key constraints between tables
    not null, defaults, etc.
History:
2014-02-11 jjschmit Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_constraint_dependency
AS
SELECT DISTINCT
    srcobj.oid AS src_oid
    ,srcnsp.nspname AS src_schemaname
    ,srcobj.relname AS src_objectname
    ,tgtobj.oid AS dependent_oid
    ,tgtnsp.nspname AS dependent_schemaname
    ,tgtobj.relname AS dependent_objectname
    ,con.conname AS constraint_name
FROM
    pg_catalog.pg_class AS srcobj
INNER JOIN
    pg_catalog.pg_namespace AS srcnsp
        ON srcobj.relnamespace = srcnsp.oid
INNER JOIN
    pg_catalog.pg_constraint AS con
        ON srcobj.oid = con.confrelid
INNER JOIN
    pg_catalog.pg_class AS tgtobj
        ON tgtobj.oid = con.conrelid
INNER JOIN
    pg_catalog.pg_namespace AS tgtnsp
        ON tgtobj.relnamespace = tgtnsp.oid
;
