--DROP VIEW admin.v_object_dependency;
/**********************************************************************************************
Purpose: A view to merge the different dependency views together
History:
2014-02-11 jjschmit Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_object_dependency
AS
SELECT 'view' AS dependency_type, *, NULL AS constraint_name FROM admin.v_view_dependency
UNION
SELECT 'fkey constraint' AS dependency_type, * FROM admin.v_constraint_dependency
;
