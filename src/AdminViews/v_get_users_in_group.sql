/**********************************************************************************************
Purpose: View to get all users in a group
History:
2013-10-29 jjschmit Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_get_users_in_group
AS
SELECT 
	pg_group.groname
	,pg_group.grosysid
	,pg_user.*
FROM pg_group, pg_user 
WHERE pg_user.usesysid = ANY(pg_group.grolist) 
ORDER BY 1,2 
;
