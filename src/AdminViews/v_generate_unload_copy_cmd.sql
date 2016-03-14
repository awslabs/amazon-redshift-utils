--DROP VIEW admin.v_generate_unload_copy_cmd;
/**********************************************************************************************
Purpose: View to get that will generate unload and copy commands for an object.  After running
	the view the user will need to fill in what filter to use in the UNLOAD query if any
	(--WHERE audit_id > ___auditid___), the bucket location (__bucketname__) and the AWS
	credentials (__creds_here__).  The where clause is commented out currently and can
	be left so if the UNLOAD needs to get all data of the table.
History:
2014-02-12 jjschmit Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_generate_unload_copy_cmd
AS
SELECT
	schemaname
	,tablename
	,cmd_type
	,dml
FROM
	(
	SELECT 
		schemaname
		,tablename
		,'unload' AS cmd_type
		,'UNLOAD (''SELECT * FROM ' + schemaname + '.' + tablename + ' --WHERE audit_id > ___auditid___'') TO ''s3://__bucketname__/' + TO_CHAR(GETDATE(), 'YYYYMMDD_HH24MISSMS')  + '/'  + schemaname + '.' + tablename + '-'' CREDENTIALS ''__creds_here__'' GZIP DELIMITER ''\\t'';' AS dml
	FROM 
		pg_tables
WHERE schemaname NOT IN ('pg_internal') 
	UNION ALL
	SELECT 
		schemaname
		,tablename
		,'copy' AS cmd_type
		,'COPY ' + schemaname + '.' + tablename + ' FROM ''s3://__bucketname__/' + TO_CHAR(GETDATE(), 'YYYYMMDD_HH24MISSMS')  + '/'  + schemaname + '.' + tablename + '-'' CREDENTIALS ''__creds_here__'' GZIP DELIMITER ''\\t'';' AS copy_dml
	FROM 
		pg_tables 
	)
WHERE schemaname NOT IN ('pg_internal')
ORDER BY 3 DESC,1,2
;
