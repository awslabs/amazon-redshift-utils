CREATE OR REPLACE VIEW admin.v_session_leakage_by_cnt
AS 
SELECT 
	i.remotehost
	,i.username
	,i.eventcount AS connects
	,d.eventcount AS disconnects
FROM 
	( 
	SELECT 
		remotehost
		,username
		,COUNT(*) AS eventcount
	FROM 
		stl_connection_log
     WHERE event = 'initiating session'
	GROUP BY remotehost, username
	) AS i
LEFT OUTER JOIN 
	( 
	SELECT 
		remotehost
		,username
		,COUNT(*) AS eventcount
	FROM 
		stl_connection_log
	WHERE event = 'disconnecting session'
     GROUP BY remotehost, username
     ) AS d 
     	ON i.remotehost = d.remotehost 
     	AND i.username = d.username
ORDER BY i.eventcount - COALESCE(d.eventcount, 0) DESC;