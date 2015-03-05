CREATE OR REPLACE VIEW admin.v_open_session
AS
SELECT
	CASE WHEN disc.recordtime IS NULL THEN 'Y' ELSE 'N' END AS connected
	,init.recordtime AS conn_recordtime
	,disc.recordtime AS disconn_recordtime
	,init.pid AS pid
	,init.remotehost
	,init.remoteport
	,init.username AS username
	,disc.duration AS conn_duration
FROM 
	(SELECT event, recordtime, remotehost, remoteport, pid, username FROM stl_connection_log WHERE event = 'initiating session') AS init
LEFT OUTER JOIN
	(SELECT event, recordtime, remotehost, remoteport, pid, username, duration FROM stl_connection_log WHERE event = 'disconnecting session') AS disc
		ON init.pid = disc.pid
		AND init.remotehost = disc.remotehost
		AND init.remoteport = disc.remoteport
;