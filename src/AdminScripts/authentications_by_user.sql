SELECT
    username,
    authmethod,
    COUNT(1) as cnt
FROM
    stl_connection_log
WHERE
    event = 'authenticated'
GROUP BY
    username, authmethod
ORDER BY
    cnt DESC

