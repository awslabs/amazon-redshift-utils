SELECT *
FROM
    stl_connection_log
WHERE
    username = 'tableauro'
ORDER BY
    recordtime DESC
LIMIT 10000