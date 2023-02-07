SELECT DISTINCT bucket, prefix
FROM SVL_S3LIST WHERE QUERY IN
( SELECT DISTINCT QUERY FROM STL_QUERY
WHERE userid>1
and starttime >= cast('{start}' as datetime)
and starttime <= cast('{end}' as datetime)
AND DATABASE = '{db}')
ORDER BY 1;