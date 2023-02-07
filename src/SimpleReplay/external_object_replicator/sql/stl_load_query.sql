SELECT DISTINCT trim(filename) AS filename
FROM STL_LOAD_COMMITS
WHERE QUERY IN
(SELECT DISTINCT QUERY FROM STL_QUERY
WHERE  starttime >= cast('{start}' as datetime)
AND  starttime <= cast('{end}' as datetime)
AND DATABASE = '{db}')
OR QUERY IN
(SELECT DISTINCT QUERY FROM STL_QUERY
WHERE starttime >= cast('{start}' as datetime)
AND  starttime <= cast('{end}' as datetime)
AND (querytxt LIKE '%manifest%' OR querytxt LIKE '%Manifest%' OR querytxt LIKE '%MANIFEST%')
AND DATABASE = '{db}')
order by 1;