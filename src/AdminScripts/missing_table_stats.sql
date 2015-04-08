/* Query shows EXPLAIN plans which flagged "missing statistics" on the underlying tables */
SELECT substring(trim(plannode),1,100) AS plannode
       ,COUNT(*)
FROM stl_explain
WHERE plannode LIKE '%missing statistics%'
GROUP BY plannode
ORDER BY 2 DESC;
