/* Query shows EXPLAIN plans which flagged "missing statistics" on the underlying tables */
SELECT substring(trim(plannode),1,100) AS plannode
       ,COUNT(*)
FROM stl_explain
WHERE plannode LIKE '%missing statistics%'
AND plannode NOT LIKE '%redshift_auto_health_check_%'
GROUP BY plannode
ORDER BY 2 DESC;
