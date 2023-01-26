SELECT DISTINCT
cast(q.database as varchar(100)) as rs_db
,cast(v.table_schema as varchar(100)) as rs_external_schema
,cast(v.table_name as varchar(100)) as external_table
,cast(es.databasename as varchar(100)) as glue_databasename
FROM stl_query q
JOIN svl_s3query_summary s
ON q.query = s.query
JOIN svv_tables v
on s.external_table_name like '%' + v.table_schema + '%'
and s.external_table_name like '%' + v.table_catalog + '%'
and s.external_table_name like '%' + v.table_name + '%'
join svv_external_schemas es
on es.schemaname like v.table_schema
WHERE q.userid > 1 and v.table_type = 'EXTERNAL TABLE'
and q.starttime >= cast('{start}' as datetime)
and q.starttime <= cast('{end}' as datetime)
AND q.DATABASE = '{db}';