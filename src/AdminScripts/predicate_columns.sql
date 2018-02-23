/**********************************************************************************************
Purpose: Return Information about columns that are predicate

Columns:
schema_name				Schema Name
table_name				Table Name
col_num					Column Number
col_name				Column Name
is_predicate			Indicates column is a predicate column
first_predicate_use		First Use as predicate column
last_analyze			Last Time column was analyzed
pct_null				Percent of null values
avg_width				Average width of the column
n_distinct				Number of distinct values
is_distkey				Indicate if column is distribution key
is_sortkey				Indicates if column is part of sort key


Notes:
Use the table_name filter to narrow the results


History:
2015-02-09 ericfe created
2015-11-20 ericfe filter off nodeid 0 rows and non proper filter plan info
**********************************************************************************************/
WITH predicate_column_info as (
SELECT ns.nspname AS schema_name, c.relname AS table_name, a.attnum as col_num,  a.attname as col_name, s.stanullfrac as pct_null, 
s.stawidth as avg_width, case when s.stadistinct < 0 then null else s.stadistinct end as n_distinct,
       a.attisdistkey, a.attsortkeyord,
        CASE
            WHEN 10002 = s.stakind1 THEN array_to_string(stavalues1, '||')
            WHEN 10002 = s.stakind2 THEN array_to_string(stavalues2, '||')
            WHEN 10002 = s.stakind3 THEN array_to_string(stavalues3, '||')
            WHEN 10002 = s.stakind4 THEN array_to_string(stavalues4, '||')
            ELSE NULL::varchar
        END AS pred_ts
   FROM pg_statistic s
   JOIN pg_class c ON c.oid = s.starelid
   JOIN pg_namespace ns ON c.relnamespace = ns.oid
   JOIN pg_attribute a ON c.oid = a.attrelid AND a.attnum = s.staattnum)
SELECT schema_name, table_name, col_num, col_name,
       pred_ts NOT LIKE '2000-01-01%' AS is_predicate,
       CASE WHEN pred_ts NOT LIKE '2000-01-01%' THEN (split_part(pred_ts, '||',1))::timestamp ELSE NULL::timestamp END as first_predicate_use,
       CASE WHEN pred_ts NOT LIKE '%||2000-01-01%' THEN (split_part(pred_ts, '||',2))::timestamp ELSE NULL::timestamp END as last_analyze,
pct_null, avg_width, n_distinct, attisdistkey as is_distkey, attsortkeyord as is_sortkey
FROM predicate_column_info
where pred_ts NOT LIKE '2000-01-01%'
-- and table_name like 'fact_visits%'
order by schema_name, table_name, col_num;
