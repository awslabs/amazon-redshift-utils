
--DROP VIEW admin.v_cost_per_schema;

/**********************************************************************************************
Purpose: View to get monthly cost of the Redshift cluster divided to every schema

This code is configured to view the costs of a Redshift cluster with 20 dc1.large nodes.
The cost of such Redshift cluster (without reserved nodes) is 22857.32$ (http://calculator.s3.amazonaws.com/index.html)
And the size is 3200000MB (https://aws.amazon.com/redshift/pricing)
The query takes into account the free space that is not being used.

You should modify the number of the cost and size in the query to suit your use case.

The view depands on the admin.v_space_used_per_tbl view
(https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_space_used_per_tbl.sql).

History:
2015-09-25 kerbelp Created
**********************************************************************************************/

CREATE OR REPLACE VIEW admin.v_cost_per_schema
(
  dbase_name,
  schemaname,
  usage_megabytes,
  usage_percentage_with_free_space,
  monthly_usage_cost
)
AS
 SELECT t.dbase_name, t.schemaname, sum(t.megabytes) AS usage_megabytes, sum(t.totalsum_actual_usage_percentage) AS usage_percentage_with_free_space, sum(t.monthly_actual_usage_cost) AS monthly_usage_cost
   FROM ( SELECT t.dbase_name, t.schemaname, t.tablename, t.megabytes, sum(t.megabytes)
          OVER(
          PARTITION BY t.dbase_name
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS totalsum, t.megabytes::double precision / (sum(t.megabytes)
          OVER(
          PARTITION BY t.dbase_name
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))::double precision * 100::double precision AS totalsum_actual_usage_percentage, 3200000 AS total_megabytes, t.megabytes::double precision / (sum(t.megabytes)
          OVER(
          PARTITION BY t.dbase_name
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))::double precision * 22857.32::double precision AS monthly_actual_usage_cost, 22857.32 AS monthly_total_cost
           FROM admin.v_space_used_per_tbl t
          WHERE t.megabytes IS NOT NULL AND t.schemaname <> 'public'::character varying::text AND t.schemaname <> 'admin'::character varying::text) t
  GROUP BY t.dbase_name, t.schemaname, t.totalsum, t.total_megabytes, t.monthly_total_cost
  ORDER BY sum(t.megabytes) DESC;