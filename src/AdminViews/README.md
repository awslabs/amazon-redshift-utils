# Redshift Admin Views 
Views objective is to help on Administration of Redshift
All views assume you have a schema called admin.

| View | Purpose |
| ------------- | ------------- |
| v_check_data_distribution.sql |   View to get data distribution across slices | 
| v_check_transaction_locks.sql | View to get information about the locks held by open transactions |
| v_check_wlm_query_time.sql | View to get  WLM Queue Wait Time , Execution Time and Total Time by Query for the past 7 Days |
| v_check_wlm_query_trend_daily.sql | View to get  WLM Query Count, Queue Wait Time , Execution Time and Total Time by Day  |
| v_check_wlm_query_trend_hourly.sql | View to get  WLM Query Count, Queue Wait Time , Execution Time and Total Time by Hour |
| v_constraint_dependency.sql |   View to get the the foreign key constraints between tables | 
| v_extended_table_info.sql| View to get extended table information for permanent database tables.
| v_generate_cancel_query.sql | View to get cancel query |
| v_generate_group_ddl.sql |   View to get the DDL for a group. | 
| v_generate_schema_ddl.sql |   View to get the DDL for schemas. | 
| v_generate_tbl_ddl.sql | View to get the DDL for a table.  This will contain the distkey, sortkey, constraints |
| v_generate_terminate_session.sql | View to get pg_terminate_backend() statements |
| v_generate_udf_ddl.sql | View to get the DDL for a UDF.
| v_generate_unload_copy_cmd.sql |   View to get that will generate unload and copy commands for an object.  After running | 
|v_generate_user_grant_revoke_ddl.sql| View to gengerate grant or revoke ddl for users and groups in the database|
| v_generate_user_object_permissions.sql |   View to get the DDL for a users permissions to tables and views. | 
| v_generate_view_ddl.sql |   View to get the DDL for a view. | 
| v_get_blocking_locks.sql | View to identify blocking locks as well as determine what/who is blocking a query |
| v_get_cluster_restart_ts.sql | View to get the datetime of when Redshift cluster was recently restarted |
| v_get_obj_priv_by_user.sql |   View to get the table/views that a user has access to | 
| v_get_schema_priv_by_user.sql |   View to get the schema that a user has access to | 
| v_get_tbl_priv_by_user.sql |   View to get the tables that a user has access to | 
| v_get_tbl_reads_and_writes.sql | View to get operations performed per table for transactions ID or query ID |
| v_get_tbl_scan_frequency.sql |   View to get list of each permanent table's scan frequency | 
| v_get_users_in_group.sql |   View to get all users in a group | 
| v_get_vacuum_details.sql | View to get vacuum details like table name, Schema Name, Deleted Rows , processing time |
| v_get_view_priv_by_user.sql |   View to get the views that a user has access to | 
| v_object_dependency.sql |   View to merge the different dependency views together | 
| v_open_session.sql |   View to monitor currently connected and disconnected sessions | 
| v_session_leakage_by_cnt.sql |   View to monitor session leakage by remote host |   
| v_space_used_per_tbl.sql |   View to get pull space used per table | 
| v_view_dependency.sql |   View to get the names of the views that are dependent other tables/views |
