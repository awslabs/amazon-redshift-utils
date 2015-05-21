# Redshift Admin Views 
Views objective is to help on Administration of Redshift
All views assume you have a schema called admin.

| View | Purpose |
| ------------- | ------------- |
| v_check_data_distribution.sql |   View to get data distribution across slices | 
| v_constraint_dependency.sql |   View to get the the foreign key constraints between tables | 
| v_generate_group_ddl.sql |   View to get the DDL for a group. | 
| v_generate_schema_ddl.sql |   View to get the DDL for schemas. | 
| v_generate_tbl_ddl.sql | View to get the DDL for a table.  This will contain the distkey, sortkey, constraints |
| v_generate_unload_copy_cmd.sql |   View to get that will generate unload and copy commands for an object.  After running | 
| v_generate_user_object_permissions.sql |   View to get the DDL for a users permissions to tables and views. | 
| v_generate_view_ddl.sql |   View to get the DDL for a view. | 
| v_get_obj_priv_by_user.sql |   View to get the table/views that a user has access to | 
| v_get_schema_priv_by_user.sql |   View to get the schema that a user has access to | 
| v_get_tbl_priv_by_user.sql |   View to get the tables that a user has access to | 
| v_get_users_in_group.sql |   View to get all users in a group | 
| v_get_view_priv_by_user.sql |   View to get the views that a user has access to | 
| v_object_dependency.sql |   A view to merge the different dependency views together | 
| v_space_used_per_tbl.sql |   View to get pull space used per table | 
| v_view_dependency.sql |   View to get the the names of the views that are dependent other tables/views |
