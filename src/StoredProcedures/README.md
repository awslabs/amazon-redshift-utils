# Redshift Stored Procedures
Sample and Usefull Stored Procedures

| Procedure                                                    | Description                                                                           |
| -------------------------------------------------------------| --------------------------------------------------------------------------------------|
| [`sp_analyze_minimal.sql`](sp_analyze_minimal.sql)           | Analyze **one** column of a table. To be used on a staging table right after loading  |
| [`sp_check_primary_key.sql`](sp_check_primary_key.sql)       | Check the integrity of the PRIMARY KEY declared on a table                            |
| [`sp_connect_by_prior.sql`](sp_connect_by_prior.sql)         | Calculate levels in a nested hierarchy tree                                           |
| [`sp_controlled_access.sql`](sp_controlled_access.sql)       | Provide controlled access to data without granting permission on the table/view       |
| [`sp_pivot_for.sql`](sp_pivot_for.sql)                       | Transpose row values into columns                                                     |
| [`sp_split_table_by_range.sql`](sp_split_table_by_range.sql) | Split a large table into parts using a numeric column                                 |
| [`sp_sync_get_new_rows.sql`](sp_sync_get_new_rows.sql)       | Sync new rows from a source table and insert them into a target table                 |
| [`sp_sync_merge_changes.sql`](sp_sync_merge_changes.sql)     | Sync new and changed rows from a source table and merge them into a target table      |
