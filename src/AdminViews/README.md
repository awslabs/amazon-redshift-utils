# Redshift Admin Views 
Views objective is to help with administration of Redshift.
All views assume you have a schema called admin.

| View | Purpose |
| ------------- | ------------- |
| v\_check\_data\_distribution.sql | View to get data distribution across slices. |
| v\_check\_transaction\_locks.sql | View to get information about the locks held by open transactions. |
| v\_check\_wlm\_query\_time.sql | View to get  WLM Queue Wait Time , Execution Time and Total Time by Query for the past 7 Days. |
| v\_check\_wlm\_query\_trend\_daily.sql | View to get WLM Query Count, Queue Wait Time , Execution Time and Total Time by Day.  |
| v\_check\_wlm\_query\_trend\_hourly.sql | View to get WLM Query Count, Queue Wait Time , Execution Time and Total Time by Hour. |
| v\_connection\_summary.sql | View to flatten stl_connection_log table and provide details like session start and end time, duration in human readble format and current state i.e disconnected, terminated by admin, active or connection lost. |
| v\_constraint\_dependency.sql | View to get the the foreign key constraints between tables. |
| v\_extended\_table\_info.sql| View to get extended table information for permanent database tables. |
| v_find_dropuser_objs.sql | View to help find all objects owned by the user to be dropped. |
| v\_fragmentation\_info.sql| View to list all fragmented tables in the database. |
| v\_generate\_cancel\_query.sql | View to get cancel query. |
| v\_generate\_cursor\_query.sql | View to get the query and statistics of the currently active cursors. | 
| v\_generate\_database\_ddl.sql | View to get the DDL for a database. |
| v\_generate\_external\_tbl\_ddl.sql | View to get the DDL for an external table. |
| v\_generate\_group\_ddl.sql | View to get the DDL for a group. |
| v\_generate\_schema\_ddl.sql | View to get the DDL for schemas. |
| v\_generate\_tbl\_ddl.sql | View to get the DDL for a table.  This will contain the distkey, sortkey, constraints. |
| v\_generate\_terminate\_session.sql | View to get pg\_terminate\_backend() statements. |
| v\_generate\_udf\_ddl.sql | View to get the DDL for a UDF. |
| v\_generate\_unload\_copy\_cmd.sql | View to get that will generate unload and copy commands for an object.  After running the view the user will need to fill in what filter to use in the UNLOAD query if any (--WHERE audit_id > `___auditid___`), the bucket location (`__bucketname__`) and the AWS credentials (`__creds_here__`).  The where clause is commented out currently and can be left so if the UNLOAD needs to get all data of the table. | 
| v\_generate\_user\_grant\_revoke\_ddl.sql| View to generate grant or revoke ddl for users and groups in the database. |
| v\_generate\_user\_object\_permissions.sql | View to get the DDL for a users permissions to tables and views. | 
| v\_generate\_view\_ddl.sql | View to get the DDL for a view. | 
| v\_get\_blocking\_locks.sql | View to identify blocking locks as well as determine what/who is blocking a query. |
| v\_get\_cluster\_restart\_ts.sql | View to get the datetime of when Redshift cluster was recently restarted. |
| v\_get\_obj\_priv\_by\_user.sql | View to get the table/views that a user has access to. |
| v\_query\_type\_duration\_summary.sql | View to summarize queries by type (Insert, Select, etc.) per hour for the past 7 Days. |
| v\_get\_schema\_priv\_by\_user.sql | View to get the schema that a user has access to |
| v\_get\_stored\_proc\_params.sql | List all stored procedures with their input parameters. |
| v\_get\_tbl\_priv\_by\_user.sql | View to get the tables that a user has access to. |
| v\_get\_tbl\_priv\_by\_group.sql | View to get the tables that a user group has access to. | 
| v\_get\_tbl\_reads\_and\_writes.sql | View to get operations performed per table for transactions ID or query ID. |
| v\_get\_tbl\_scan\_frequency.sql | View to get list of each permanent table's scan frequency. |
| v\_get\_users\_in\_group.sql | View to get all users in a group. |
| v\_get\_vacuum\_details.sql | View to get vacuum details like table name, Schema Name, Deleted Rows, processing time. |
| v\_get\_view\_priv\_by\_user.sql | View to get the views that a user has access to. |
| v\_my\_last\_query\_summary.sql | View that shows a formatted extract of SVL\_QUERY\_SUMMARY for the last query run in the session. |
| v\_my\_last\_copy\_errors.sql | View to see any errors associated with a COPY command that was run in the session and had errors. |
| v\_object\_dependency.sql | View to merge the different dependency views together. |
| v\_open\_session.sql | View to monitor currently connected and disconnected sessions. |
| v\_session\_leakage\_by\_cnt.sql | View to monitor session leakage by remote host. |
| v\_space\_used\_per\_tbl.sql | View to get pull space used per table. |
| v\_vacuum\_summary.sql | View to flatten stl_vacuum table and provide details like vacuum start and end times, current status, changed rows and freed blocks all in one row. |
| v\_view\_dependency.sql | View to get the names of the views that are dependent on other tables/views. |
| v\_view\_table\_column\_dependency.sql | View to get the views that are dependent on specific columns. |
| v\_wlm\_queue\_state.sql | Adds the WLM_QUEUE_STATE_VW view from [Amazon Redshift Tutorial](https://docs.aws.amazon.com/redshift/latest/dg/tutorial-wlm-understanding-default-processing.html). |
