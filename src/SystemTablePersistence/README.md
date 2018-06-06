
# Amazon Redshift System Object Persistence Utility
Some Redshift users would prefer a custom retention period for their Redshift system objects. The Redshift system tables and views, numbering over 100, have a system-controlled retention that is variable but tends to be less than a week for common Redshift use-cases.

## More About the Redshift System Tables and Views:

This is the taxonomy of Redshift system tables and views from [link](https://docs.aws.amazon.com/redshift/latest/dg/c_types-of-system-tables-and-views.html).

* The **stl_** prefix denotes system table logs. stl_ tables contain logs about operations that happened on the cluster in the past few days.
* The **stv_** prefix denotes system table snapshots. stv_ tables contain a snapshot of the current state of the cluster.
* The **svl_** prefix denotes system view logs. svl_ views join some number of system tables to provide more descriptive info.
* The **svv_** prefix denotes system view snapshots. Like the svl_ views, the svv_ views join some system tables to provide more descriptive info.

To persist the tables for a longer amount of time, the material below provides an example implementation to create, populate, and use five of the most common objects that we see being given this treatment: STL_QUERY, STL_WLM_QUERY, STL_EXPLAIN, SVL_QUERY_SUMMARY and STL_LOAD_ERRORS. This mix of tables and views will highlight some of the edge cases users will encounter when applying the techniques to their own list of tables.

## One Time Only Actions: ##

### Creating the HISTORY Schema: ###

Creating a separate schema is a convenient way to sequester the history tables and views away from other objects. Any preferred schema name can be used; a case-insensitive search-and-replace of "HISTORY" will correctly update all the schema references in the included SQL.

```
CREATE SCHEMA IF NOT EXISTS history;
```

### Creating the History Tables: ###

The persisted data will be stored in direct-attached storage tables in Redshift. For the tables, the `CREATE TABLE {tablename} LIKE` technique is an easy way to inherit the column names, datatypes, encoding, and table distribution from system tables. For system views, an Internet search on the view name (for example: `SVL_QUERY_SUMMARY`) will direct the user to the Redshift documentation which includes the column-level description. This table will frequently copy-and-paste well into a spreadsheet program, allow for easy extraction of the column name and data type information.

History tables are created in the `HISTORY` schema, and will be verified on each run of the table persistence system. You can view the table creation statements in [`lib/history_table_creation.sql`](lib/history_table_creation.sql).


### Creating the Views to Join the Historical and Current Information: ###

The views return data from both the current system objects and the historical tables. The anti-join pattern is used to accomplish the deduplication of rows.

```
CREATE OR REPLACE VIEW history.all_stl_load_errors AS
(
SELECT le.*  FROM stl_load_errors le
UNION ALL
SELECT h.* FROM stl_load_errors le
RIGHT OUTER JOIN history.hist_stl_load_errors h ON (le.query = h.query AND le.starttime = h.starttime)
WHERE le.query IS NULL
);
 
CREATE OR REPLACE VIEW history.all_stl_query AS
(
SELECT q.* FROM stl_query q
UNION ALL
SELECT h.* FROM stl_query q
RIGHT OUTER JOIN history.hist_stl_query h ON (q.query = h.query AND q.starttime = h.starttime)
WHERE q.query IS NULL
);
 
CREATE OR REPLACE VIEW history.all_stl_wlm_query AS
(
SELECT wq.* FROM stl_wlm_query wq
UNION ALL
SELECT h.* FROM stl_wlm_query wq
RIGHT OUTER JOIN history.hist_stl_wlm_query h ON (wq.query = h.query AND wq.service_class_start_time = h.service_class_start_time)
WHERE wq.query IS NULL
);
 
CREATE OR REPLACE VIEW history.all_stl_explain AS
(
SELECT e.* FROM stl_explain e
UNION ALL
SELECT h.* FROM stl_explain e
RIGHT OUTER JOIN history.hist_stl_explain h ON (e.query = h.query AND e.userid = h.userid AND e.nodeid = h.nodeid AND e.parentid = h.parentid AND e.plannode = h.plannode)
WHERE e.query IS NULL
);
 
CREATE OR REPLACE VIEW history.all_svl_query_summary AS
(
SELECT qs.* FROM svl_query_summary qs
UNION ALL
SELECT h.* FROM svl_query_summary qs
RIGHT OUTER JOIN history.hist_svl_query_summary h ON (qs.query = h.query AND qs.userid = h.userid AND qs.stm = h.stm AND qs.seg = h.seg AND qs.step = h.step AND qs.maxtime = h.maxtime AND qs.label = h.label)
WHERE qs.query IS NULL
);
```

## Populating the History Tables: ##

This can be done Daily or on a User-Selected Frequency (we recommend populating the history tables daily).

This utility will insert only the new rows using the model as described above, with an anti-join:

```
INSERT INTO history.hist_stl_load_errors (
  SELECT le.* FROM stl_load_errors le, (SELECT NVL(MAX(starttime),'01/01/1902'::TIMESTAMP) AS max_starttime FROM history.hist_stl_load_errors) h WHERE le.starttime > h.max_starttime);
```

You can view the statements that will be run by the utility in [`lib/history_table_config.json`](lib/history_table_config.json);

The [Redshift Automation project](https://github.com/awslabs/amazon-redshift-utils/tree/master/src/RedshiftAutomation) is used to host and run this utility, and this can be setup with a one-click deployment to AWS Lambda. However, we’ve also run across many customers who already have an EC2 host for crontab-related activities. If you wish to use ec2 or other runners with cron, then the Redshift Automation command line provides an option to run this application:

```
./ra --utility SystemTablePersistence --config s3://mybucket/prefix/config.json
```


### Querying the Views in the History Schema: ###
The history schema views can be queried in exactly the same way that users have interacted with the existing system objects.

```
SELECT * FROM history.all_stl_load_errors WHERE UPPER(err_reason) LIKE '%DELIMITER NOT FOUND%';
SELECT * FROM history.all_stl_query WHERE query = 1121;
SELECT COUNT(*) FROM history.all_stl_wlm_query WHERE service_class = 6;
SELECT * FROM history.all_stl_explain WHERE query = 1121 ORDER BY nodeid;
SELECT * FROM history.all_svl_query_summary WHERE bytes > 1000000;
```

## Long term archival of System Table data

If required, this utility can archive data from the internal HISTORY tables to Amazon S3. Add the following configuration values to your configuration file:

```
"s3_unload_location":"s3://mybucket/prefix/redshift/systables/archive"
"s3_unload_role_arn":"arn:aws:iam::<acct>:role/<role name>"
```

Please note that the `s3_unload_role_arn` should be linked to the Redshift cluster to enable S3 access [as outlined here](https://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html).

The structure of the exported data will make it suitable for querying via the AWS Glue Data Catalog. Both the cluster-name and the datetime of data export will be configured as partitions. The structure of the data in the exported location will be:

* `Configured output location`
	* `<table name>` - such as `hist_svl_query_summary`, `hist_stl_explain`
		* `cluster=<your cluster name>`
			* `datetime=<exported date time information>`

For example: 
![image](exported_s3_structure.png)

All exported data is quoted, compressed csv data.

## Other Considerations: ##
As with any table in Redshift, it's a best practice to analyze (even just a handful of columns) on a weekly basis. This will help inform the query planner of the attributes of the table. Users may also want to enhance the performance of the history views by adding sort keys to the underlying history tables. It is recommended to consider columns used in the filter condition on the associated view for good sort key candidates.

## Conclusion: ##
For five of the most commonly retained Redshift system tables and views that we encounter, the code on this page can be copied-and-pasted, and it’ll “just work”. Of course, each customer’s use-case is unique and extending this model to any of the Redshift system objects is possible. If you create any extensions to this framework, please don’t hesitate to share them back to the Redshift Engineering GitHub community.


