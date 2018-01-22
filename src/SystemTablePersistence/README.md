
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

The persisted data will be stored in direct-attached storage tables in Redshift. For the tables, the CREATE TABLE {tablename} LIKE technique is an easy way to inherit the column names, datatypes, encoding, and table distribution from system tables. For system views, an Internet search on the view name (for example: SVL_QUERY_SUMMARY) will direct the user to the Redshift documentation which includes the column-level description. This table will frequently copy-and-paste well into a spreadsheet program, allow for easy extraction of the column name and data type information. This example uses ZSTD for its encoding; but that is of course up to the user.

```
CREATE TABLE IF NOT EXISTS history.hist_stl_load_errors (LIKE STL_LOAD_ERRORS);
CREATE TABLE IF NOT EXISTS history.hist_stl_query (LIKE stl_query);
CREATE TABLE IF NOT EXISTS history.hist_stl_wlm_query (LIKE stl_wlm_query);
CREATE TABLE IF NOT EXISTS history.hist_stl_explain (LIKE stl_explain);
CREATE TABLE IF NOT EXISTS history.hist_svl_query_summary
    (
      userid            INTEGER ENCODE zstd, 
      query             INTEGER ENCODE zstd,
      stm               INTEGER ENCODE zstd,
      seg               INTEGER ENCODE zstd,
      step              INTEGER ENCODE zstd,
      maxtime           BIGINT ENCODE zstd,
      avgtime           BIGINT ENCODE zstd,
      ROWS 		BIGINT ENCODE zstd,
      bytes             BIGINT ENCODE zstd,
      rate_row       	DOUBLE precision ENCODE zstd,
      rate_byte      	DOUBLE precision ENCODE zstd,
      label             TEXT ENCODE zstd,
      is_diskbased    	CHARACTER(1) ENCODE zstd,
      workmem           BIGINT ENCODE zstd,
      is_rrscan         CHARACTER(1) ENCODE zstd,
      is_delayed_scan   CHARACTER(1) ENCODE zstd,
      rows_pre_filter   BIGINT ENCODE zstd
    );
```


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

## Daily or User-Selected Frequency (we recommend populating the history tables daily): ##

### Populating the History Tables: ###

There are two patterns described below. The first the a stand-only insert statement, where the table itself (for example, STL_LOAD_ERRORS) contains a timestamp column that can be used to disambiguate the new information, from the rows that are already in the history tables. The second pattern includes a join (typically to STL_QUERY) in order to obtain the timestamp for the activity. It’s recommended that a transaction is created that starts with the population of STL_QUERY, and then all the tables the user has selected that require a join to STL_QUERY.

```
INSERT INTO history.hist_stl_load_errors (
  SELECT le.* FROM stl_load_errors le, (SELECT NVL(MAX(starttime),'01/01/1902'::TIMESTAMP) AS max_starttime FROM history.hist_stl_load_errors) h WHERE le.starttime > h.max_starttime);
INSERT INTO history.hist_stl_wlm_query (
  SELECT wq.* FROM stl_wlm_query wq, (SELECT NVL(MAX(service_class_start_time),'01/01/1902'::TIMESTAMP) AS max_service_class_start_time FROM history.hist_stl_wlm_query) h WHERE wq.service_class_start_time > h.max_service_class_start_time);
 
BEGIN; 
INSERT INTO history.hist_stl_query (
  SELECT q.* FROM stl_query q, (SELECT NVL(MAX(starttime),'01/01/1902'::TIMESTAMP) AS max_starttime FROM history.hist_stl_query) h WHERE q.starttime > h.max_starttime);
INSERT INTO history.hist_stl_explain (
  SELECT e.* FROM stl_explain e, stl_query q, (SELECT NVL(MAX(starttime),'01/01/1902'::TIMESTAMP) AS max_starttime FROM history.hist_stl_query) h WHERE e.query = q.query AND q.starttime > h.max_starttime);
INSERT INTO history.hist_svl_query_summary (
  SELECT qs.* FROM svl_query_summary qs, stl_query q, (SELECT NVL(MAX(starttime),'01/01/1902'::TIMESTAMP) AS max_starttime FROM history.hist_stl_query) h WHERE qs.query = q.query AND q.starttime > h.max_starttime);
COMMIT;
```

### Where to Run the Data Population SQL: ###
We’ve run across many customers who already have an EC2 host for crontab-related activities. That would be a good fit for this work as well. In addition, this work could be folded into the Redshift Automation project (see the implementation at [link](https://github.com/awslabs/amazon-redshift-utils/tree/master/src/RedshiftAutomation). Finally, a Lambda function is an alternative for hosting this work.

### Querying the Views in the History Schema: ###
The history schema views can be queried in exactly the same way that users have interacted with the existing system objects.

```
SELECT * FROM history.all_stl_load_errors WHERE UPPER(err_reason) LIKE '%DELIMITER NOT FOUND%';
SELECT * FROM history.all_stl_query WHERE query = 1121;
SELECT COUNT(*) FROM history.all_stl_wlm_query WHERE service_class = 6;
SELECT * FROM history.all_stl_explain WHERE query = 1121 ORDER BY nodeid;
SELECT * FROM history.all_svl_query_summary WHERE bytes > 1000000;
```

## Other Considerations: ##
As with any table in Redshift, it's a best practice to analyze (even just a handful of columns) on a weekly basis. This will help inform the query planner of the attributes of the table. Users may also want to enhance the performance of the history views by adding sort keys to the underlying history tables. It is recommended to consider columns used in the filter condition on the associated view for good sort key candidates.

## Conclusion: ##
For five of the most commonly retained Redshift system tables and views that we encounter, the code on this page can be copied-and-pasted, and it’ll “just work”. Of course, each customer’s use-case is unique and extending this model to any of the Redshift system objects is possible. If you create any extensions to this framework, please don’t hesitate to share them back to the Redshift Engineering GitHub community.


