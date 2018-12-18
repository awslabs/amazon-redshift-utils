# Analyze & Vacuum Schema Utility

In order to get the best performance from your Redshift Database, you must ensure that database tables regularly analyzed and vacuumed. For more information , please read the below Redshift documentation,

http://docs.aws.amazon.com/redshift/latest/dg/t_Reclaiming_storage_space202.html
http://docs.aws.amazon.com/redshift/latest/dg/t_Analyzing_tables.html

Whenever you insert, delete, or update (In Redshift update = delete + insert) a significant number of rows, you should run a VACUUM command and then an ANALYZE command. 
In Redshift, the data blocks are immutable, i.e. when rows are DELETED or UPDATED against a table they are simply logically deleted (flagged for deletion), but not physically removed from disk. This causes the rows to continue consuming disk space and those blocks are scanned when a query scans the table. The result of this, table storage space is increased and degraded performance due to otherwise avoidable disk IO during scans. A vacuum recovers the space from deleted rows and restores the sort order. 

To avoid resource intensive VACUUM operation, you can load the data in sort key order, or design your table maintain data for a rolling time period, using time series tables. 

If your table has a large unsorted region (which can’t be vacuumed), a deep copy is much faster than a vacuum. You can use the Column Encoding Utility from our open source GitHub project https://github.com/awslabs/amazon-redshift-utils to perform a deep copy. The Column Encoding Utility takes care of the compression analysis, column encoding and deep copy.

The ANALYZE command updates the statistics metadata, which enables the query optimizer to generate more accurate query plans. COPY automatically updates statistics after loading an empty table, so your statistics should be up to date. 

The Redshift ‘Analyze Vacuum Utility’ gives you the ability to automate VACUUM and ANALYZE operations. When run, it will VACUUM or ANALYZE an entire schema or individual tables. 

This Utility Analyzes and Vacuums table(s) in a Redshift Database schema, based on certain parameters like unsorted, stats off and size of the table and system alerts from `stl_explain` & `stl_alert_event_log`. By turning on/off '--analyze-flag’ and  '--vacuum-flag' parameters, you can run it as  'vacuum-only' or  'analyze-only' utility. This script can be scheduled to run VACUUM and ANALYZE as part of regular maintenance/housekeeping activities, when there are fewer database activities.

## Vacuum

This script runs vacuum in two phases,

### Phase 1: 

Identify and run vacuum based on the alerts recorded in ``stl_alert_event_log``. ``stl_alert_event_log``, records an alert when the query optimizer identifies conditions that might indicate performance issues. We can use the ``stl_alert_event_log`` table to identify the top 25 tables that need vacuum. The script uses SQL to get the list of tables and number of alerts, which indicate that vacuum is required.

Variables affecting this phase:

* `goback_no_of_days`: To control number days to look back from CURRENT_DATE

### Phase 2: 

Identify and run vacuum based on certain thresholds related to table statistics (Like unsorted > 10% and Stats Off > 10% and limited to specific table sizes.

Variables affecting this phase:

* `stats_off_pct`: To control the threshold of statistics inaccuracy
* `min_unsorted_pct`: To control the lower limit of unsorted blocks
* `max_unsorted_pct`: To control the upper limit of unsorted blocks (preventing vacuum on super large tables)
* `max_tbl_size_mb`: To control when a table is too large to be vacuumed by this utility


## Analyze

This script runs Analyze in two phases:

### Phase 1: 

Run ANALYZE based on the alerts recorded in ``stl_explain`` & ``stl_alert_event_log``. 

### Phase 2: 

Run ANALYZE based the `stats_off` metric in `svv_table_info`. If table has a `stats_off_pct` > 10%, then the script runs ANALYZE command to update the statistics.

## Summary of Parameters:

| Parameter | Mandatory | Default Value |
| :--- | :---: | :--- |
| --db | Yes | |
| --db-user | Yes | |
|--db-pwd | Yes | |
|--db-host | Yes | |
|--db-port | No | 5439 |
|--db-conn-opts | No | |
|--schema-name | No | Public |
|--table-name | No | |
|--output-file | Yes | |
|--debug | No | False |
|--slot-count | No | 1 |
|--ignore-errors | No | False |
|--query_group | No | None |
|--analyze-flag | No | False |
|--vacuum-flag | No | False |
|--vacuum-parameter | No | FULL |
|--min-unsorted-pct | No | 0.05 |
|--max-unsorted-pct | No | 0.5 |
|--stats-off-pct | No | 0.1 |
|--stats-threshold | No | 0.1 |
|--max-table-size-mb | No | 700*1024 |
|--predicate-cols | No | False |

The above parameter values depend on the cluster type, table size, available system resources and available ‘Time window’ etc. The default values provided here are based on ds2.8xlarge, 8 node cluster. It may take some trial and error to come up with correct parameter values to vacuum and analyze your table(s). If table size is greater than certain size (`max_table_size_mb`) and has a large unsorted region (`max_unsorted_pct`), consider performing a deep copy, which will be much faster than a vacuum.

As VACUUM & ANALYZE operations are resource intensive, you should ensure that this will not adversely impact other database operations running on your cluster. AWS has thoroughly tested this software on a variety of systems, but cannot be responsible for the impact of running the utility against your database.

### Parameter Guidance

### Schema Name

The utility will accept a valid schema name, or alternative a regular expression pattern which will be used to match to all schemas in the database. This uses Posix regular expression syntax. You can use `(.*)` to match all schemas. 

#### Slot Count

Sets the number of query slots a query will use.

Workload management (WLM) reserves slots in a service class according to the concurrency level set for the queue (for example, if concurrency level is set to 5, then the service class has 5 slots). WLM allocates the available memory for a service class equally to each slot. For more information, see Implementing Workload Management.

For operations where performance is heavily affected by the amount of memory allocated, such as Vacuum, increasing the value of wlm_query_slot_count can improve performance. In particular, for slow Vacuum commands, inspect the corresponding record in the `SVV_VACUUM_SUMMARY` view. If you see high values (close to or higher than 100) for `sort_partitions` and `merge_increments` in the `SVV_VACUUM_SUMMARY` view, consider increasing the value for `wlm_query_slot_count` the next time you run Vacuum against that table.

Increasing the value of wlm_query_slot_count limits the number of concurrent queries that can be run.

Note:

If the value of `wlm_query_slot_count` is larger than the number of available slots (concurrency level) for the queue targeted by the user, the utilty will fail. If you encounter an error, decrease `wlm_query_slot_count` to an allowable value.

#### analyze-flag

Flag to turn ON/OFF ANALYZE functionality (True or False). If you want run the script to only perform ANALYZE on a schema or table, set this value ‘False’ : Default = ‘False’.

#### vacuum-flag

Flag to turn ON/OFF VACUUM functionality (True or False). If you want run the script to only perform VACUUM on a schema or table, set this value ‘False’ : Default = ‘False’.

#### vacuum-parameter

Specify vacuum parameters [ `FULL | SORT ONLY | DELETE ONLY | REINDEX` ] Default = FULL

#### min-unsorted-pct

Minimum unsorted percentage (%) to consider a table for vacuum: Default = 5%.

#### max-unsorted-pct

Maximum unsorted percentage(%) to consider a table for vacuum : Default = 50%.

#### stats-off-pct

Minimum stats off percentage(%) to consider a table for analyze : Default = 10%

#### max-table-size-mb 

Maximum table size 700GB in MB : Default = 700*1024 MB

#### predicate-cols

Analyze predicate columns only. Default = False


## Sample Usage

```
python analyze-vacuum-schema.py  --db <> --db-user <> --db-pwd <> --db-port 8192 --db-host aaa.us-west-2.redshift.amazonaws.com --schema-name public  --table-name customer_v6 --output-file /Users/test.log --debug True  --ignore-errors False --slot-count 2 --min-unsorted-pct 5 --max-unsorted-pct 50 --stats-off-pct 10 --max-table-size-mb 700*1024
```

## Install Notes

```
sudo easy_install pip  
sudo pip install 
```

## Limitations

1. Script runs all VACUUM commands sequentially. Currently in Redshift multiple concurrent vacuum operations are not supported. 
2. Script runs all ANALYZE commands sequentially not concurrently.
3. Does not support column level ANALYZE. 
4. Multiple schemas are not supported.
5. Skew factor is not considered.
