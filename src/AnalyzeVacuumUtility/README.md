# Analyze & Vacuum Schema Utility

In order to get the best performance from your Redshift Database, you must ensure that database tables regularly analyzed and vacuumed. For more information , please read the below Redshift documentation,

http://docs.aws.amazon.com/redshift/latest/dg/t_Reclaiming_storage_space202.html
http://docs.aws.amazon.com/redshift/latest/dg/t_Analyzing_tables.html

Whenever you insert, delete, or update (In Redshift update = delete + insert) a significant number of rows, you should run a VACUUM command and then an ANALYZE command. 
In Redshift, the data blocks are immutable, i.e. when rows are DELETED or UPDATED against a table they are simply logically deleted (flagged for deletion), but not physically removed from disk. These causes the rows to continue consuming disk space and those blocks are scanned when a query scans the table. The result of this, table storage space is increased and degraded performance due to otherwise avoidable disk IO during scans. A vacuum recovers the space from deleted rows and restores the sort order. 

To avoid resource intensive VACUUM operation, you can load the data in sort key order, or design your table maintain data for a rolling time period, using time series tables. 

If your table has a large unsorted region (which can’t be vacuumed), a deep copy is much faster than a vacuum. You can use the Column Encoding Utility from our open source GitHub project (https://github.com/awslabs/amazon-redshift-utils) to perform a deep copy. The Column Encoding Utility takes care of the compression analysis, column encoding and deep copy.

The ANALYZE command updates the statistics metadata, which enables the query optimizer to generate more accurate query plans. COPY automatically updates statistics after loading an empty table, so your statistics should be up-to-date. 

The Redshift ‘Analyze Vacuum Utility’ gives you the ability to automate VACUUM and ANALYZE operations. When run, it will VACUUM or ANALYZE an entire schema or individual tables. 

This Utility Analyzes and Vacuums table(s) in a Redshift Database schema, based on certain parameters like unsorted, stats off and size of the table and system alerts from stl_explain & stl_alert_event_log. By turning on/off '--analyze-flag’ and  '--vacuum-flag' parameters, you can run it as  'vacuum-only' or  'analyze-only' utility. This script can be scheduled to run VACUUM and ANALYZE as part of regular maintenance/housekeeping activities, when there are less database activities (quiet period).

## Vacuum

This script runs vacuum in two phases,

### Phase 1: 

Identify and run vacuum based on the alerts recorded in STL_ALERT_EVENT_LOG. STL_ALERT_EVENT_LOG, records an alert when the query optimizer identifies conditions that might indicate performance issues. We can use the STL_ALERT_EVENT_LOG table to identify tables that needs vacuum. The script uses the following query to get the list of tables and number of alerts (count), that needs vacuum based on alerts raised by optimizer:

```
SELECT schema_name, 
       table_name 
FROM   (SELECT TRIM(n.nspname)             schema_name, 
               c.relname                   table_name, 
               DENSE_RANK() 
                 OVER ( 
                   ORDER BY COUNT(*) DESC) AS qry_rnk, 
               Count(*) 
        FROM   stl_alert_event_log AS l 
               JOIN (SELECT query, 
                            tbl, 
                            perm_table_name 
                     FROM   stl_scan 
                     WHERE  perm_table_name <> 'Internal Worktable' 
                     GROUP  BY query, 
                               tbl, 
                               perm_table_name) AS s 
                 ON s.query = l.query 
               JOIN pg_class c 
                 ON c.oid = s.tbl 
               JOIN PG_CATALOG.pg_namespace n 
                 ON n.oid = c.relnamespace 
        WHERE  l.userid > 1 
               AND l.event_time >= DATEADD(DAY, $(goback_no_of_days)  , CURRENT_DATE) 
               AND l.Solution LIKE '%VACUUM command%' 
        GROUP  BY TRIM(n.nspname), 
                  c.relname) anlyz_tbl 
WHERE  anlyz_tbl.qry_rnk < $( query_rank )
```

Variables and default values (which can be changed):

* goback_no_of_days: To control number days to look back from CURRENT_DATE,  Default value = 1 
* query_rank : To get the top N rank tables based on the stl_alert_event_log alerts, Default value = 25

### Phase 2: 

Identify and run vacuum based on the certain thresholds (Like unsorted > 10% and Stats Off > 10% and Size < 700 GB):

```
SELECT DISTINCT 'vacuum' + "schema" + '.' + "table" + ' ; ' 
FROM   svv_table_info 
WHERE  "schema" = $( schema_name)
       AND (
--If the size of the table is less than the max_table_size_mb then , run vacuum based on condition: >min_unsorted_pct AND >deleted_pct 

( ( size < $(max_table_size_mb) )AND ( unsorted > $(min_unsorted_pct) OR empty > $( deleted_pct) ) ) 

OR 
           --If the size of the table is greater than the max_table_size_mb then , run vacuum based on condition:   
           -- >min_unsorted_pct AND < max_unsorted_pct AND >deleted_pct 
           --This is to avoid big table with large unsorted_pct 

( ( size > $(max_table_size_mb) )AND ( unsorted > $(min_unsorted_pct)  AND unsorted < $(max_unsorted_pct) ) ) )

ORDER  BY "size" ASC;
```

## Analyze

This script runs Analyze in two phases

### Phase 1: 

Run ANALYZE based on the alerts recorded in stl_explain & stl_alert_event_log.  The script uses the following query to get the list of tables and number of alerts (count), that needs analyze based on alerts raised by optimizer:

```
SELECT DISTINCT 'analyze ' + feedback_tbl.schema_name + '.' + feedback_tbl.table_name + ' ; ' 
                                    + '/* '+ ' Table Name : ' + info_tbl."schema" + '.' + info_tbl."table" 
                                        + ', Stats_Off : ' + CAST(info_tbl."stats_off" AS VARCHAR(10)) + ' */ ;' 
                                    FROM ((SELECT TRIM(n.nspname) schema_name, 
                                          c.relname table_name 
                                   FROM (SELECT TRIM(SPLIT_PART(SPLIT_PART(a.plannode,':',2),' ',2)) AS Table_Name, 
                                                COUNT(a.query), 
                                                DENSE_RANK() OVER (ORDER BY COUNT(a.query) DESC) AS qry_rnk 
                                         FROM stl_explain a, 
                                              stl_query b 
                                         WHERE a.query = b.query 
                                         AND   CAST(b.starttime AS DATE) >= dateadd (DAY,-1,CURRENT_DATE) 
                                         AND   a.userid > 1 
                                         AND   a.plannode LIKE '%missing statistics%' 
                                         AND   a.plannode NOT LIKE '%_bkp_%' 
                                         GROUP BY Table_Name) miss_tbl 
                                     LEFT JOIN pg_class c ON c.relname = TRIM (miss_tbl.table_name) 
                                     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace 
                                   WHERE miss_tbl.qry_rnk <= 25) 
                                   UNION 
                                   SELECT schema_name, 
                                          table_name 
                                   FROM (SELECT TRIM(n.nspname) schema_name, 
                                                c.relname table_name, 
                                                DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS qry_rnk, 
                                                COUNT(*) 
                                         FROM stl_alert_event_log AS l 
                                           JOIN (SELECT query, 
                                                        tbl, 
                                                        perm_table_name 
                                                 FROM stl_scan 
                                                 WHERE perm_table_name <> 'Internal Worktable' 
                                                 GROUP BY query, 
                                                          tbl, 
                                                          perm_table_name) AS s ON s.query = l.query 
                                           JOIN pg_class c ON c.oid = s.tbl 
                                           JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace 
                                         WHERE l.userid > 1 
                                         AND   l.event_time >= dateadd (DAY,-1,CURRENT_DATE) 
                                         AND   l.Solution LIKE '%ANALYZE command%' 
                                         GROUP BY TRIM(n.nspname), 
                                                  c.relname) anlyz_tbl 
                                   WHERE anlyz_tbl.qry_rnk < 25) feedback_tbl 
                              JOIN svv_table_info info_tbl 
                                ON info_tbl.schema = feedback_tbl.schema_name 
                               AND info_tbl.table = feedback_tbl.table_name 
                            WHERE info_tbl.stats_off::DECIMAL (32,4) > $(stats_off_pct)
                            AND   TRIM(info_tbl.schema) = $( schema_name)
                            ORDER BY info_tbl.size ASC ;
```

### Phase 2: 

Run ANALYZE based on the below filter criteria:

```
SELECT DISTINCT 'analyze ' + "schema" + '.' + "table" + ' ; ' 
      + '/* '+ ' Table Name : ' + "schema" + '.' + "table" 
      + ', Stats_Off : ' + CAST("stats_off" AS VARCHAR(10)) + ' */ ;' 
                                       FROM svv_table_info 
WHERE stats_off> $(stats_off_pct) 
AND  "schema" = $(schema_name);
```

If table has a stats_off_pct > 10 %, then the script runs ANALYZE command to update the statistics.

## Summary of Parameters:

```
Sl.No	Parameter				Mandatory	Default Value
1.		--db                 	Yes	
2.		--db-user            	Yes	
3.		--db-pwd             	Yes	
4.		--db-host            	Yes	
5.		--db-port            	No			5439
6.		--db-conn-opts       	No			
7.		--schema-name    		No			Public
8.		--table-name         	No			Schema
9.		--output-file        	Yes	
10.		--debug              	No			False
11.		--slot-count         	No			1
12.		--ignore-errors      	No			False
13.		--query_group        	No			None
14.		--analyze-flag			No			True
15.		--vacuum-flag			No			True
16.		--vacuum-parameter		No			FULL
17.		--min-unsorted-pct		No			05%
18.		--max-unsorted-pct		No			50%
19.		--deleted-pct			No			05%
20.		--stats-off-pct			No			10%
21.		--stats-threshold			No			10%
22.		--max-table-size-mb		No			700*1024 MB
23.		--predicate-cols		No			False
```

The above parameter values depends on the cluster type, table size, available system resources and available ‘Time window’ etc. The default values provided here are based on ds2.8xlarge, 8 node cluster. It may take some trial and error to come up with correct parameter values to vacuum and analyze your table(s). If table size is greater than certain size (max_table_size_mb) and has a large unsorted region (deleted_pct  or max_unsorted_pct), consider deep copy, which would be much faster than a vacuum.

As VACUUM & ANALYZE operations are resource intensive, you should ensure that this will not adversely impact other database operations running on your cluster. AWS has thoroughly tested this software on a variety of systems, but cannot be responsible for the impact of running the utility against your database.

### Parameter Guidance

#### Slot Count

Sets the number of query slots a query will use.

Workload management (WLM) reserves slots in a service class according to the concurrency level set for the queue (for example, if concurrency level is set to 5, then the service class has 5 slots). WLM allocates the available memory for a service class equally to each slot. For more information, see Implementing Workload Management.

For operations where performance is heavily affected by the amount of memory allocated, such as Vacuum, increasing the value of wlm_query_slot_count can improve performance. In particular, for slow Vacuum commands, inspect the corresponding record in the SVV_VACUUM_SUMMARY view. If you see high values (close to or higher than 100) for sort_partitions and merge_increments in the SVV_VACUUM_SUMMARY view, consider increasing the value for wlm_query_slot_count the next time you run Vacuum against that table.

Increasing the value of wlm_query_slot_count limits the number of concurrent queries that can be run.

Note:

If the value of wlm_query_slot_count is larger than the number of available slots (concurrency level) for the service class, the query will fail. If you encounter an error, decrease wlm_query_slot_count to an allowable value.

#### analyze-flag

Flag to turn ON/OFF ANALYZE functionality (True or False). If you want run the script to only perform ANALYZE on a schema or table, set this value ‘False’ : Default = ‘True’.

#### vacuum-flag

Flag to turn ON/OFF VACUUM functionality (True or False). If you want run the script to only perform VACUUM on a schema or table, set this value ‘False’ : Default = ‘True’.

#### vacuum-parameter

Specify vacuum parameters [ FULL | SORT ONLY | DELETE ONLY | REINDEX ] Default = FULL'
Vacuum Syntax:
VACUUM [ FULL | SORT ONLY | DELETE ONLY | REINDEX ] [ table_name ]

#### min-unsorted-pct

Minimum unsorted percentage (%) to consider a table for vacuum: Default = 05%.

#### max-unsorted-pct

Maximum unsorted percentage(%) to consider a table for vacuum : Default = 50%.

#### deleted-pct

Minimum deleted percentage (%) to consider a table for vacuum: Default = 05%

#### stats-off-pct

Minimum stats off percentage(%) to consider a table for analyze : Default = 10%

#### max-table-size-mb 

Maximum table size in MB : Default = 700*1024 MB

#### predicate-cols

Analyze predicate columns only. Default = False


## Sample Usage

```
python analyze-vacuum-schema.py  --db <> --db-user <> --db-pwd <> --db-port 8192 --db-host aaa.us-west-2.redshift.amazonaws.com --schema-name public  --table-name customer_v6 --output-file /Users/test.log --debug True  --ignore-errors False --slot-count 2   --min-unsorted-pct 5 --max-unsorted-pct 50 --deleted-pct 15 --stats-off-pct 10 --max-table-size-mb 700*1024
```

## Install Notes

To install PyGreSQL (Python PostgreSQL Driver) on Amazon Linux, please ensure that you follow the below steps as the ec2-user:

```
sudo easy_install pip 
sudo yum install postgresql postgresql-devel gcc python-devel 
sudo pip install PyGreSQL 
```

## Limitations

1. Script runs all VACUUM commands sequentially. Currently in Redshift multiple concurrent vacuum operations are not supported. 
2. Script runs all ANALYZE commands sequentially not concurrently.
3. Does not support column level ANALYZE. 
4. Multiple schemas are not supported.
5. Skew factor is not considered.
