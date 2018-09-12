# Redshift Admin Scripts
Scripts objective is to help on tuning and troubleshooting.
If you are using psql, you can use `psql [option] -f <script.sql>` to run.

| Script                       | Purpose                                                                                   |
|------------------------------|-------------------------------------------------------------------------------------------|
| commit_stats.sql             | Shows information on consumption of cluster resources through COMMIT statements           |
| copy_performance.sql         | Shows longest running copy for past 7 days                                                |
| current_session_info.sql     | Query showing information about sessions with currently running queries                   |
| filter_used.sql              | Return filter applied to tables on scans. To aid on choosing sortkey                      |
| generate_calendar.sql        | Creates a calendar dimension table useful for star-schema joins                           |
| missing_table_stats.sql      | Query shows EXPLAIN plans which flagged "missing statistics" on the underlying tables     |
| perf_alert.sql               | Return top occurrences of alerts, join with table scans and SQL Text                      |
| table_alerts.sql             | Return top occurrences of table access related alerts                                     |
| predicate_columns.sql        | Return Information about Predicate Columns on tables                                      |
| queuing_queries.sql          | Query showing queries which are waiting on a WLM Query Slot                               |
| queue_resources_hourly.sql   | Returns hourly resources usage for WLM query queues                                       |
| running_queues.sql           | Returns queries running and queueing and resources consumed                               |
| table_info.sql               | Return Table storage information (size, skew, etc)                                        |
| table_inspector.sql          | Table Analysis based on content in [Analyzing Table Design](http://docs.aws.amazon.com/redshift/latest/dg/c_analyzing-table-design.html). Complements table_info.sql                                           |
| top_queries.sql              | Return the top 50 most time consuming statements in the last 7 days                       |
| top_queries_and_cursors.sql  | Return the top 50 most time consuming statements in the last 7 days - include cursor text |
| unscanned_table_summary.sql  | Summarizes storage consumed by unscanned tables                                           |
| wlm_apex.sql                 | Returns overall high water-mark for WLM query queues and time queuing last occurred       |
| wlm_apex_hourly.sql          | Returns hourly high water-mark for WLM query queues                                       |
| wlm_qmr_rule_candidates.sql  | Calculate candidates for new WLM Query Monitoring rules                                   |
| user_to_be_dropped_objs.sql  | Find objects owned by a user that cannot be dropped                                       |
| user_to_be_dropped_privs.sql | Find privileges granted to a user that cannot be dropped                                  |

