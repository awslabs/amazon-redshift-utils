# Redshift Admin Scripts
Scripts objective is to help on tuning and troubleshooting.
If you are using psql, you can use \i &lt;script.sql&gt; to run.

| Script | Purpose |
| ------------- | ------------- |
| top_queries.sql | Return the top 50 most time consuming statements in the last 7 days |
| perf_alerts.sql | Return top occurrences of alerts, join with table scans |
| filter_used.sql | Return filter applied to tables on scans. To aid on choosing sortkey |
| commit_stats.sql | Shows information on consumption of cluster resources through COMMIT statements |
| current_session_info.sql | Query showing information about sessions with currently running queries |
| missing_table_stats.sql | Query shows EXPLAIN plans which flagged "missing statistics" on the underlying tables |
| queuing_queries.sql | Query showing queries which are waiting on a WLM Query Slot |
| table_info.sql | Return Table storage information (size, skew, etc) |
| wlm_apex.sql | Returns overall high water-mark for WLM query queues and time queuing last occurred |
| wlm_apex_hourly.sql | Returns hourly high water-mark for WLM query queues |

