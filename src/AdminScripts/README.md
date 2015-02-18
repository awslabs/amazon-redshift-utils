# Redshift Admin Scripts
Scripts objective is to help on tuning and troubleshooting.
If you are using psql, you can use \i &lt;script.sql&gt; to run.

| Script | Purpose |
| ------------- | ------------- |
| top_queries.sql | Return the top 50 most time consuming statements in the last 7 days |
| perf_alerts.sql | Return top occurrences of alerts, join with table scans |
| filter_used.sql | Return filter applied to tables on scans. To aid on choosing sortkey |
| commit_stats.sql | Shows information on consumption of cluster resources through COMMIT statements |
| table_info.sql | Return Table storage information (size, skew, etc) |
