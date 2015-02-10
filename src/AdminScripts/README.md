
Scripts objective is to help on tuning and troubleshooting.
If you are using psql, you can use \i <script>.sql to initiate it

Scripts:

top_queries.sql		Return the top 50 most time consuming statements in the last 7 days
perf_alerts.sql		Return top occurencies of alerts, join with table scans 
filter_used.sql		Return filter applied to tables on scans. To aid on choosing sortkey

