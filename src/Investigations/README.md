#Investigations

Certain Redshift service nuances are most effectively explained using verbose investigations including code samples, performance metrics, system table queries, and the interpretation of query results.

The content in this project subfolder will serve as a collection of various investigations which highlight these behaviors. 

| Category    | Topic                      | Description                                       |
| :---------- | :------------------------- | :------------------------------------------------ |
| Performance | [early materialization][1] | Data compressions impact on early materialization |


[1]: https://github.com/awslabs/amazon-redshift-utils/blob/master/src/Investigations/EarlyMaterialization.md

[//]: # (Predicate operator cost)
[//]: # (Zone Map Invalidation)
[//]: # (Redundant Compression Analysis)
[//]: # (Colocation vs Parallelism: ALL vs KEY)
[//]: # (Colocation vs Parallelism: EVEN vs KEY)
[//]: # (Commit Queuing)
[//]: # (WLM Queuing)
[//]: # (Unsorted sorted tables) 
[//]: # (Wide column impact)
[//]: # (Interleaved Sortkey Skew and Bucket monitoring)
[//]: # (ODBC HTTP/TCP Proxy Support) 
[//]: # (ODBC Tracing configuration) 
[//]: # (JDBC tracing configuration)
[//]: # (Serializable Isolation Violations)
