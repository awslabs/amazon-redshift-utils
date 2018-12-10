/**********************************************************************************************
Purpose: Returns the high-water mark for WLM query queues and time queuing was last encountered.
    These results can be used to fine tune WLM queues which contain too many or too few slots 
    resulting in WLM queuing or unutilized cluster memory.
    
Columns:
service_class: ID for the service class, defined in the WLM configuration file. 
max_wlm_concurrency: Current actual concurrency level of the service class.
max_service_class_slots: Max number of WLM query slots in the service_class at a point in time.
max_slots_ts: Most recent time at which the max_service_class_slots occurred.
last_queued_time: Most recent time at which queuing in the service_class occurred. NULL if no queuing.
Notes:
- Since generate_series is unsupported in Redshift, this uses an unelegant method to generate a dt
    series. Max 7 day range with 1 sec granularity for perf considerations due to nonequijoin nested 
    loop. 
- If SVL_QUERY_REPORT has < 604800 rows you may want to substitue SVL_QUERY_REPORT for another table
- Will only monitor service_class state as far back as records exist in STL_WLM_QUERY
- Best run after period of heaviest query activity 
History:
2015-08-31 chriz-bigdata created
2018-12-10 zach-data improved performance by switching to stl_scan with 5 second granularity
**********************************************************************************************/
WITH 
	generate_dt_series AS (select sysdate - (n * interval '5 second') as dt from (select row_number() over () as n from stl_scan limit 120960)),
	-- For 1 second granularity use the below CTE for generate_dt_series scanning any table with more than 604800 rows								      
	-- generate_dt_series AS (select sysdate - (n * interval '1 second') as dt from (select row_number() over () as n from [table_with_604800_rows] limit 604800)),
	apex AS (SELECT iq.dt, iq.service_class, iq.num_query_tasks, count(iq.slot_count) as service_class_queries, sum(iq.slot_count) as service_class_slots
		FROM  
		(select gds.dt, wq.service_class, wscc.num_query_tasks, wq.slot_count
		FROM stl_wlm_query wq
		JOIN stv_wlm_service_class_config wscc ON (wscc.service_class = wq.service_class AND wscc.service_class > 4)
		JOIN generate_dt_series gds ON (wq.service_class_start_time <= gds.dt AND wq.service_class_end_time > gds.dt)
		WHERE wq.userid > 1 AND wq.service_class > 4) iq
	GROUP BY iq.dt, iq.service_class, iq.num_query_tasks),
	maxes as (SELECT apex.service_class, max(service_class_slots) max_service_class_slots 
			from apex group by apex.service_class),
	queued as (	select service_class, max(queue_end_time) max_queue_end_time from stl_wlm_query where total_queue_time > 0 GROUP BY service_class)
select apex.service_class, apex.num_query_tasks as max_wlm_concurrency, apex.service_class_slots as max_service_class_slots, max(apex.dt) max_slots_ts, queued.max_queue_end_time last_queued_time
FROM apex
JOIN maxes ON (apex.service_class = maxes.service_class AND apex.service_class_slots = maxes.max_service_class_slots)
LEFT JOIN queued ON queued.service_class = apex.service_class
GROUP BY  apex.service_class, apex.num_query_tasks, apex.service_class_slots, queued.max_queue_end_time
ORDER BY apex.service_class;
