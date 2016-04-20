/**********************************************************************************************
Purpose: Returns the per-hour high water-mark for WLM query queues. These results can be used 
    to fine tune WLM queues which contain too many or too few slots, resulting in WLM queuing 
    or unutilized cluster memory. With hourly aggregates you can leverage dynamic WLM changes
    to ensure your cluster is correctly configured for workloads with a predictable pattern.
    
Columns:

   service_class: ID for the service class, defined in the WLM configuration file. 
   max_wlm_concurrency: Current actual concurrency level of the service class.
   day: Day of specified range. 
   hour: 1 hour UTC range of time. 
   max_service_class_slots: Max number of WLM query slots in the service_class at a point in time.

Notes:

- Since generate_series is unsupported in Redshift, this uses an unelegant method to generate a dt
    series. Max 7 day range with 1 sec granularity for perf considerations. 
- Will only monitor service_class state as far back as records exist in STL_WLM_QUERY

History:

2015-09-16 chriz-bigdata created
**********************************************************************************************/

WITH
        -- Replace STL_SCAN in generate_dt_series with another table which has > 604800 rows if STL_SCAN does not
        generate_dt_series AS (select sysdate - (n * interval '1 second') as dt from (select row_number() over () as n from stl_scan limit 604800)),
        apex AS (SELECT iq.dt, iq.service_class, iq.num_query_tasks, count(iq.slot_count) as service_class_queries, sum(iq.slot_count) as service_class_slots
                FROM
                (select gds.dt, wq.service_class, wscc.num_query_tasks, wq.slot_count
                FROM stl_wlm_query wq
                JOIN stv_wlm_service_class_config wscc ON (wscc.service_class = wq.service_class AND wscc.service_class > 4)
                JOIN generate_dt_series gds ON (wq.service_class_start_time <= gds.dt AND wq.service_class_end_time > gds.dt)
                WHERE wq.userid > 1 AND wq.service_class > 4) iq
        GROUP BY iq.dt, iq.service_class, iq.num_query_tasks),
        maxes as (SELECT apex.service_class, trunc(apex.dt) as d, date_part(h,apex.dt) as dt_h, max(service_class_slots) max_service_class_slots
                        from apex group by apex.service_class, apex.dt, date_part(h,apex.dt))
SELECT apex.service_class, apex.num_query_tasks as max_wlm_concurrency, maxes.d as day, maxes.dt_h || ':00 - ' || maxes.dt_h || ':59' as hour, MAX(apex.service_class_slots) as max_service_class_slots
FROM apex
JOIN maxes ON (apex.service_class = maxes.service_class AND apex.service_class_slots = maxes.max_service_class_slots)
GROUP BY  apex.service_class, apex.num_query_tasks, maxes.d, maxes.dt_h
ORDER BY apex.service_class, maxes.d, maxes.dt_h;      
