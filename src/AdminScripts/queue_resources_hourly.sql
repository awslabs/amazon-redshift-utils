/**********************************************************************************************
Purpose: Returns the per-hour Resources usage per queue for the past 2 days. 
		 These results can be used to fine tune WLM queues and find peak times for workload.
    
Columns:

   exec_hour: 		Hour of execution of queries
   q: 			ID for the service class, defined in the WLM configuration file. 
   n_cp:		Number of queries executed on that queue/hour
   avg_q_sec:		Average Queueing time in seconds
   avg_e_sec:		Averagte Executiong time in seconds
   avg_pct_cpu:		Average percentage of CPU used by the query. Value can be more than 100% for multi-cpu/slice systems
   max_pct_cpu:		Max percentage of CPU used by the query. Value can be more than 100% for multi-cpu/slice systems
   sum_spill_mb:	Sum of Spill usage by that queue on that hour
   sum_row_scan:	Sum of rows scanned on that queue/hour
   sum_join_rows:	Sum of rows joined on that queue/hour
   sum_nl_join_rows:	Sum of rows Joined using Nested Loops on that queue/hour
   sum_ret_rows:	Sum of rows returned to the leader/client on that queue/hour
   sum_spec_mb:		Sum of Megabytes scanned by a Spectrum query on that queue/hour
   
Notes:


History:

2017-08-09 ericnf created
2017-12-18 ericnf add rows for cached queries
**********************************************************************************************/
select date_trunc('hour', convert_timezone('utc','utc',w.exec_start_time)) as exec_hour, w.service_class as "Q", sum(decode(w.final_state, 'Completed',1,'Evicted',0,0)) as n_cp,  sum(decode(w.final_state, 'Completed',0,'Evicted',1,0)) as n_ev, avg(w.total_queue_time/1000000) as avg_q_sec, avg(w.total_exec_time/1000000) as avg_e_sec,
       avg(m.query_cpu_usage_percent) as avg_pct_cpu, max(m.query_cpu_usage_percent) as max_pct_cpu, max(m.query_temp_blocks_to_disk) as max_spill, sum(m.query_temp_blocks_to_disk) as sum_spill_mb, sum(m.scan_row_count) as sum_row_scan, sum(m.join_row_count) as sum_join_rows, sum(m.nested_loop_join_row_count) as sum_nl_join_rows, 
       sum(m.return_row_count) as sum_ret_rows, sum(m.spectrum_scan_size_mb) as sum_spec_mb
from   stl_wlm_query as w left join svl_query_metrics_summary as m using (userid,service_Class, query)
where  service_class > 5 
  and     w.exec_start_time >=  dateadd(day, -1, current_Date) group by 1,2 
union all
select date_trunc('hour', convert_timezone('utc','utc',c.starttime)) as exec_hour, 0 as "Q", sum(decode(c.aborted, 1,0,1)) as n_cp,  sum(decode(c.aborted, 1,1,0)) as n_ev, 0 as avg_q_sec, avg(c.elapsed/1000000) as avg_e_sec,
       0 as avg_pct_cpu, 0 as max_pct_cpu, 0 as max_spill, 0 as sum_spill_mb, 0 as sum_row_scan, 0 as sum_join_rows, 0 as sum_nl_join_rows, sum(m.return_row_count) as sum_ret_rows, 0 as sum_spec_mb
from svl_qlog c left join svl_query_metrics_summary as m on ( c.userid = m.userid and c.source_query=m.query ) 
 where source_query is not null and     c.starttime >=  dateadd(day, -1, current_Date)
group by 1,2  order by  1 desc,2 ;
