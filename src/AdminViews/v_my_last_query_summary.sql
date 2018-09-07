/**********************************************************************************************
Purpose:        View to help you see the summary of the last query that was run

Notes: Query simply applies formatting to SVL_QUERY_SUMMARY for the last run query by the session 
       via the pg_last_query_id() function. Please note that this function may return null for
       queries that are taking advantage of resultset caching.
                
History:
2018-01-18 meyersi Created View Script

**********************************************************************************************/

create or replace view admin.v_my_last_query_summary as 
select query,
       maxtime, 
       avgtime, 
       rows, 
       bytes, 
       lpad(' ',stm+seg+step) || label as label, 
       is_diskbased, 
       workmem, 
       is_rrscan, 
       is_delayed_scan, 
       rows_pre_filter 
from svl_query_summary 
where query = pg_last_query_id() 
order by stm, seg, step;
