SELECT h.user_id,
       u.usename as user_name,
       query_id,
       transaction_id,
       session_id,
       database_name,
       start_time,
       end_time,
       elapsed_time,
       status,
       result_cache_hit,
       queue_time,
       execution_time,
       query_text,
       query_label,
       query_type,
       error_message,
       returned_rows,
       returned_bytes,
       redshift_version from sys_query_history h
       LEFT JOIN pg_user u on h.user_id=u.usesysid
WHERE user_id > 1
  AND   start_time >= {{START_TIME}}
  AND   start_time <= {{END_TIME}};
;
