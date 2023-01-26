SELECT user_id,
query_id,
status,
session_id,
transaction_id,
database_name,
table_name,
start_time,
end_time,
duration,
data_source,
loaded_rows,
loaded_bytes,
source_file_count,
source_file_bytes,
error_count from SYS_LOAD_HISTORY
WHERE user_id > 1
  AND   start_time >= {{START_TIME}}
  AND   start_time <= {{END_TIME}};




