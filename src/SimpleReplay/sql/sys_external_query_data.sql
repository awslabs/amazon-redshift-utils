SELECT user_id,
       query_id,
       child_query_sequence,
       transaction_id,
       segment_id,
       source_type,
       start_time,
       end_time,
       duration,
       total_partitions,
       qualified_partitions,
       scanned_files,
       returned_rows,
       returned_bytes,
       file_format,
       file_location,
       external_query_text from SYS_EXTERNAL_QUERY_DETAIL
    WHERE user_id > 1
      AND   start_time >= {{START_TIME}}
      AND   start_time <= {{END_TIME}};
    ;