SELECT
    CASE
      WHEN REGEXP_INSTR ("query_text",'(padb_|pg_|catalog_history)')
      THEN 'SYSTEM'
      WHEN query_type = 'DELETE'
      THEN 'DELETE'
      WHEN query_type = 'COPY'
      THEN 'COPY'
      WHEN query_type = 'UPDATE'
      THEN 'UPDATE'
      WHEN query_type = 'INSERT'
      THEN 'INSERT'
      WHEN query_type = 'SELECT'
      THEN 'SELECT'
      WHEN query_type = 'UNLOAD'
      THEN 'UNLOAD'
      WHEN query_type = 'DDL'
      THEN 'DDL'
      WHEN query_type = 'UTILITY'
      THEN CASE
             WHEN REGEXP_INSTR ("query_text",'[vV][aA][cC][uU][uU][mM][ :]')
             THEN 'VACUUM'
             WHEN REGEXP_INSTR ("query_text",'[rR][oO][lL][lL][bB][aA][cC][kK] ')
             THEN 'ROLLBACK'
             WHEN REGEXP_INSTR ("query_text",'[fF][eE][tT][cC][hH] ')
             THEN 'FETCH'
             WHEN REGEXP_INSTR ("query_text",'[cC][uU][rR][sS][oO][rR] ')
             THEN 'CURSOR'
             ELSE 'UTILITY'
           END
      ELSE 'OTHER'
    END statement_type
    , COUNT(CASE
              WHEN status = 'failed'
              THEN 1
            END) AS aborted
    , COUNT(*) AS total_count
FROM
    sys_query_history
WHERE    user_id > 1
         AND query_text LIKE '%replay_start%'
         AND start_time >= {{START_TIME}}
         AND start_time <= {{END_TIME}}
GROUP BY
    1
ORDER BY
    2 DESC;
