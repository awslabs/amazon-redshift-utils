SELECT CASE
         WHEN "userid" = 1 THEN 'SYSTEM'
         WHEN REGEXP_INSTR ("querytxt",'(padb_|pg_|catalog_history)') THEN 'SYSTEM'
         WHEN REGEXP_INSTR ("querytxt",'[uU][nN][dD][oO][iI][nN][gG] ') THEN 'ROLLBACK'
         WHEN REGEXP_INSTR ("querytxt",'[cC][uU][rR][sS][oO][rR] ') THEN 'CURSOR'
         WHEN REGEXP_INSTR ("querytxt",'[fF][eE][tT][cC][hH] ') THEN 'FETCH'
         WHEN REGEXP_INSTR ("querytxt",'[dD][eE][lL][eE][tT][eE] ') THEN 'DELETE'
         WHEN REGEXP_INSTR ("querytxt",'[cC][oO][pP][yY] ') THEN 'COPY'
         WHEN REGEXP_INSTR ("querytxt",'[uU][pP][dD][aA][tT][eE] ') THEN 'UPDATE'
         WHEN REGEXP_INSTR ("querytxt",'[iI][nN][sS][eE][rR][tT] ') THEN 'INSERT'
         WHEN REGEXP_INSTR ("querytxt",'[vV][aA][cC][uU][uU][mM][ :]') THEN 'VACUUM'
         WHEN REGEXP_INSTR ("querytxt",'[sS][eE][lL][eE][cC][tT] ') THEN 'SELECT'
         ELSE 'OTHER'
       END statement_type,
       COUNT(*) total_count,
       SUM(aborted) AS aborted,
       SUM(CASE WHEN concurrency_scaling_status = 1 THEN 1 ELSE 0 END) count_cs
FROM stl_query
WHERE starttime >= {{START_TIME}}
AND   starttime <= {{END_TIME}}
AND   userid > 1
AND   querytxt LIKE '%replay_start%'
GROUP BY 1
UNION ALL
SELECT 'DDL Statements' AS statement_type,
       COUNT(DISTINCT xid) AS total_count,
       0 AS aborted_count,
       0 AS count_cs
FROM stl_ddltext
WHERE starttime >= {{START_TIME}}
AND   starttime <= {{END_TIME}}
AND   userid > 1
UNION ALL
SELECT 'Utility Statements' AS statement_type,
       COUNT(DISTINCT xid) AS total_count,
       0 AS aborted_count,
       0 AS count_cs
FROM stl_utilitytext
WHERE starttime >= {{START_TIME}}
AND   starttime <= {{END_TIME}}
AND   userid > 1
ORDER BY 2 DESC

