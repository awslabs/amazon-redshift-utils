--DROP VIEW admin.v_query_type_duration_summary;
/**********************************************************************************************
Purpose: View to summarize queries by type (Insert, Select, etc.) per hour for the past 7 Days
History:
2016-07-13 joeharris76 Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_query_type_duration_summary
AS
SELECT  database, query_type, query_hour
        /* Overall query count and average duration */
       ,COUNT(*)            query_total
       ,AVG(query_duration) avg_duration
        /* Central tendency (truncated mean 25-75%) average query duration */
       ,AVG(CASE WHEN icosile BETWEEN 6 AND 15 THEN query_duration ELSE NULL END) central_duration
        /* Rough 50th percentile (45-55%) average query duration */
       ,AVG(CASE WHEN icosile IN (10,11) THEN query_duration ELSE NULL END) "50th_percentile_dur"
        /* Rough 95th percentile (top 5%) average query duration */
       ,AVG(CASE WHEN icosile = 20 THEN query_duration ELSE NULL END) "95th_percentile_dur"
FROM /* Calculate the icosile (1/20th) for each query by type and hour */
     (SELECT database, query_type, query_duration, query_hour
            ,NTILE(20) OVER (PARTITION BY database, query_type ORDER BY query_duration) icosile
      FROM /* Classify each query and calculate the duration 
              NOTE: The order of the search is important. */
           (SELECT  CASE  WHEN "userid" = 1                                             THEN 'SYSTEM'
                          WHEN REGEXP_INSTR("querytxt",'(padb_|pg_internal)'          ) THEN 'SYSTEM'
                          WHEN REGEXP_INSTR("querytxt",'[uU][nN][dD][oO][iI][nN][gG] ') THEN 'ROLLBACK'
                          WHEN REGEXP_INSTR("querytxt",'[cC][uU][rR][sS][oO][rR] '    ) THEN 'CURSOR'
                          WHEN REGEXP_INSTR("querytxt",'[fF][eE][tT][cC][hH] '        ) THEN 'CURSOR'
                          WHEN REGEXP_INSTR("querytxt",'[dD][eE][lL][eE][tT][eE] '    ) THEN 'DELETE'
                          WHEN REGEXP_INSTR("querytxt",'[cC][oO][pP][yY] '            ) THEN 'COPY'
                          WHEN REGEXP_INSTR("querytxt",'[uU][pP][dD][aA][tT][eE] '    ) THEN 'UPDATE'
                          WHEN REGEXP_INSTR("querytxt",'[iI][nN][sS][eE][rR][tT] '    ) THEN 'INSERT'
                          WHEN REGEXP_INSTR("querytxt",'[sS][eE][lL][eE][cC][tT] '    ) THEN 'SELECT'
                    ELSE 'OTHER' END query_type
                   ,DATEPART(hour, starttime) AS                    query_hour
                   ,DATEDIFF(milliseconds , starttime , endtime)    query_duration
                  ,database
              FROM stl_query
           ) a
      ) b
GROUP BY 1,2,3
ORDER BY 1,2,3
;
