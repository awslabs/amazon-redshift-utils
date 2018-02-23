/**********************************************************************************************
WLM QMR Rule Candidates

Purpose: 
    Results can be used to as starting point when implementing WLM Query Monitoring Rules. 
    QMR monitors certain metrics during query execution to catch poorly tuned or badly written 
    queries. Queries can either hop to a different queue or be aborted. QMR can be used to 
    prevent bad queries from degrading the overall performance of the cluster. 

    Calculate candidates for new QMR rules. The candidate rule will affect queries that are 
    beyond the 99th percentile in the WLM service class for each metric.  
        - https://docs.aws.amazon.com/redshift/latest/dg/cm-c-wlm-query-monitoring-rules.html

    Rank the rule candidates on how much the max observation is beyond the 99th percentile 
    ("magnitude"). Excludes rules which would be ineffective (catch too few queries) or too 
    broad (catch too many queries).  

Columns:
    service_class:  ID for the service class, defined in the WLM configuration file. 
    qmr_metric:     Name of the QMR metric
    p50:            The median value (50th percentile) for the metric
    p99:            The 99th percentile value for the metric
    pmax:           The maximum value for the metric
    rule_candidate: The rule candidate for evaluation 
    pmax_magnitude: Order of magnitude difference between candidate and maximum value
    rule_order:     Evaluation sequence ordered by most potential impact

Notes:
    - !! Please set your rule action to `LOG` initially to evaluate the impact !!
        - Check the STL_WLM_RULE_ACTION table to see which queries would be affected
          https://docs.aws.amazon.com/redshift/latest/dg/r_STL_WLM_RULE_ACTION.html
        - Once you are confident the rule affects the correct queries change the action
    - Only applicable for existing WLM queue configuration and only considers queries 
      as far back as records exist in SVL_QUERY_METRICS_SUMMARY.

History:
2017-07-31 joeharris76 created
2018-02-20 joeharris76 committed to Redshift Utils
**********************************************************************************************/

WITH qmr AS (
              SELECT service_class, 'query_cpu_time'            ::VARCHAR(30) qmr_metric, MEDIAN(query_cpu_time            ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY query_cpu_time            ) p99, MAX(query_cpu_time            ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'query_blocks_read'         ::VARCHAR(30) qmr_metric, MEDIAN(query_blocks_read         ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY query_blocks_read         ) p99, MAX(query_blocks_read         ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'query_execution_time'      ::VARCHAR(30) qmr_metric, MEDIAN(query_execution_time      ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY query_execution_time      ) p99, MAX(query_execution_time      ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'query_cpu_usage_percent'   ::VARCHAR(30) qmr_metric, MEDIAN(query_cpu_usage_percent   ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY query_cpu_usage_percent   ) p99, MAX(query_cpu_usage_percent   ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'query_temp_blocks_to_disk' ::VARCHAR(30) qmr_metric, MEDIAN(query_temp_blocks_to_disk ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY query_temp_blocks_to_disk ) p99, MAX(query_temp_blocks_to_disk ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'segment_execution_time'    ::VARCHAR(30) qmr_metric, MEDIAN(segment_execution_time    ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY segment_execution_time    ) p99, MAX(segment_execution_time    ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'cpu_skew'                  ::VARCHAR(30) qmr_metric, MEDIAN(cpu_skew                  ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY cpu_skew                  ) p99, MAX(cpu_skew                  ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'io_skew'                   ::VARCHAR(30) qmr_metric, MEDIAN(io_skew                   ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY io_skew                   ) p99, MAX(io_skew                   ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'scan_row_count'            ::VARCHAR(30) qmr_metric, MEDIAN(scan_row_count            ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY scan_row_count            ) p99, MAX(scan_row_count            ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'join_row_count'            ::VARCHAR(30) qmr_metric, MEDIAN(join_row_count            ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY join_row_count            ) p99, MAX(join_row_count            ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'nested_loop_join_row_count'::VARCHAR(30) qmr_metric, MEDIAN(nested_loop_join_row_count) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY nested_loop_join_row_count) p99, MAX(nested_loop_join_row_count) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'return_row_count'          ::VARCHAR(30) qmr_metric, MEDIAN(return_row_count          ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY return_row_count          ) p99, MAX(return_row_count          ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'spectrum_scan_row_count'   ::VARCHAR(30) qmr_metric, MEDIAN(spectrum_scan_row_count   ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY spectrum_scan_row_count   ) p99, MAX(spectrum_scan_row_count   ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1
    UNION ALL SELECT service_class, 'spectrum_scan_size_mb'     ::VARCHAR(30) qmr_metric, MEDIAN(spectrum_scan_size_mb     ) p50, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY spectrum_scan_size_mb     ) p99, MAX(spectrum_scan_size_mb     ) pmax FROM svl_query_metrics_summary WHERE userid > 1 GROUP BY 1    
)
SELECT service_class
      ,qmr_metric,p50,p99,pmax
      ,(LEFT(p99,1)::INT+1)*POWER(10,LENGTH((p99/10)::BIGINT)) candidate_rule
      ,ROUND(pmax/((LEFT(p99,1)::INT+1)*POWER(10,LENGTH((p99/10)::BIGINT))),2) pmax_magnitude
      ,ROW_NUMBER() OVER (PARTITION BY service_class ORDER BY (NVL(pmax,1)/((LEFT(p99,1)::INT+1)*POWER(10,LENGTH((p99/10)::BIGINT)))) DESC) rule_order
FROM   qmr
--Exclude rule if:
--  Values are too small to be effective
WHERE NVL(p99,0) >= 10 
--  Value distribution is too flat
AND (NVL(p50,0) + NVL(p99,0)) < NVL(pmax,0) 
--  Proposed rule has no effect
AND ((LEFT(p99,1)::INT+1)*POWER(10,LENGTH((p99/10)::BIGINT))) < NVL(pmax,0) 
ORDER BY service_class
        ,rule_order
;