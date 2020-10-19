--SVL_STATEMENTTEXT
UNLOAD ('SELECT * FROM SVL_STATEMENTTEXT WHERE userid>1') TO '' CREDENTIALS '';
--STL_Query
UNLOAD ('SELECT * FROM STL_QUERY WHERE userid>1') TO '' CREDENTIALS '';
--STL_WLM_QUERY
UNLOAD ('SELECT * FROM STL_WLM_QUERY WHERE userid>1') TO '' CREDENTIALS '';

--stl_wlm_service_class_config
--UNLOAD ('SELECT * FROM stl_wlm_service_class_config') TO '' CREDENTIALS '';

--stv_wlm_qmr_config
UNLOAD ('SELECT * FROM stv_wlm_qmr_config') TO '' CREDENTIALS '';

--stv_wlm_query_queue_state
UNLOAD ('SELECT * FROM stv_wlm_query_queue_state') TO '' CREDENTIALS '';
--stv_wlm_query_state
UNLOAD ('SELECT * FROM stv_wlm_query_state') TO '' CREDENTIALS '';
--stl_connection_log
UNLOAD ('SELECT * FROM stl_connection_log') TO '' CREDENTIALS '';
--stl_compile_info
--UNLOAD ('SELECT * FROM stl_compile_info WHERE userid>1') TO '' CREDENTIALS '';
--stl_catalog_bloat
--UNLOAD ('SELECT * FROM stl_catalog_bloat WHERE userid>1') TO '' CREDENTIALS '';
--stl_catalog_rebuild_info
--UNLOAD ('SELECT * FROM stl_catalog_rebuild_info') TO '' CREDENTIALS '';
--stl_query_metrics
UNLOAD ('SELECT * FROM stl_query_metrics WHERE userid>1') TO '' CREDENTIALS '';
--svl_query_summary
UNLOAD ('SELECT * FROM svl_query_summary WHERE userid>1') TO '' CREDENTIALS '';
--svl_query_report
UNLOAD ('SELECT * FROM svl_query_report WHERE userid>1') TO '' CREDENTIALS '';
--stl_vacuum
UNLOAD ('SELECT * FROM stl_vacuum WHERE userid>1') TO '' CREDENTIALS '';

--stl_s3client

UNLOAD ('SELECT * FROM stl_s3client WHERE userid>1') TO '' CREDENTIALS '';

--stl_tiered_storage_s3_blocks
--UNLOAD ('SELECT * FROM stl_tiered_storage_s3_blocks') TO '' CREDENTIALS '';


--stl_commit_stats
UNLOAD ('SELECT * FROM stl_commit_stats') TO '' CREDENTIALS '';

--svl_query_metrics_summary
UNLOAD ('SELECT * FROM svl_query_metrics_summary') TO '' CREDENTIALS '';
