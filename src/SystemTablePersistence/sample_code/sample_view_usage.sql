SELECT * FROM history.all_stl_load_errors WHERE UPPER(err_reason) LIKE '%DELIMITER NOT FOUND%';

SELECT * FROM history.all_stl_query WHERE query = 1121;

SELECT COUNT(*) FROM history.all_stl_wlm_query WHERE service_class = 6;

SELECT * FROM history.all_stl_explain WHERE query = 1121 ORDER BY nodeid;

SELECT * FROM history.all_svl_query_summary WHERE bytes > 1000000;

