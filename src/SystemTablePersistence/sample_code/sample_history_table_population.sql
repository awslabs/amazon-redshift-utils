INSERT INTO history.hist_stl_load_errors (
  SELECT le.* FROM stl_load_errors le, (SELECT NVL(MAX(starttime),'01/01/1902'::TIMESTAMP) AS max_starttime FROM history.hist_stl_load_errors) h WHERE le.starttime > h.max_starttime);
INSERT INTO history.hist_stl_wlm_query (
  SELECT wq.* FROM stl_wlm_query wq, (SELECT NVL(MAX(service_class_start_time),'01/01/1902'::TIMESTAMP) AS max_service_class_start_time FROM history.hist_stl_wlm_query) h WHERE wq.service_class_start_time > h.max_service_class_start_time);

BEGIN;
INSERT INTO history.hist_stl_query (
  SELECT q.* FROM stl_query q, (SELECT NVL(MAX(starttime),'01/01/1902'::TIMESTAMP) AS max_starttime FROM history.hist_stl_query) h WHERE q.starttime > h.max_starttime);
INSERT INTO history.hist_stl_explain (
  SELECT e.* FROM stl_explain e, stl_query q, (SELECT NVL(MAX(starttime),'01/01/1902'::TIMESTAMP) AS max_starttime FROM history.hist_stl_query) h WHERE e.query = q.query AND q.starttime > h.max_starttime);
INSERT INTO history.hist_svl_query_summary (
  SELECT qs.* FROM svl_query_summary qs, stl_query q, (SELECT NVL(MAX(starttime),'01/01/1902'::TIMESTAMP) AS max_starttime FROM history.hist_stl_query) h WHERE qs.query = q.query AND q.starttime > h.max_starttime);
COMMIT;

