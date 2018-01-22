CREATE OR REPLACE VIEW history.all_stl_load_errors AS
(
SELECT le.*  FROM stl_load_errors le
UNION ALL
SELECT h.* FROM stl_load_errors le
RIGHT OUTER JOIN history.hist_stl_load_errors h ON (le.query = h.query AND le.starttime = h.starttime)
WHERE le.query IS NULL
);

CREATE OR REPLACE VIEW history.all_stl_query AS
(
SELECT q.* FROM stl_query q
UNION ALL
SELECT h.* FROM stl_query q
RIGHT OUTER JOIN history.hist_stl_query h ON (q.query = h.query AND q.starttime = h.starttime)
WHERE q.query IS NULL
);

CREATE OR REPLACE VIEW history.all_stl_wlm_query AS
(
SELECT wq.* FROM stl_wlm_query wq
UNION ALL
SELECT h.* FROM stl_wlm_query wq
RIGHT OUTER JOIN history.hist_stl_wlm_query h ON (wq.query = h.query AND wq.service_class_start_time = h.service_class_start_time)
WHERE wq.query IS NULL
);

CREATE OR REPLACE VIEW history.all_stl_explain AS
(
SELECT e.* FROM stl_explain e
UNION ALL
SELECT h.* FROM stl_explain e
RIGHT OUTER JOIN history.hist_stl_explain h ON (e.query = h.query AND e.userid = h.userid AND e.nodeid = h.nodeid AND e.parentid = h.parentid AND e.plannode = h.plannode)
WHERE e.query IS NULL
);

CREATE OR REPLACE VIEW history.all_svl_query_summary AS
(
SELECT qs.* FROM svl_query_summary qs
UNION ALL
SELECT h.* FROM svl_query_summary qs
RIGHT OUTER JOIN history.hist_svl_query_summary h ON (qs.query = h.query AND qs.userid = h.userid AND qs.stm = h.stm AND qs.seg = h.seg AND qs.step = h.step AND qs.maxtime = h.maxtime AND qs.label = h.label)
WHERE qs.query IS NULL
);
