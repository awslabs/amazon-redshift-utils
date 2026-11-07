--DROP VIEW admin.v_mv_health;
/**********************************************************************************************
Purpose: View to report the health of every materialized view in the current database, with
         one row per MV: state, staleness, autorefresh interval, refresh cost, query activity,
         and MV-on-MV dependencies.

Notes:   Reads SVV_MV_INFO, SVV_MV_DEPENDENCY, SYS_MV_REFRESH_HISTORY, SYS_QUERY_DETAIL and
         SYS_QUERY_HISTORY, with a 7-day lookback (default system table retention).
         Requires Amazon Redshift patch 206 or later (autorefresh_interval columns).
         To see activity across all users, query it as a superuser, or as a user with the
         sys:monitor role and USAGE on each schema that contains MVs.
         Does not depend on mv_definitions. Join to it to add blocker_hint.

Usage:
    SELECT * FROM admin.v_mv_health;

    -- Add blocker_hint with sp_populate_mv_definitions() from amazon-redshift-udfs:
    -- https://github.com/aws-samples/amazon-redshift-udfs/tree/master/stored-procedures/sp_populate_mv_definitions
    CALL sp_populate_mv_definitions();
    SELECT v.*, d.blocker_hint
    FROM admin.v_mv_health v
    LEFT JOIN mv_definitions d
        ON v.mv_schema = d.mv_schema
       AND v.mv_name   = d.mv_name;

History:
2026-11-06 nickbarc Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_mv_health AS
WITH RECURSIVE

params AS (
    SELECT DATEADD('day', -7, GETDATE()) AS min_start_time
),

mv_info AS (
    SELECT
        TRIM(database_name) AS database_name,
        TRIM(schema_name) AS schema_name,
        TRIM(name) AS mv_name,
        state,
        CASE
            WHEN state = 1 THEN 'incremental'
            WHEN state = 0 THEN 'full_recompute'
            WHEN state = 101 THEN 'broken:column_dropped'
            WHEN state = 102 THEN 'broken:type_changed'
            WHEN state = 103 THEN 'broken:table_renamed'
            WHEN state = 104 THEN 'broken:column_renamed'
            WHEN state = 105 THEN 'broken:schema_renamed'
            WHEN state >= 101 THEN 'broken:' || state
            ELSE 'unknown'
        END AS state_label,
        is_stale,
        autorefresh AS autorefresh_enabled,
        autorefresh_interval,
        has_default_autorefresh_interval,
        autorewrite AS autorewrite_eligible
    FROM SVV_MV_INFO
),

refresh_stats AS (
    SELECT
        TRIM(schema_name) AS schema_name,
        TRIM(mv_name) AS mv_name,
        COUNT(*) AS refresh_count,
        AVG(CASE WHEN status NOT ILIKE '%already updated%'
                 THEN DATEDIFF('ms', start_time, end_time) / 1000.0 END) AS avg_refresh_sec,
        SUM(CASE WHEN status ILIKE '%recompute%' THEN 1 ELSE 0 END) AS recompute_count
    FROM SYS_MV_REFRESH_HISTORY
    CROSS JOIN params
    WHERE start_time >= params.min_start_time
    GROUP BY 1, 2
),

-- Each MV stores its result in an internal table in the pg_mv schema. Resolve
-- that table through the MV's rewrite rule rather than deriving its name: long
-- MV names get a hashed storage-table name. The name (unlike the OID) survives
-- a full recompute, so scans logged before the last recompute still match.
mv_storage AS (
    SELECT DISTINCT
        vn.nspname::VARCHAR(128) AS mv_schema,
        v.relname::VARCHAR(128)  AS mv_name,
        t.relname::VARCHAR(128)  AS storage_table
    FROM pg_class v
    JOIN pg_namespace vn ON vn.oid = v.relnamespace
    JOIN pg_rewrite r    ON r.ev_class = v.oid
    JOIN pg_depend d     ON d.objid = r.oid
    JOIN pg_class t      ON t.oid = d.refobjid
    JOIN pg_namespace tn ON tn.oid = t.relnamespace AND tn.nspname = 'pg_mv'
    WHERE v.relkind = 'v'
),

-- Queries that scanned the MV's stored result. This counts queries that name
-- the MV and queries the optimizer served through automatic query rewriting.
-- Scans made by refreshes (including a nested MV refreshing from its parent)
-- and by DDL (CREATE MATERIALIZED VIEW populating a nested MV from its parent)
-- are excluded.
mv_reads AS (
    SELECT
        i.schema_name,
        i.mv_name,
        COUNT(DISTINCT q.query_id) AS query_count
    FROM mv_info i
    JOIN mv_storage s
        ON s.mv_schema = i.schema_name
       AND s.mv_name   = i.mv_name
    CROSS JOIN params
    JOIN SYS_QUERY_DETAIL qd
        ON TRIM(qd.step_name) = 'scan'
       AND TRIM(qd.table_name) = i.database_name || '.pg_mv.' || s.storage_table
       AND qd.start_time >= params.min_start_time
    JOIN SYS_QUERY_HISTORY q
        ON q.query_id = qd.query_id
       AND q.status = 'success'
       AND q.query_type <> 'DDL'
       AND q.transaction_id NOT IN (SELECT transaction_id FROM SYS_MV_REFRESH_HISTORY
                                    WHERE transaction_id IS NOT NULL)
    GROUP BY i.schema_name, i.mv_name
),

mv_rows AS (
    SELECT t.schema AS mv_schema, t."table" AS mv_name, t.tbl_rows AS mv_rows
    FROM SVV_TABLE_INFO t
    WHERE t."table" IN (SELECT mv_name FROM mv_info)
),

chain_depth (mv_name, ancestor, depth) AS (
    SELECT name, dependent_name, 1
    FROM SVV_MV_DEPENDENCY

    UNION ALL

    SELECT cd.mv_name, d.dependent_name, cd.depth + 1
    FROM chain_depth cd
    JOIN SVV_MV_DEPENDENCY d ON cd.ancestor = d.name
    -- Cap recursion at depth 10. SVV_MV_DEPENDENCY should be a DAG, so this is a
    -- safety guard against an unexpected cycle or runaway fan-out rather than an
    -- expected limit; 10 is well beyond realistic MV-on-MV nesting.
    WHERE cd.depth < 10
),

max_chain AS (
    SELECT mv_name, MAX(depth) AS chain_depth
    FROM chain_depth
    GROUP BY mv_name
),

dep_list AS (
    SELECT mv_name,
           LISTAGG(DISTINCT ancestor, ', ') WITHIN GROUP (ORDER BY ancestor) AS mv_dependencies
    FROM chain_depth
    GROUP BY mv_name
),

stale_chain AS (
    SELECT
        cd.mv_name,
        LISTAGG(DISTINCT CASE WHEN i.is_stale = 't' THEN cd.ancestor END, ', ')
            WITHIN GROUP (ORDER BY cd.ancestor) AS stale_ancestors
    FROM chain_depth cd
    JOIN mv_info i ON cd.ancestor = i.mv_name
    GROUP BY cd.mv_name
),

-- Denominator for pct_of_cluster_time: all successful queries, plus refreshes
-- that SYS_QUERY_HISTORY doesn't record (autorefresh runs internally and is only
-- logged in SYS_MV_REFRESH_HISTORY). Manual refreshes are already counted in
-- SYS_QUERY_HISTORY, so only refreshes absent from it are added.
total_workload AS (
    SELECT GREATEST(q.query_us + a.auto_refresh_us, 1) AS total_us
    FROM (
        SELECT COALESCE(SUM(elapsed_time), 0) AS query_us
        FROM SYS_QUERY_HISTORY
        CROSS JOIN params
        WHERE start_time >= params.min_start_time
          AND status = 'success'
    ) q
    CROSS JOIN (
        SELECT COALESCE(SUM(DATEDIFF('us', r.start_time, r.end_time)), 0) AS auto_refresh_us
        FROM SYS_MV_REFRESH_HISTORY r
        CROSS JOIN params
        LEFT JOIN (SELECT DISTINCT transaction_id FROM SYS_QUERY_HISTORY) h
            ON h.transaction_id = r.transaction_id
        WHERE r.start_time >= params.min_start_time
          AND h.transaction_id IS NULL
    ) a
),

refresh_cost AS (
    SELECT TRIM(schema_name) AS schema_name, TRIM(mv_name) AS mv_name,
           SUM(DATEDIFF('us', start_time, end_time)) AS refresh_us
    FROM SYS_MV_REFRESH_HISTORY
    CROSS JOIN params
    WHERE start_time >= params.min_start_time
    GROUP BY 1, 2
),

last_refresh AS (
    SELECT schema_name, mv_name, status AS last_refresh_status
    FROM (
        SELECT TRIM(schema_name) AS schema_name, TRIM(mv_name) AS mv_name, status,
               ROW_NUMBER() OVER (PARTITION BY TRIM(schema_name), TRIM(mv_name)
                                  ORDER BY start_time DESC) AS rn
        FROM SYS_MV_REFRESH_HISTORY
    )
    WHERE rn = 1
)

SELECT
    i.schema_name                                   AS mv_schema,
    i.mv_name,
    i.state || ' (' || i.state_label || ')'         AS state,
    CASE i.is_stale WHEN 't' THEN 'true' WHEN 'f' THEN 'false' ELSE 'unknown' END AS is_stale,
    CASE i.autorefresh_enabled WHEN 't' THEN 'true' ELSE 'false' END               AS is_autorefresh,
    i.autorefresh_interval                          AS autorefresh_interval_sec,
    CASE i.has_default_autorefresh_interval WHEN 't' THEN 'true' ELSE 'false' END   AS is_default_autorefresh_interval,
    CASE i.autorewrite_eligible WHEN 't' THEN 'true' ELSE 'false' END               AS is_autorewrite_eligible,
    COALESCE(rs.refresh_count, 0)                   AS refresh_count,
    COALESCE(rd.query_count, 0)                     AS query_count,
    CASE WHEN COALESCE(rd.query_count, 0) > 0
         THEN ROUND(COALESCE(rs.recompute_count, 0)::FLOAT / rd.query_count, 1)
    END                                             AS recompute_to_query_ratio,
    ROUND(COALESCE(rs.avg_refresh_sec, 0), 1)       AS avg_refresh_sec,
    COALESCE(lr.last_refresh_status, '')            AS last_refresh_status,
    COALESCE(mr.mv_rows, 0)                         AS mv_rows,
    COALESCE(mc.chain_depth, 0)                     AS chain_depth,
    COALESCE(dl.mv_dependencies, '')                AS mv_dependencies,
    COALESCE(sc.stale_ancestors, '')                AS stale_ancestors,
    ROUND(COALESCE(rc.refresh_us, 0)::FLOAT / tw.total_us * 100, 2) AS pct_of_cluster_time
FROM mv_info i
LEFT JOIN refresh_stats rs  ON i.mv_name = rs.mv_name AND i.schema_name = rs.schema_name
LEFT JOIN mv_reads rd       ON i.mv_name = rd.mv_name AND i.schema_name = rd.schema_name
LEFT JOIN mv_rows mr        ON i.mv_name = mr.mv_name AND i.schema_name = mr.mv_schema
LEFT JOIN max_chain mc      ON i.mv_name = mc.mv_name
LEFT JOIN dep_list dl       ON i.mv_name = dl.mv_name
LEFT JOIN stale_chain sc    ON i.mv_name = sc.mv_name
LEFT JOIN refresh_cost rc   ON i.mv_name = rc.mv_name AND i.schema_name = rc.schema_name
LEFT JOIN last_refresh lr   ON i.mv_name = lr.mv_name AND i.schema_name = lr.schema_name
CROSS JOIN total_workload tw
ORDER BY
    CASE WHEN i.state >= 101 THEN 0 WHEN i.state = 0 THEN 1 ELSE 2 END,
    COALESCE(rc.refresh_us, 0) DESC;
