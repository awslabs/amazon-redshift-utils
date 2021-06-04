drop table if exists public.source_cluster_query_stats cascade;
drop table if exists public.target_cluster_query_stats cascade;
drop table if exists public.replica_cluster_query_stats cascade;

CREATE TABLE IF NOT EXISTS public.source_cluster_query_stats(
  queue                VARCHAR(64)
, username           VARCHAR(128)
, cc_scaling         INT
, aborted            INT
, queue_time   BIGINT
, compile_time BIGINT
, exec_time    BIGINT
, total_query_time   BIGINT
, userid             INT
, query              INT
, query_label        VARCHAR(320)
, xid                BIGINT
, pid                INT
, service_class      INT
, starttime          TIMESTAMP WITHOUT TIME ZONE
, endtime            TIMESTAMP WITHOUT TIME ZONE
, tables_scanned     VARCHAR(65535)
, querytxt           VARCHAR(4000));

CREATE TABLE IF NOT EXISTS public.target_cluster_query_stats(
  queue                VARCHAR(64)
, username           VARCHAR(128)
, cc_scaling         INT
, aborted            INT
, queue_time   BIGINT
, compile_time BIGINT
, exec_time    BIGINT
, total_query_time   BIGINT
, userid             INT
, query              INT
, query_label        VARCHAR(320)
, xid                BIGINT
, pid                INT
, service_class      INT
, starttime          TIMESTAMP WITHOUT TIME ZONE
, endtime            TIMESTAMP WITHOUT TIME ZONE
, tables_scanned     VARCHAR(65535)
, querytxt           VARCHAR(4000));


CREATE TABLE IF NOT EXISTS public.replica_cluster_query_stats(
  queue                VARCHAR(64)
, username           VARCHAR(128)
, cc_scaling         INT
, aborted            INT
, queue_time   BIGINT
, compile_time BIGINT
, exec_time    BIGINT
, total_query_time   BIGINT
, userid             INT
, query              INT
, query_label        VARCHAR(320)
, xid                BIGINT
, pid                INT
, service_class      INT
, starttime          TIMESTAMP WITHOUT TIME ZONE
, endtime            TIMESTAMP WITHOUT TIME ZONE
, tables_scanned     VARCHAR(65535)
, querytxt           VARCHAR(4000));


select count(1) from public.source_cluster_query_stats;

drop view if exists public.detailed_query_stats cascade;

CREATE OR replace VIEW public.detailed_query_stats
AS
  WITH queries
       AS (SELECT query,
                  Listagg(DISTINCT schemaname
                                   ||'.'
                                   ||table_name, ',')
                    within GROUP(ORDER BY table_name) tables_scanned
           FROM   (WITH scan_delete_insert
                        AS (SELECT 'scan'                query_type,
                                   query,
                                   Lpad(segment, 3, '0') segment,
                                   tbl
                            FROM   stl_scan
                            WHERE  userid > 1
                                   AND perm_table_name != 'Internal Worktable'
                                   AND tbl <> 0
                            UNION ALL
                            SELECT 'delete'              query_type,
                                   query,
                                   Lpad(segment, 3, '0') segment,
                                   tbl
                            FROM   stl_delete
                            WHERE  userid > 1
                                   AND tbl <> 0
                            UNION ALL
                            SELECT 'insert'              query_type,
                                   query,
                                   Lpad(segment, 3, '0') segment,
                                   tbl
                            FROM   stl_insert
                            WHERE  userid > 1
                                   AND tbl <> 0)
                   SELECT sdi.query_type,
                          sdi.query,
                          sdi.segment,
                          sdi.tbl,
                          Trim(n.nspname) AS schemaname,
                          Trim(c.relname) table_name
                    FROM   scan_delete_insert sdi
                           join pg_class c
                             ON c.oid = sdi.tbl
                           join pg_namespace n
                             ON n.oid = c.relnamespace)
           GROUP  BY query),
       compiles
       AS (SELECT query,
                  SUM(Datediff (microsecond, q.starttime, q.endtime))
                  compile_time
           FROM   svl_compile q
           WHERE  COMPILE = 1
           GROUP  BY query)
  SELECT Trim(s.name)                                      queue,
         Trim(u.usename)                                   AS username,
         CASE
           WHEN q.concurrency_scaling_status = 1 THEN 1
           ELSE 0
         END                                               AS cc_scaling,
         q.aborted,
         w.total_queue_time                                queue_time,
         Nvl(ct.compile_time, 0)                     compile_time,
         w.total_exec_time - Nvl(ct.compile_time, 0) exec_time,
         Datediff(microsecond, q.starttime, q.endtime)     AS total_query_time,
         q.userid,
         q.query,
         q.label                                           query_label,
         q.xid,
         q.pid,
         w.service_class,
         q.starttime,
         q.endtime,
         tables_scanned,
         Trim(q.querytxt)                                  querytxt
  FROM   stl_query q
         join stl_wlm_query w USING (userid, query)
         join pg_user u
           ON u.usesysid = q.userid
         join stv_wlm_service_class_config s
           ON w.service_class = s.service_class
         left outer join queries
                      ON queries.query = q.query
         left outer join compiles ct
                      ON w.query = ct.query
  WHERE  q.userid > 1
         AND w.service_class > 5;


drop view if exists public.source_target_comparison_raw cascade;

create or replace view public.source_target_comparison_raw as
SELECT
         COUNT(1) total_executions
         , MIN(CASE
                 WHEN source = 'replica'
                 THEN queue
               END) replica_queue
         , MIN(CASE
                 WHEN source = 'target'
                 THEN queue
               END) target_queue
         , MIN(CASE
                 WHEN source = 'replica'
                 THEN username
               END) replica_username
         , MIN(CASE
                 WHEN source = 'target'
                 THEN username
               END) target_username
         , MIN(CASE
                 WHEN source = 'replica'
                 THEN query_label
               END) replica_query_label
         , MIN(CASE
                 WHEN source = 'target'
                 THEN query_label
               END) target_query_label
         , MIN(CASE
                 WHEN source = 'replica'
                 THEN total_query_time
               END) replica_total_query_time
         , MIN(CASE
                 WHEN source = 'target'
                 THEN total_query_time
               END) target_total_query_time
         , MIN(CASE
                 WHEN source = 'replica'
                 THEN queue_time
               END) replica_total_queue_time
         , MIN(CASE
                 WHEN source = 'target'
                 THEN queue_time
               END) target_total_queue_time
         , MIN(CASE
                 WHEN source = 'replica'
                 THEN compile_time
               END) replica_total_compile_time
         , MIN(CASE
                 WHEN source = 'target'
                 THEN compile_time
               END) target_total_compile_time
         , MIN(CASE
                 WHEN source = 'replica'
                 THEN exec_time
               END) replica_total_exec_time
         , MIN(CASE
                 WHEN source = 'target'
                 THEN exec_time
               END) target_total_exec_time
         , MIN(CASE
                 WHEN source = 'replica'
                 THEN cc_scaling
               END) replica_cc_scaling
         , MIN(CASE
                 WHEN source = 'target'
                 THEN cc_scaling
               END) target_cc_scaling
         , MIN(CASE
                 WHEN source = 'replica'
                 THEN xid
               END) replica_xid
         , MIN(CASE
                 WHEN source = 'target'
                 THEN xid
               END) target_xid
         , MIN(CASE
                 WHEN source = 'replica'
                 THEN query
               END) query
         , MIN(CASE
                 WHEN source = 'target'
                 THEN query
               END) target_query
         , MIN(CASE
                 WHEN source = 'replica'
                 THEN starttime
               END) replica_starttime
         , MIN(CASE
                 WHEN source = 'replica'
                 THEN endtime
               END) replica_endtime
         , MIN(CASE
                 WHEN source = 'target'
                 THEN starttime
               END) target_starttime
         , MIN(CASE
                 WHEN source = 'target'
                 THEN endtime
               END) target_endtime
         , MIN(CASE
                 WHEN source = 'replica'
                 THEN querytxt
               END) querytxt
     FROM
         (SELECT
               'replica' source
               , queue
               , username
               , cc_scaling
               , aborted
               , queue_time
               , compile_time
               , exec_time
               , total_query_time
               , userid
               , query
               , query_label
               , xid
               , pid
               , service_class
               , starttime
               , endtime
               , tables_scanned
               , querytxt
               , SHA2(querytxt,256) query_hash
           FROM
               public.replica_cluster_query_stats q
           WHERE  EXISTS (SELECT
                             1
                         FROM
                             public.target_cluster_query_stats x
                         WHERE  SHA2(x.querytxt,256) = SHA2(q.querytxt,256))
           UNION ALL
           SELECT
               'target' source
               , queue
               , username
               , cc_scaling
               , aborted
               , queue_time
               , compile_time
               , exec_time
               , total_query_time
               , userid
               , query
               , query_label
               , xid
               , pid
               , service_class
               , starttime
               , endtime
               , tables_scanned
               , querytxt
               , SHA2(querytxt,256) query_hash
           FROM
               public.target_cluster_query_stats q
           WHERE  EXISTS (SELECT
                             1
                         FROM
                             public.replica_cluster_query_stats x
                         WHERE  SHA2(x.querytxt,256) = SHA2(q.querytxt,256)))
     GROUP BY
         query_hash;


drop view if exists public.source_target_comparison cascade;


create or replace view public.source_target_comparison as
WITH replica_queries
     AS ( SELECT
              r.queue
              , r.username
              , r.cc_scaling
              , SUM(1) total_queries
              , ROUND(SUM(r.total_query_time::NUMERIC) / ( 1000 * 1000 ) ,2) total_query_time_seconds
              , ROUND(AVG(r.total_query_time::NUMERIC) / ( 1000 * 1000 ) ,2) mean_query_time_seconds
              , ROUND(( PERCENTILE_CONT(0.50) WITHIN GROUP( ORDER BY total_query_time) )::NUMERIC / ( 1000 * 1000 ),3) AS median_query_time_seconds
              , ROUND(( PERCENTILE_CONT(0.75) WITHIN GROUP( ORDER BY total_query_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct75_query_time_seconds
              , ROUND(( PERCENTILE_CONT(0.90) WITHIN GROUP( ORDER BY total_query_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct90_query_time_seconds
              , ROUND(( PERCENTILE_CONT(0.95) WITHIN GROUP( ORDER BY total_query_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct95_query_time_seconds
              , ROUND(MAX(total_query_time)::NUMERIC / ( 1000 * 1000 ),2) max_query_time_seconds
          FROM
              public.replica_cluster_query_stats r
          WHERE    aborted = 0
                   AND EXISTS (SELECT
                                   1
                               FROM
                                   public.target_cluster_query_stats t
                               WHERE  SHA2(t.querytxt::TEXT, 256) = SHA2(r.querytxt::TEXT, 256)
                                      AND r.userid = t.userid
                                      AND r.query_label = t.query_label
                                      AND r.aborted = t.aborted)
          GROUP BY
              queue
              , username
              , cc_scaling ), target_queries
     AS ( SELECT
              t.queue
              , t.username
              , t.cc_scaling
              , SUM(1) total_queries
              , ROUND(SUM(t.total_query_time::NUMERIC) / ( 1000 * 1000 ) ,2) total_query_time_seconds
              , ROUND(AVG(t.total_query_time::NUMERIC) / ( 1000 * 1000 ) ,2) mean_query_time_seconds
              , ROUND(( PERCENTILE_CONT(0.50) WITHIN GROUP( ORDER BY total_query_time) )::NUMERIC / ( 1000 * 1000 ),3) AS median_query_time_seconds
              , ROUND(( PERCENTILE_CONT(0.75) WITHIN GROUP( ORDER BY total_query_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct75_query_time_seconds
              , ROUND(( PERCENTILE_CONT(0.90) WITHIN GROUP( ORDER BY total_query_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct90_query_time_seconds
              , ROUND(( PERCENTILE_CONT(0.95) WITHIN GROUP( ORDER BY total_query_time) )::NUMERIC / ( 1000 * 1000 ),3) AS pct95_query_time_seconds
              , ROUND(MAX(total_query_time)::NUMERIC / ( 1000 * 1000 ),2) max_query_time_seconds
          FROM
              public.target_cluster_query_stats t
          WHERE    aborted = 0
                   AND EXISTS (SELECT
                                   1
                               FROM
                                   public.replica_cluster_query_stats r
                               WHERE  SHA2(t.querytxt::TEXT, 256) = SHA2(r.querytxt::TEXT, 256)
                                      AND r.userid = t.userid
                                      AND r.query_label = t.query_label
                                      AND r.aborted = t.aborted)
          GROUP BY
              queue
              , username
              , cc_scaling )
    SELECT
        queue
        , username
        , source_cluster
        , cc_scaling
        , total_queries
        , total_query_time_seconds
        , ( CASE
              WHEN source_cluster = 'target'
              THEN LAG(total_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
              WHEN source_cluster = 'replica'
              THEN LEAD(total_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
            END ) - total_query_time_seconds total_query_time_saved_seconds
        , mean_query_time_seconds
        , ( CASE
              WHEN source_cluster = 'target'
              THEN LAG(mean_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
              WHEN source_cluster = 'replica'
              THEN LEAD(mean_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
            END ) - mean_query_time_seconds mean_query_time_saved_seconds
        , median_query_time_seconds
        , ( CASE
              WHEN source_cluster = 'target'
              THEN LAG(median_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
              WHEN source_cluster = 'replica'
              THEN LEAD(median_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
            END ) - median_query_time_seconds median_query_time_saved_seconds
        , pct75_query_time_seconds
        , ( CASE
              WHEN source_cluster = 'target'
              THEN LAG(pct75_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
              WHEN source_cluster = 'replica'
              THEN LEAD(pct75_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
            END ) - pct75_query_time_seconds pct75_query_time_saved_seconds
        , pct90_query_time_seconds
        , ( CASE
              WHEN source_cluster = 'target'
              THEN LAG(pct90_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
              WHEN source_cluster = 'replica'
              THEN LEAD(pct90_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
            END ) - pct90_query_time_seconds pct90_query_time_saved_seconds
        , pct95_query_time_seconds
        , ( CASE
              WHEN source_cluster = 'target'
              THEN LAG(pct95_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
              WHEN source_cluster = 'replica'
              THEN LEAD(pct95_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
            END ) - pct95_query_time_seconds pct95_query_time_saved_seconds
        , max_query_time_seconds
        , ( CASE
              WHEN source_cluster = 'target'
              THEN LAG(max_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
              WHEN source_cluster = 'replica'
              THEN LEAD(max_query_time_seconds,1) OVER (ORDER BY queue, username, source_cluster,cc_scaling)
            END ) - max_query_time_seconds max_query_time_saved_seconds
    FROM
        (SELECT
             queue
             , username
             , 'replica' source_cluster
             , cc_scaling
             , total_queries
             , total_query_time_seconds
             , mean_query_time_seconds
             , median_query_time_seconds
             , pct75_query_time_seconds
             , pct90_query_time_seconds
             , pct95_query_time_seconds
             , max_query_time_seconds
         FROM
             replica_queries
         UNION ALL
         SELECT
             queue
             , username
             , 'target' source_cluster
             , cc_scaling
             , total_queries
             , total_query_time_seconds
             , mean_query_time_seconds
             , median_query_time_seconds
             , pct75_query_time_seconds
             , pct90_query_time_seconds
             , pct95_query_time_seconds
             , max_query_time_seconds
         FROM
             target_queries)
    ORDER BY
        1
        , 2
        , 3
        , 4;


drop view if exists public.source_target_queries_15_minutes_interval cascade;

CREATE OR REPLACE VIEW public.source_target_queries_15_minutes_interval AS
SELECT
    replica.period AS time_interval_15_minutes
    , replica.total_queries AS replica_total_queries
    , target.total_queries AS target_total_queries
    , replica.query_time_seconds_50pct AS replica_query_time_seconds_50pct
    , target.query_time_seconds_50pct AS target_query_time_seconds_50pct
    , replica.query_time_seconds_75pct AS replica_query_time_seconds_75pct
    , target.query_time_seconds_75pct AS target_query_time_seconds_75pct
    , replica.query_time_seconds_90pct AS replica_query_time_seconds_90pct
    , target.query_time_seconds_90pct AS target_query_time_seconds_90pct
    , replica.query_time_seconds_95pct AS replica_query_time_seconds_95pct
    , target.query_time_seconds_95pct AS target_query_time_seconds_95pct
    , replica.query_time_seconds_99pct AS replica_query_time_seconds_99pct
    , target.query_time_seconds_99pct AS target_query_time_seconds_99pct
    , replica.max_queue_time AS replica_max_queue_time
    , target.max_queue_time AS target_max_queue_time
    , replica.max_compile_time AS replica_max_compile_time
    , target.max_compile_time AS target_max_compile_time
    , replica.max_exec_time AS replica_max_exec_time
    , target.max_exec_time AS target_max_exec_time
    , replica.max_query_time AS replica_max_query_time
    , target.max_query_time AS target_max_query_time
    , replica.avg_queue_time AS replica_avg_queue_time
    , target.avg_queue_time AS target_avg_queue_time
    , replica.avg_compile_time AS replica_avg_compile_time
    , target.avg_compile_time AS target_avg_compile_time
    , replica.avg_exec_time AS replica_avg_exec_time
    , target.avg_exec_time AS target_avg_exec_time
    , replica.avg_query_time AS replica_avg_query_time
    , target.avg_query_time AS target_avg_query_time
FROM
    (SELECT
         DATEADD(min, ( EXTRACT(minute FROM starttime) / 15::INT ) * 15,DATE_TRUNC('hour',starttime)) AS period
         , COUNT(1) AS total_queries
         , ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_query_time) / ( 1000 * 1000 ),2) AS query_time_seconds_50pct
         , ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_query_time) / ( 1000 * 1000 ),2) AS query_time_seconds_75pct
         , ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_query_time) / ( 1000 * 1000 ),2) AS query_time_seconds_90pct
         , ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_query_time) / ( 1000 * 1000 ),2) AS query_time_seconds_95pct
         , ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_query_time) / ( 1000 * 1000 ),2) AS query_time_seconds_99pct
         , MAX(queue_time) AS max_queue_time
         , MAX(compile_time) AS max_compile_time
         , MAX(exec_time) AS max_exec_time
         , MAX(total_query_time) AS max_query_time
         , AVG(queue_time) AS avg_queue_time
         , AVG(compile_time) AS avg_compile_time
         , AVG(exec_time) AS avg_exec_time
         , AVG(total_query_time) AS avg_query_time
     FROM
         public.replica_cluster_query_stats
     GROUP BY
         1) AS replica,
    (SELECT
         DATEADD(min, ( EXTRACT(minute FROM starttime) / 15::INT ) * 15,DATE_TRUNC('hour',starttime)) AS period
         , COUNT(1) AS total_queries
         , ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_query_time) / ( 1000 * 1000 ),2) AS query_time_seconds_50pct
         , ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_query_time) / ( 1000 * 1000 ),2) AS query_time_seconds_75pct
         , ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_query_time) / ( 1000 * 1000 ),2) AS query_time_seconds_90pct
         , ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_query_time) / ( 1000 * 1000 ),2) AS query_time_seconds_95pct
         , ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_query_time) / ( 1000 * 1000 ),2) AS query_time_seconds_99pct
         , MAX(queue_time) AS max_queue_time
         , MAX(compile_time) AS max_compile_time
         , MAX(exec_time) AS max_exec_time
         , MAX(total_query_time) AS max_query_time
         , AVG(queue_time) AS avg_queue_time
         , AVG(compile_time) AS avg_compile_time
         , AVG(exec_time) AS avg_exec_time
         , AVG(total_query_time) AS avg_query_time
     FROM
         public.target_cluster_query_stats
     GROUP BY
         1) AS target
WHERE  target.period = replica.period ;


grant select on public.source_cluster_query_stats to public;
grant select on public.target_cluster_query_stats to public;
grant select on public.replica_cluster_query_stats to public;
grant select on public.detailed_query_stats to public;
grant select on public.source_target_queries_15_minutes_interval to public;
grant select on public.source_target_comparison to public;
grant select on public.source_target_comparison_raw to public;


CREATE OR REPLACE PROCEDURE public.unload_detailed_query_stats(bucket_prefix in varchar(100)) AS
$$
BEGIN
execute 'unload (''select * from public.detailed_query_stats where starttime > to_timestamp(''''' + bucket_prefix + ''''',''''YYYY-MM-DD-HH24-MI-SS'''')'') to ''s3://<<replay_bucket>>/' + bucket_prefix +   '/replica/detailed_query_stats/'' FORMAT AS PARQUET ALLOWOVERWRITE iam_role ''<<redshift_iam_role>>''';
END;
$$
LANGUAGE plpgsql;




CREATE OR REPLACE PROCEDURE load_detailed_query_stats(bucket_prefix in varchar(100)) AS
$$
BEGIN
truncate table source_cluster_query_stats;
truncate table replica_cluster_query_stats;
truncate table target_cluster_query_stats;
execute 'copy public.source_cluster_query_stats FROM ''s3://<<replay_bucket>>/' + bucket_prefix + '/source/detailed_query_stats/'' iam_role ''<<redshift_iam_role>>'' format parquet';
execute 'copy public.replica_cluster_query_stats FROM ''s3://<<replay_bucket>>/' + bucket_prefix + '/replica/detailed_query_stats/'' iam_role ''<<redshift_iam_role>>'' format parquet';
execute 'insert into public.target_cluster_query_stats select * from public.detailed_query_stats where starttime>to_timestamp(''' + bucket_prefix + ''',''YYYY-MM-DD-HH24-MI-SS'')';
execute 'unload (''select * from public.replica_cluster_query_stats order by starttime'') to ''s3://<<replay_bucket>>/' + bucket_prefix +   '/comparison_output/replica_cluster_query_stats.csv'' FORMAT AS CSV HEADER PARALLEL OFF ALLOWOVERWRITE iam_role ''<<redshift_iam_role>>''';
execute 'unload (''select * from public.target_cluster_query_stats order by starttime'') to ''s3://<<replay_bucket>>/' + bucket_prefix +   '/comparison_output/target_cluster_query_stats.csv'' FORMAT AS CSV HEADER PARALLEL OFF ALLOWOVERWRITE iam_role ''<<redshift_iam_role>>''';
execute 'unload (''select * from public.source_target_comparison order by 1,2,3,4'') to ''s3://<<replay_bucket>>/' + bucket_prefix +   '/comparison_output/source_target_comparison.csv'' FORMAT AS CSV HEADER PARALLEL OFF ALLOWOVERWRITE iam_role ''<<redshift_iam_role>>''';
execute 'unload (''select * from public.source_target_queries_15_minutes_interval order by 1'') to ''s3://<<replay_bucket>>/' + bucket_prefix +   '/comparison_output/source_target_queries_15_minutes_interval.csv'' FORMAT AS CSV HEADER PARALLEL OFF ALLOWOVERWRITE iam_role ''<<redshift_iam_role>>''';

commit;
END;
$$
LANGUAGE plpgsql;
