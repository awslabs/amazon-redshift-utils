create schema if not exists history;

CREATE TABLE IF NOT EXISTS history.hist_stl_load_errors (LIKE STL_LOAD_ERRORS);
CREATE TABLE IF NOT EXISTS history.hist_stl_query (LIKE stl_query);
CREATE TABLE IF NOT EXISTS history.hist_stl_wlm_query (LIKE stl_wlm_query);
CREATE TABLE IF NOT EXISTS history.hist_stl_explain (LIKE stl_explain);
CREATE TABLE IF NOT EXISTS history.hist_svl_query_summary
  (
    userid            INTEGER ENCODE zstd,
    query             INTEGER ENCODE zstd,
    stm               INTEGER ENCODE zstd,
    seg               INTEGER ENCODE zstd,
    step              INTEGER ENCODE zstd,
    maxtime           BIGINT ENCODE zstd,
    avgtime           BIGINT ENCODE zstd,
    ROWS              BIGINT ENCODE zstd,
    bytes             BIGINT ENCODE zstd,
    rate_row          DOUBLE precision ENCODE zstd,
    rate_byte         DOUBLE precision ENCODE zstd,
    label             TEXT ENCODE zstd,
    is_diskbased      CHARACTER(1) ENCODE zstd,
    workmem           BIGINT ENCODE zstd,
    is_rrscan         CHARACTER(1) ENCODE zstd,
    is_delayed_scan   CHARACTER(1) ENCODE zstd,
    rows_pre_filter   BIGINT ENCODE zstd
  );
ALTER TABLE HISTORY.hist_svl_s3query_summary add column is_nested text default null;
CREATE TABLE IF NOT EXISTS history.hist_svl_s3query_summary
  (
    userid                   integer,
    query                    integer,
    xid                      bigint,
    pid                      integer,
    segment                  integer,
    step                     integer,
    starttime                timestamp without time zone,
    endtime                  timestamp without time zone,
    elapsed                  bigint,
    aborted                  integer,
    external_table_name      text,
    file_format              character(16),
    is_partitioned           text,
    is_rrscan                text,
    s3_scanned_rows          bigint,
    s3_scanned_bytes         bigint,
    s3query_returned_rows    bigint,
    s3query_returned_bytes   bigint,
    files                    bigint,
    files_max                integer,
    files_avg                bigint,
    splits                   bigint,
    splits_max               integer,
    splits_avg               bigint,
    total_split_size         bigint,
    max_split_size           bigint,
    avg_split_size           bigint,
    total_retries            bigint,
    max_retries              integer,
    max_request_duration     integer,
    avg_request_duration     bigint,
    max_request_parallelism  integer,
    avg_request_parallelism  double precision
  );
CREATE TABLE IF NOT EXISTS HISTORY.HIST_SVL_S3QUERY
  (
    userid                   integer,
    query                    integer,
    segment                  integer,
    step                     integer,
    node                     integer,
    slice                    integer,
    starttime                timestamp without time zone,
    endtime                  timestamp without time zone,
    elapsed                  bigint,
    external_table_name      text,
    file_format              character(16),
    is_partitioned           character(1),
    is_rrscan                character(1),
    s3_scanned_rows          bigint,
    s3_scanned_bytes         bigint,
    s3query_returned_rows    bigint,
    s3query_returned_bytes   bigint,
    files                    integer,
    splits                   integer,
    total_split_size         bigint,
    max_split_size           bigint,
    total_retries            integer,
    max_retries              integer,
    max_request_duration     integer,
    avg_request_duration     integer,
    max_request_parallelism  integer,
    avg_request_parallelism  double precision
  );
ALTER TABLE HISTORY.hist_svl_s3query add column is_nested text default null;