CREATE SCHEMA IF NOT EXISTS history;

CREATE TABLE IF NOT EXISTS history.hist_stl_load_errors (LIKE STL_LOAD_ERRORS);

CREATE TABLE IF NOT EXISTS history.hist_stl_query (
  "userid" INTEGER NOT NULL  ENCODE lzo,
  "query" INTEGER NOT NULL  ENCODE lzo,
  "label" CHAR(30) NOT NULL  ENCODE lzo,
  "xid" BIGINT NOT NULL  ENCODE lzo,
  "pid" INTEGER NOT NULL  ENCODE lzo,
  "database" VARCHAR(32) NOT NULL  ENCODE lzo,
  "querytxt" VARCHAR(4000) NOT NULL  ENCODE lzo, 
  "starttime" TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE lzo,
  "endtime" TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE lzo,
  "aborted" INTEGER NOT NULL  ENCODE lzo,
  "insert_pristine" INTEGER NOT NULL  ENCODE lzo
);

CREATE TABLE IF NOT EXISTS history.hist_stl_wlm_query (LIKE stl_wlm_query);

CREATE TABLE IF NOT EXISTS history.hist_stl_explain
(
  "userid" INTEGER NOT NULL  ENCODE lzo,
  "query" INTEGER NOT NULL  ENCODE lzo,
  "nodeid" INTEGER NOT NULL  ENCODE lzo,
  "parentid" INTEGER NOT NULL  ENCODE lzo,
  "plannode" VARCHAR(400) NOT NULL  ENCODE lzo,
  "info" VARCHAR(400) NOT NULL  ENCODE lzo
);

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
CREATE TABLE IF NOT EXISTS history.hist_svl_s3query_summary
  (
    userid                   INTEGER,
    query                    INTEGER,
    xid                      BIGINT,
    pid                      INTEGER,
    segment                  INTEGER,
    step                     INTEGER,
    starttime                TIMESTAMP WITHOUT TIME ZONE,
    endtime                  TIMESTAMP WITHOUT TIME ZONE,
    elapsed                  BIGINT,
    aborted                  INTEGER,
    external_table_name      TEXT,
    file_format              CHARACTER(16),
    is_partitioned           TEXT,
    is_rrscan                TEXT,
    s3_scanned_rows          BIGINT,
    s3_scanned_bytes         BIGINT,
    s3query_returned_rows    BIGINT,
    s3query_returned_bytes   BIGINT,
    files                    BIGINT,
    files_max                INTEGER,
    files_avg                BIGINT,
    splits                   BIGINT,
    splits_max               INTEGER,
    splits_avg               BIGINT,
    total_split_size         BIGINT,
    max_split_size           BIGINT,
    avg_split_size           BIGINT,
    total_retries            BIGINT,
    max_retries              INTEGER,
    max_request_duration     INTEGER,
    avg_request_duration     BIGINT,
    max_request_parallelism  INTEGER,
    avg_request_parallelism  DOUBLE PRECISION
  );
ALTER TABLE history.hist_svl_s3query_summary ADD COLUMN is_nested TEXT DEFAULT NULL;
CREATE TABLE IF NOT EXISTS history.HIST_SVL_S3QUERY
  (
    userid                   INTEGER,
    query                    INTEGER,
    segment                  INTEGER,
    step                     INTEGER,
    node                     INTEGER,
    slice                    INTEGER,
    starttime                TIMESTAMP WITHOUT TIME ZONE,
    endtime                  TIMESTAMP WITHOUT TIME ZONE,
    elapsed                  BIGINT,
    external_table_name      TEXT,
    file_format              CHARACTER(16),
    is_partitioned           CHARACTER(1),
    is_rrscan                CHARACTER(1),
    s3_scanned_rows          BIGINT,
    s3_scanned_bytes         BIGINT,
    s3query_returned_rows    BIGINT,
    s3query_returned_bytes   BIGINT,
    files                    INTEGER,
    splits                   INTEGER,
    total_split_size         BIGINT,
    max_split_size           BIGINT,
    total_retries            INTEGER,
    max_retries              INTEGER,
    max_request_duration     INTEGER,
    avg_request_duration     INTEGER,
    max_request_parallelism  INTEGER,
    avg_request_parallelism  DOUBLE PRECISION
  );
ALTER TABLE history.hist_svl_s3query ADD COLUMN is_nested TEXT DEFAULT NULL;

CREATE TABLE IF NOT EXISTS history.hist_stl_query_metrics (like stl_query_metrics);