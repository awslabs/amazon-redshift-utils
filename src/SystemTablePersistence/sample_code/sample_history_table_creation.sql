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
    
