--DROP VIEW admin.v_generate_cursor_query;
/**********************************************************************************************
Purpose: View to get the query and statistics of the currently active cursors.
History:
2016-09-14 Jan-Zeiseweis Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_generate_cursor_query
AS
SELECT
  cur.xid
  , cur.pid
  , cur.userid                                                      AS user_id
  , usr.usename                                                     AS username
  , min(cur.starttime)                                              AS start_time
  , DATEDIFF(second, min(cur.starttime), getdate())                 AS run_time
  , min(cur.byte_count)                                             AS bytes_in_result_set
  , round(min(cur.byte_count) / pow(1024, 2), 2)                    AS mb_in_result_set
  , round(min(cur.byte_count) / pow(1024, 3), 2)                    AS gb_in_result_set
  , min(cur.row_count)                                              AS row_count
  , min(cur.row_count - cur.fetched_rows)                           AS remaining_rows_to_fetch
  , min(cur.fetched_rows)                                           AS fetched_rows
  , listagg(util_text.text)
    WITHIN GROUP (ORDER BY util_text.starttime, util_text.sequence) AS query
FROM STV_ACTIVE_CURSORS cur
  JOIN STL_UTILITYTEXT util_text
    ON cur.pid = util_text.pid 
        AND cur.xid = util_text.xid 
        AND util_text.text != 'begin;'
  JOIN PG_USER usr
    ON usr.usesysid = cur.userid
GROUP BY cur.userid, cur.xid, cur.pid, usr.usename
