--DROP VIEW admin.v_get_tbl_reads_and_writes;
/**********************************************************************************************
Purpose: View to get the READ and WRITE operations per table for specific transactions.  
This view should be used with a filter that limits the output for transaction IDs or query IDs.  
This view will help to see what tables are operated on by transactions and to see how transactions
have dependencies between each other.  The operation will be one of the 
following:
 - R if it is a read operation that was done
 - W if it is a write operation that was done
 - A if the query statement got aborted this could be due to a serializable isolation violation
   the user will need to check the query manually.

Another output is the transaction actions (tx_action) which will be:
 - R if the transaction is rolled back
 - C if the transaction is committed

History:
2016-11-03 pvbouwel Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_get_tbl_reads_and_writes
AS
WITH v_operations AS 
  ( SELECT query, tbl, 'R' AS operation FROM stl_scan WHERE type=2 GROUP BY query, tbl
      UNION
    SELECT query, tbl, 'W' AS operation FROM stl_delete GROUP BY query, tbl
      UNION
    SELECT query, tbl, 'W' AS operation FROM stl_insert GROUP BY query, tbl
      UNION
    SELECT sq.query AS query, stc.table_id AS tbl, 'A' AS operation FROM stl_query sq LEFT JOIN stl_tr_conflict stc on sq.xid = stc.xact_id  where aborted=1 GROUP BY query, tbl
  ),
v_end_of_transaction AS
  ( SELECT xid, MAX(tx_action) AS tx_action, MAX(endtime) as endtime FROM
    (  SELECT xid, 'R' AS tx_action, endtime FROM stl_utilitytext WHERE text ILIKE '%rollback%' OR text ILIKE '%aborted%'
         UNION ALL
       SELECT xid, 'C' AS tx_action, endtime FROM stl_utilitytext WHERE text ILIKE '%commit%' OR text ILIKE '%end%'
         UNION ALL
       SELECT xid, 'C' AS tx_action, endtime from stl_commit_stats WHERE node=-1
    ) GROUP BY xid
  )
SELECT
  xid
  ,query
  ,tbl
  ,operation
  ,statement_starttime
  ,statement_endtime
  ,transaction_endtime
  ,transaction_action
FROM (
  SELECT
    sq.xid as xid
    ,sq.query as query
    ,vo.tbl as tbl
    ,vo.operation as operation
    ,sq.starttime as statement_starttime
    ,sq.endtime as statement_endtime
    ,ve.endtime as transaction_endtime
    ,ve.tx_action as transaction_action
  FROM stl_query sq
       LEFT JOIN v_operations vo ON sq.query=vo.query
       LEFT JOIN v_end_of_transaction ve ON sq.xid=ve.xid
  UNION ALL
    SELECT
     xid as xid
     ,null as query
     ,null as tbl
     ,'C' as operation
     ,startqueue as statement_starttime
     ,endtime as statement_endtime
     ,endtime as transaction_endtime
     ,'C' as transaction_action
    FROM stl_commit_stats where node=-1
) order by statement_starttime;
