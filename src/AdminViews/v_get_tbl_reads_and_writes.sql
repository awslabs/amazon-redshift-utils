--DROP VIEW admin.v_get_tbl_reads_and_writes;
/**********************************************************************************************
Purpose: View to get the READ and WRITE operations per table.  This view should be used with a
filter that limits the output for query IDs and/or transaction IDs.  This view will help to see 
what tables are operated on by transactions or queries.  The operation will be one of the 
following:
 - R if it is a read operation that was done
 - W if it is a write operation that was done
 - A if the query statement got aborted this could be due to a serializable isolation violation
   the user will need to check the query manually.

History:
2016-11-03 pvbouwel Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_get_tbl_reads_and_writes
AS
WITH v_operations AS 
  ( SELECT query, tbl, 'R' AS operation FROM stl_scan WHERE type=2 GROUP BY query, tbl
      UNION ALL
    SELECT query, tbl, 'W' AS operation FROM stl_delete GROUP BY query, tbl
      UNION ALL
    SELECT query, tbl, 'W' AS operation FROM stl_insert GROUP BY query, tbl
      UNION ALL
    SELECT query, NULL::int as tbl, 'A' AS operation FROM stl_query WHERE aborted=1 GROUP BY query, tbl
  )
SELECT
  sq.xid
  ,sq.query
  ,vo.tbl
  ,vo.operation
  ,sq.starttime
  ,sq.endtime
FROM stl_query sq
     LEFT JOIN v_operations vo ON sq.query=vo.query;
