--DROP VIEW admin.v_get_tbl_reads_and_writes;
/**********************************************************************************************
Purpose: View to get the READ and WRITE operations per table.  This view should be used with a
filter that limits the output for query IDs and transaction IDs.  This view will help to see 
what tables are operated on by transactions or queries.
History:
2016-11-03 pvbouwel Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_get_tbl_reads_and_writes
AS
WITH v_operations AS 
  ( SELECT query, tbl, min(starttime) AS starttime, max(endtime) AS endtime,'R' AS operation FROM stl_scan GROUP BY query, tbl
      UNION ALL
    SELECT query,tbl,min(starttime) AS starttime,max(endtime) AS endtime,'w' AS operation FROM stl_delete GROUP BY query,tbl
      UNION ALL
    SELECT query,tbl,min(starttime) AS starttime,max(endtime) AS endtime,'W' AS operation FROM stl_insert GROUP BY query,tbl
  )
SELECT
  sq.xid
  ,sq.query
  ,vo.tbl
  ,vo.operation
  ,vo.starttime
  ,vo.endtime
FROM stl_query sq
     LEFT JOIN v_operations vo ON sq.query=vo.query;
