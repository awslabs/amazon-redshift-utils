/**********************************************************************************************
Purpose: Transpose row values into columns 

Notes:  Emulates result of PIVOT aggregate() FOR syntax

Syntax:
        SELECT [ $(select_cols) , ] $(generated PIVOT columns)
          FROM  $(table_name)
         PIVOT ( $(agg_func) ( $(metric_col) ) 
           FOR $(pivot_src) IN ( SELECT DISTINCT $(pivot_src) FROM $(table_name) ) ) pvt
        [ WHERE $(where_clause) ]
        [ GROUP BY $(select_cols) ] ;'

Parameters:
        select_cols : Text list of columns to be added to the SELECT clause
        table_name  : Schema qualified name of table to be queried
        pivot_src   : Name of the column whose value will be pivoted 
        agg_func    : Name of the aggregate function to apply
        metric_col  : Name of the column to be aggregated
        where_clause: Text of predicate criteria to be added to the WHERE clause
        result_set  : Name of cursor used for output

History:
2019-05-25 - joeharris76 - Created
2019-11-04 - joeharris76 - Revised for publication
**********************************************************************************************/

-- DROP PROCEDURE sp_pivot(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,REFCURSOR);
CREATE OR REPLACE PROCEDURE sp_pivot_for (
      select_cols  IN    VARCHAR(512)
    , table_name   IN    VARCHAR(256)
    , pivot_src    IN    VARCHAR(128)
    , agg_func     IN    VARCHAR(32)
    , metric_col   IN    VARCHAR(128)
    , where_clause IN    VARCHAR(512)
    , result_set   INOUT REFCURSOR )
AS $$
DECLARE
  pivot_col  RECORD;
  final_sql  VARCHAR(MAX) := '';
  pivot_sql  VARCHAR(MAX) := '';
  val_cnt    INTEGER  := 0;
  col_cnt    SMALLINT := 0;
BEGIN
    -- Check the number of values being pivoted. Placeholders used for NULL and zero length values.
    pivot_sql := 'SELECT COUNT(DISTINCT CASE WHEN '||quote_ident(pivot_src)||' IS NULL THEN ''<NULL>'''|| 
                                           ' WHEN '||quote_ident(pivot_src)||' = '''' THEN ''<EMPTY>'''|| 
                                           ' ELSE '||quote_ident(pivot_src)||' END ) AS val_cnt '||
           'FROM '||table_name||' ;';
    EXECUTE pivot_sql INTO val_cnt;
    IF val_cnt > 256 THEN -- Limit columns to prevent excessive width
        RAISE EXCEPTION 'Too many values to pivot. Found % but limit is 256.', val_cnt;
    END IF; 
    IF select_cols <> '' THEN
        final_sql := 'SELECT '||select_cols||', ';
    ELSE
        final_sql := 'SELECT ';
    END IF;
    -- Query to get the row values being pivoted
    pivot_sql := 'SELECT DISTINCT CASE WHEN '||quote_ident(pivot_src)||' IS NULL THEN ''<NULL>'''||
                                     ' WHEN '||quote_ident(pivot_src)||' = '''' THEN ''<EMPTY>'''||
                                     ' ELSE '||quote_ident(pivot_src)||' END AS col_n '||
                 'FROM '||table_name||CASE WHEN where_clause <> '' THEN ' WHERE '||where_clause ELSE '' END||' ORDER BY 1;';
    -- Iterate over the row values
    FOR pivot_col IN EXECUTE pivot_sql
    LOOP
       col_cnt := col_cnt + 1;
       IF col_cnt > 1 THEN
         final_sql := final_sql||', ';
       END IF;
       -- Add the CASE statement for each column to the SQL
       IF pivot_col.col_n = '<NULL>' THEN
           final_sql := final_sql||agg_func||'(CASE WHEN '||quote_ident(pivot_src)||' IS NULL THEN '||quote_ident(metric_col)
                                           ||' ELSE NULL END) AS '||quote_ident(pivot_col.col_n);
       ELSIF pivot_col.col_n = '<EMPTY>' THEN
           final_sql := final_sql||agg_func||'(CASE WHEN '||quote_ident(pivot_src)||' = '''' THEN '||quote_ident(metric_col)
                                           ||' ELSE NULL END) AS '||quote_ident(pivot_col.col_n);
       ELSE
           final_sql := final_sql||agg_func||'(CASE WHEN '||quote_ident(pivot_src)||' = '||quote_literal(pivot_col.col_n)
                                           ||' THEN '||quote_ident(metric_col)||' ELSE NULL END) AS '||quote_ident(pivot_col.col_n);
       END IF;
    END LOOP;
    final_sql := final_sql||' FROM '||table_name;
    IF where_clause <> '' THEN
        final_sql := final_sql||' WHERE '||where_clause||' ';
    END IF;
    IF select_cols <> '' THEN
        final_sql := final_sql||' GROUP BY '||select_cols||' ;';
    ELSE
        final_sql := final_sql||' ;';
    END IF;
    -- Open the cursor and execute the SQL
    OPEN result_set FOR EXECUTE final_sql;
END;
$$ LANGUAGE plpgsql;


/* Usage Example:

    BEGIN;

    DROP TABLE IF EXISTS tmp_state_metrics;
    CREATE TEMP TABLE tmp_state_metrics ( country VARCHAR, state VARCHAR, entries INTEGER);
    INSERT INTO tmp_state_metrics VALUES
          ('USA', NULL, 15177), ('USA', NULL, 1)
        , ('USA', 'AK',  7001), ('USA', 'AK', 1)
        , ('USA', 'AL', 20225), ('USA', 'AL', 1)
        , ('USA', 'AR', 22772), ('USA', 'AR', 1)
        , ('USA', 'AZ',  4577), ('USA', 'AZ', 1)
        , ('USA', 'CA', 17877), ('USA', 'CA', 1)
        , ('USA', 'CO', 19503), ('USA', 'CO', 1);

    CALL sp_pivot_for ('country','tmp_state_metrics','state','SUM','entries','','pivot_result');
    FETCH ALL FROM pivot_result; CLOSE pivot_result;
    --  country | <null> |  ak  |  al   |  ar   |  az  |  ca   |  co
    --  USA     |  15178 | 7002 | 20226 | 22773 | 4578 | 17878 | 19504

    CALL sp_pivot_for ('country','tmp_state_metrics','state','SUM','entries','state IS NOT NULL','pivot_result');
    FETCH ALL FROM pivot_result; 
    -- country |  ak  |  al   |  ar   |  az  |  ca   |  co
    -- USA     | 7002 | 20226 | 22773 | 4578 | 17878 | 19504

    END;
*/
