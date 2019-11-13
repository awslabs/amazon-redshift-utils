/**********************************************************************************************
Purpose: Calculate levels in a nested hierarchy tree

Notes:  Emulates result of CONNECT BY PRIOR … START WITH syntax

Syntax:
            SELECT $(select_cols)
                 , LEVEL
                 , $(child_col) , $(parent_col) 
            FROM  $(table_name)
            CONNECT BY PRIOR $(child_col) = $(parent_col) 
            START WITH $(start_with)
            WHERE LEVEL <= $(max_level)
              AND $(where_clause) ;
Parameters:
        select_cols : Text list of columns to be added to the SELECT clause
        table_name  : Schema qualified name of table to be queried
        child_col   : Child key column, e.g., employee_id
        parent_col  : Parent key column, e.g., ,manager_id
        start_with  : Criteria for the start of the hierarchy 
        where_clause: Text of predicate criteria to be added to the WHERE clause
        max_level   : Maximum hierarchy depth to be returned
        temp_tbl_n  : Name of temp table to be used for output

History:
2019-10-25 - joeharris76 - Created
**********************************************************************************************/

-- DROP PROCEDURE sp_connect_by_prior(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,INTEGER,VARCHAR);
CREATE OR REPLACE PROCEDURE sp_connect_by_prior (
      select_cols  IN VARCHAR(256)
    , table_name   IN VARCHAR(128)
    , child_col    IN VARCHAR(128)
    , parent_col   IN VARCHAR(128)
    , start_with   IN VARCHAR(128)
    , where_clause IN VARCHAR(256)
    , max_level    IN INTEGER
    , temp_tbl_n   IN VARCHAR(128)
) AS $$
DECLARE
  rows  INTEGER;
  level INTEGER := 1;
  sql   VARCHAR(MAX) := '';
BEGIN
    --Check that required parameters are not empty
    IF table_name='' OR parent_col='' OR child_col='' THEN
         RAISE EXCEPTION 'Parameters cannot be empty: schema_n, table_n, parent_col, child_col';
    END IF;
    --Retrieve the starting point of the hierarchy
    sql := ' SELECT '||select_cols||', '||level||' AS level'||
                 ', '||quote_ident(child_col)||', '||quote_ident(parent_col)||
           '  FROM  '||table_name;
    --Start with parent_col IS NULL if start_with is empty
    IF start_with = '' THEN
       sql := sql||' WHERE '||quote_ident(parent_col)||' IS NULL ';
    ELSE
       sql := sql||' WHERE '||start_with;
    END IF;
    IF where_clause <> '' THEN
       sql := sql||' AND '||where_clause;
    END IF;
    --Create temp table to hold results
    -- /*DEBUG:*/ RAISE INFO 'SQL: %', sql;
    EXECUTE 'DROP TABLE IF EXISTS '||temp_tbl_n||';';
    EXECUTE 'CREATE TEMPORARY TABLE '||temp_tbl_n||' AS '||sql||';';
    --Print row count for the first level
    EXECUTE 'SELECT COUNT(*) FROM '||temp_tbl_n||' ;' INTO rows ;
    RAISE INFO 'Level %: Rows found = %', level,rows;
    -- Loop over the hierarchy until max_level is reached
    <<get_children>>
    WHILE level < max_level LOOP
        level := (level + 1);
        --Join the temp table to the source to find next level
        sql := 'INSERT INTO '||quote_ident(temp_tbl_n)||
               ' SELECT '||select_cols||', '||level||' AS level'||
                     ', '||quote_ident(child_col)||', '||quote_ident(parent_col)||
                ' FROM '||table_name||
                ' WHERE '||quote_ident(parent_col)||
                '  IN (SELECT '||quote_ident(child_col)||
                       ' FROM '||quote_ident(temp_tbl_n)||
                      ' WHERE level = '||(level-1)||')';
        IF where_clause <> '' THEN
           sql := sql||' AND '||where_clause;
        END IF;
        -- /*DEBUG:*/ RAISE INFO 'SQL: %', sql;
        EXECUTE sql||';';
        --Print row count for the first level
        GET DIAGNOSTICS rows := ROW_COUNT;
        RAISE INFO 'Level %: Rows found = %', level, rows;
        IF rows = 0 THEN
            EXIT get_children;
        END IF;
    END LOOP;
END
$$ LANGUAGE plpgsql;

/* Usage Example:

    CREATE TEMP TABLE tmp_employee ( emp_title VARCHAR, emp_id INT, mgr_id INT, dept VARCHAR);
    INSERT INTO tmp_employee VALUES
          ('Chairman'       , 100, NULL, 'Board'    )
        , ('CEO'            , 101, 100 , 'Board'    )
        , ('CTO'            , 102, 101 , 'IT'       )
        , ('CMO'            , 103, 101 , 'Sales/Mkt')
        , ('VP Analytics'   , 104, 102 , 'IT'       )
        , ('VP Engineering' , 105, 102 , 'IT'       )
        , ('Sales Director' , 106, 103 , 'Sales/Mkt')
        , ('Sales Mgr West' , 107, 106 , 'Sales/Mkt')
        , ('Sales Mgr East' , 108, 106 , 'Sales/Mkt')
        , ('Sales Mgr South', 109, 106 , 'Sales/Mkt');
    CALL sp_connect_by_prior(
            'dept,emp_title','tmp_employee','emp_id','mgr_id','mgr_id IS NULL','',3,'tmp_result'
        );
    SELECT * FROM connect_by_result ORDER BY level, mgr_id, emp_id;
    --    dept    | emp_title | level | emp_id | mgr_id
    --  Board     | Chairman  |     1 |    100 |
    --  Board     | CEO       |     2 |    101 |    100
    --  IT        | CTO       |     3 |    102 |    101
    --  Sales/Mkt | CMO       |     3 |    103 |    101
    CALL sp_connect_by_prior(
            'dept,emp_title','tmp_employee','emp_id','mgr_id','emp_id = 101','dept=\'Sales/Mkt\'',3,'tmp_result'
        );
    SELECT * FROM connect_by_result ORDER BY level, mgr_id, emp_id;
    --   dept    |    emp_title    | level | emp_id | mgr_id
    -- Sales/Mkt | CMO             |     1 |    103 |    101
    -- Sales/Mkt | Sales Director  |     2 |    106 |    103
    -- Sales/Mkt | Sales Mgr West  |     3 |    107 |    106
    -- Sales/Mkt | Sales Mgr East  |     3 |    108 |    106
    -- Sales/Mkt | Sales Mgr South |     3 |    109 |    106
*/

