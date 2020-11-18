/**********************************************************************************************
Purpose: Provide controlled access to data without granting permission on the table/view
Notes:  
        Must set the SECURITY attribute set as DEFINER
        An unprivileged user can run the procedure when granted the EXECUTE permission

Parameters:
        select_cols : Text list of columns to be added to the SELECT clause
        query_from  : Schema qualified name of table/view to be queried
        where_clause: Text of predicate criteria to be added to the WHERE clause
        max_level   : Maximum number of rows to be returned
        result_set  : Name of cursor used for output

Requirements:
        Must have a table containing user authorizations with the following DDL:
            CREATE TABLE $(auth_table)
                ( query_from   VARCHAR,
                , user_name    VARCHAR,
                , valid_until  TIMESTAMP,
                , max_rows     INT );

History:
2019-11-09 - joeharris76 - Created
**********************************************************************************************/

-- DROP PROCEDURE sp_controlled_access(VARCHAR,VARCHAR,VARCHAR,INTEGER,REFCURSOR);
CREATE OR REPLACE PROCEDURE sp_controlled_access (
      select_cols  IN VARCHAR(256)
    , query_from   IN VARCHAR(128)
    , where_clause IN VARCHAR(256)
    , max_rows     IN INTEGER
    , result_set   INOUT REFCURSOR )
AS $$
DECLARE
    user_name  VARCHAR(128);
    sql        VARCHAR(MAX) := '';
    auth_table VARCHAR(256) := 'hr.access_authority';
    rows_limit INTEGER := 0;
BEGIN          
    user_name = session_user;
    -- Check the user is authorized for this query
    sql := 'SELECT NVL(max_rows,1000) rows_limit FROM '||auth_table||
          ' WHERE query_from = '''||query_from||''' '||
          ' AND user_name = '''||user_name||''' '||
          ' AND (valid_until IS NULL OR valid_until >= '''||SYSDATE||''') ;';
    EXECUTE sql INTO rows_limit;
    IF NVL(rows_limit,0) = 0 THEN
        RAISE EXCEPTION 'ERROR: Query Is Not Authorized';
    ELSE
        -- Compose the user query
        IF select_cols <> '' THEN
            sql := 'SELECT '||select_cols;
        ELSE
            sql := 'SELECT * ';
        END IF;
        sql := sql||' FROM '||query_from;
        IF where_clause <> '' THEN
            sql := sql||' WHERE '||where_clause||' ';
        END IF;
        IF rows_limit > max_rows THEN
            rows_limit := max_rows;
        END IF;
        sql := sql||' LIMIT '||rows_limit||' ;';
        -- Open the cursor and execute the SQL
        RAISE INFO 'SQL: %', sql;
        OPEN result_set FOR EXECUTE sql;
        RAISE INFO 'AUTHORIZED: Query on `%` completed',query_from;
    END IF;
END;
$$ LANGUAGE plpgsql
SECURITY DEFINER;


/* Usage Example:

    SELECT current_user;
    CREATE SCHEMA hr;
    CREATE TABLE hr.employee ( title VARCHAR, emp INT, mgr INT, dept VARCHAR);
    INSERT INTO hr.employee VALUES
          ('Chairman', 100, NULL, 'Board'    )
        , ('CEO'     , 101, 100 , 'Board'    )
        , ('CTO'     , 102, 101 , 'IT'       )
        , ('CMO'     , 103, 101 , 'Sales/Mkt');
    CREATE USER user_no_priv WITH PASSWORD DISABLE;
    CREATE TABLE hr.access_authority ( query_from VARCHAR, user_name VARCHAR, valid_until TIMESTAMP, max_rows INT ); 
    INSERT INTO hr.access_authority VALUES ('hr.employee','user_no_priv','2019-12-31',99);
    GRANT ALL ON SCHEMA public TO user_no_priv;
    GRANT EXECUTE ON PROCEDURE sp_controlled_access(VARCHAR,VARCHAR,VARCHAR,INTEGER,REFCURSOR) TO user_no_priv;

    -- Change session to the new user
    SET SESSION AUTHORIZATION user_no_priv;
    SELECT current_user; -- user_no_priv
    SELECT * FROM hr.employee; -- ERROR: permission denied
    BEGIN; -- Call the stored procedure 
    CALL sp_controlled_access (
          'title,emp,mgr,dept'::VARCHAR -- select_cols
        , 'hr.employee'::VARCHAR        -- query_from
        , 'dept = ''Board'''::VARCHAR   -- where_clause
        , 10                            -- max_rows
        , 'employee_data' );            -- result_set
    -- INFO:  AUTHORIZED: Query on `hr.employee` completed
    --   result_set
    -- ---------------
    --  employee_data
    FETCH ALL FROM employee_data; 
    --   title   | emp | mgr | dept
    -- ----------+-----+-----+-------
    --  Chairman | 100 |     | Board
    --  CEO      | 101 | 100 | Board
    END;

    -- RESET SESSION AUTHORIZATION;
    -- DROP TABLE hr.employee;
    -- DROP TABLE hr.access_authority;
    -- DROP SCHEMA hr;
    -- REVOKE EXECUTE ON PROCEDURE sp_controlled_access(VARCHAR,VARCHAR,VARCHAR,INTEGER,REFCURSOR) FROM user_no_priv;
    -- REVOKE ALL ON SCHEMA public FROM user_no_priv;
    -- DROP USER user_no_priv;

*/
