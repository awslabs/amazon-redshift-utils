/**********************************************************************************************
Purpose: Export/Unload multiple tables, schemas or all tables to S3 with Partitions.
Notes:   You need to install this on all databases to work with seamlessly.
Detailed Blog about this process: https://thedataguy.in/redshift-unload-multiple-tables-schema-to-s3/

Parameters:
    s3_location:     location to store the exported data in s3(also add prefix)
    schema_list:      the list of schema that should be exported
    table_list:  the list table names that should be exported

Usage:
1. Export all the tables in the schema sc1: call unload_pro('s3://bucket/prefix/','sc1',NULL);
2. Export all the tables in the schema sc3,public: call unload_pro('s3://bucket/prefix/','sc3,public',NULL);
3. Export the tables tbl1, tbl2 in the schema sc1: call unload_pro('s3://bucket/prefix/','sc1','tbl1,tbl2');
4. Export the tbl4, tbl5 without specifying any schema name: call unload_pro('s3://bucket/prefix/',NULL,'tbl4,tbl5');
5. Export all the tables in all the schemas: call unload_pro('s3://bucket/prefix/',NULL,NULL);

History:
2019-11-22 BhuviTheDataGuy Created this procedure

Dependancy: 
Create this table to maintain the unload history.

CREATE TABLE unload_history
(
pid          INT IDENTITY(1, 1),
u_id         INT,
u_timestamp  DATETIME,
start_time   DATETIME,
end_time     DATETIME,
db_name      VARCHAR(100),
table_schema VARCHAR(100),
table_name   VARCHAR(100),
export_query VARCHAR(65000),
import_query VARCHAR(65000)
);


Change these values before executing it:

1. IAM ROLE is used here to export. Its hardcoded in the procedure. 
Replace this iamrole:='arn:aws:iam::123123123123:role/myredshiftrole' IAM role in iamrole variable.

2. The default behaviour of this procedure will export the data in CSV format. Also its uses Parallel, GZIP compression, Max file size and etc.
You can change them in unload_query variable.

**********************************************************************************************/
CREATE OR replace PROCEDURE sp_unload(s3_location VARCHAR(10000), 
                                                   schema_list VARCHAR(100), 
                                                   table_list  VARCHAR(1000)) 
    LANGUAGE plpgsql 
    AS 
      $$ 
      DECLARE 
        list RECORD; 
        db          VARCHAR(100); 
        tablename   VARCHAR(100); 
        tableschema VARCHAR(100); 
        starttime   datetime; 
        endtime     datetime; 
        SQL text; 
        s3_path      VARCHAR(1000); 
        iamrole      VARCHAR(100); 
        delimiter    VARCHAR(10); 
        max_filesize VARCHAR(100); 
        un_year      INT; 
        un_month     INT; 
        un_day       INT; 
        unload_query VARCHAR(65000); 
        copy_query   VARCHAR(65000); 
        unload_id    INT; 
        unload_time timestamp; 
        sc_name  VARCHAR(100); 
        tbl_list VARCHAR(1000); 
      BEGIN 
        -- Pass values for the variables 
        SELECT extract(year FROM getdate()) 
        INTO   un_year; 
         
        SELECT extract(month FROM getdate()) 
        INTO   un_month; 
         
        SELECT extract(day FROM getdate()) 
        INTO   un_day; 
         
        SELECT DISTINCT(table_catalog) 
        FROM            information_schema.TABLES 
        INTO            db; 
         
        SELECT coalesce(max(u_id), 0)+1 
        FROM   unload_history 
        INTO   unload_id; 
         
        SELECT getdate() 
        INTO   unload_time; 
         
        s3_path:=s3_location; 
        
        -- IAM ROLE and the Delimiter is hardcoded here 
        iamrole:='arn:aws:iam::123123123123:role/myredshiftrole'; 
        
        delimiter:='|'; 
        
        IF schema_list IS NULL THEN 
          DROP TABLE IF EXISTS sp_tmp_schemalist; 
           
          CREATE temp TABLE sp_tmp_schemalist (sc_list VARCHAR(100)); 
          INSERT INTO sp_tmp_schemalist 
          SELECT   nspname 
          FROM     pg_class pc 
          join     pg_namespace pn 
          ON       pc.relnamespace = pn.oid 
          WHERE    nspname NOT IN ( 'pg_catalog', 
                                   'information_schema', 
                                   'pg_toast') 
          AND      nspname NOT LIKE 'pg_%' 
          GROUP BY nspname; 
           
          SELECT   listagg(sc_list, ',') within GROUP (ORDER BY sc_list) 
          FROM     sp_tmp_schemalist 
          INTO     sc_name; 
         
        ELSE 
          sc_name:=schema_list; 
        END IF; 
        
        IF table_list IS NULL THEN 
          DROP TABLE IF EXISTS sp_tmp_tablelist; 
           
          CREATE temp TABLE sp_tmp_tablelist (tbl_name VARCHAR(100)); 
          INSERT INTO sp_tmp_tablelist 
          SELECT relname 
          FROM   pg_class pc 
          join   pg_namespace pn 
          ON     pc.relnamespace = pn.oid 
          WHERE  nspname NOT IN ( 'pg_catalog', 
                                 'information_schema', 
                                 'pg_toast' ) 
          AND    relname NOT LIKE 'sp_tmp_%'; 
           
          SELECT   listagg(tbl_name, ',') within GROUP (ORDER BY tbl_name) 
          FROM     sp_tmp_tablelist 
          INTO     tbl_list; 
         
        ELSE 
          tbl_list:=table_list; 
        END IF; 
        
        DROP TABLE IF EXISTS sp_tmp_quote_schema; 
        DROP TABLE IF EXISTS sp_tmp_quote_table; 
        DROP TABLE IF EXISTS sp_tmp_token_schema; 
        DROP TABLE IF EXISTS sp_tmp_token_table; 
         
        CREATE TABLE sp_tmp_quote_schema 
                     ( 
                                  comma_quote_schema VARCHAR(400) 
                     ); 
         
        CREATE TABLE sp_tmp_quote_table 
                     ( 
                                  comma_quote_table VARCHAR(400) 
                     ); 
         
        
        EXECUTE 'INSERT INTO sp_tmp_quote_schema VALUES ('|| quote_literal(sc_name)|| ')'; 
        EXECUTE 'INSERT INTO sp_tmp_quote_table VALUES ('|| quote_literal(tbl_list)|| ')'; 
        
        DROP TABLE IF EXISTS sp_tmp_ns; 
        CREATE temp TABLE sp_tmp_ns (n INT); 
        
        INSERT INTO sp_tmp_ns 
        SELECT 
          ROW_NUMBER() OVER () as n
        FROM 
             (SELECT 0 as n UNION SELECT 1) p0,
             (SELECT 0 as n UNION SELECT 1) p1,
             (SELECT 0 as n UNION SELECT 1) p2,
             (SELECT 0 as n UNION SELECT 1) p3,
             (SELECT 0 as n UNION SELECT 1) p4,
             (SELECT 0 as n UNION SELECT 1) p5,
             (SELECT 0 as n UNION SELECT 1) p6,
             (SELECT 0 as n UNION SELECT 1) p7,
             (SELECT 0 as n UNION SELECT 1) p8,
             (SELECT 0 as n UNION SELECT 1) p9,
             (SELECT 0 as n UNION SELECT 1) p10;
         
        SELECT     trim(split_part(b.comma_quote_schema, ',', ns.n)) AS sname 
        INTO       TABLE sp_tmp_token_schema 
        FROM       sp_tmp_ns ns 
        inner join sp_tmp_quote_schema b 
        ON         ns.n <= regexp_count(b.comma_quote_schema, ',') + 1; 
         
        SELECT     trim(split_part(b.comma_quote_table, ',', ns.n)) AS tname 
        INTO       TABLE sp_tmp_token_table 
        FROM       sp_tmp_ns ns 
        inner join sp_tmp_quote_table b 
        ON         ns.n <= regexp_count(b.comma_quote_table, ',') + 1; 
         
        FOR list IN 
        SELECT nspname :: text AS table_schema, 
               relname :: text AS table_name 
        FROM   pg_class pc 
        join   pg_namespace pn 
        ON     pc.relnamespace = pn.oid 
        WHERE  trim(relname :: text) IN 
               ( 
                      SELECT tname 
                      FROM   sp_tmp_token_table) 
        AND    relname !='unload_history' 
        AND    trim(nspname :: text) IN 
               ( 
                      SELECT sname 
                      FROM   sp_tmp_token_schema) LOOP 
        SELECT getdate() 
        INTO   starttime; 
         
        SQL:='select * from ' 
        ||list.table_schema 
        ||'.' 
        ||list.table_name 
        ||'' ; 
        RAISE info '[%] Unloading... schema = % and table = %',starttime, list.table_schema, list.table_name;
        -- Start unloading the data 
        unload_query := 'unload (''' 
        ||SQL 
        ||''') to ''' 
        ||s3_path 
        ||un_year 
        ||'/' 
        ||un_month 
        ||'/' 
        ||un_day 
        ||'/' 
        ||db 
        ||'/' 
        ||list.table_schema 
        ||'/' 
        ||list.table_name 
        ||'/' 
        ||list.table_schema 
        ||'-' 
        ||list.table_name 
        ||'_'' iam_role ''' 
        ||iamrole 
        ||''' delimiter ''' 
        ||delimiter 
        ||''' MAXFILESIZE 300 MB PARALLEL ADDQUOTES HEADER GZIP'; 
        
        EXECUTE unload_query; 
        
        copy_query := 'copy ' 
        ||list.table_schema 
        ||'.' 
        ||list.table_name 
        ||' from ''' 
        ||s3_path 
        ||un_year 
        ||'/' 
        ||un_month 
        ||'/' 
        ||un_day 
        ||'/' 
        ||db 
        ||'/' 
        ||list.table_schema 
        ||'/' 
        ||list.table_name 
        ||'/'' iam_role ''' 
        ||iamrole 
        ||''' delimiter ''' 
        ||delimiter 
        ||''' IGNOREHEADER 1 REMOVEQUOTES gzip'; 
        
        SELECT getdate() 
        INTO   endtime; 
         
        SELECT list.table_schema 
        INTO   tableschema; 
         
        SELECT list.table_name 
        INTO   tablename; 
         
        -- Insert into the history table 
        INSERT INTO unload_history 
                    ( 
                                u_id, 
                                u_timestamp, 
                                start_time, 
                                end_time, 
                                db_name, 
                                table_schema, 
                                table_name, 
                                export_query, 
                                import_query 
                    ) 
                    VALUES 
                    ( 
                                unload_id, 
                                unload_time, 
                                starttime, 
                                endtime, 
                                db, 
                                tableschema, 
                                tablename, 
                                unload_query, 
                                copy_query 
                    ); 
       
      END LOOP; 
      RAISE info ' Unloading of the DB [%] is success !!!' ,db; 
    END; 
    $$;