--DROP VIEW admin.v_generate_external_tbl_ddl;
/**********************************************************************************************
Purpose: View to get the DDL for an external table.

 History:
  2019-07-10 styerp Created
 **********************************************************************************************/

CREATE OR REPLACE VIEW admin.v_generate_external_tbl_ddl AS
    SELECT schemaname
         , tablename
         , seq
         , ddl
        FROM (
             SELECT 'CREATE EXTERNAL TABLE ' + quote_ident(schemaname) + '.' + quote_ident(tablename) + '('
                     + quote_ident(columnname) + ' ' + external_type AS ddl
                  , 0                                                AS seq
                  , schemaname
                  , tablename
                 FROM svv_external_columns
                 WHERE columnnum = 1
             UNION ALL
             SELECT ', ' + quote_ident(columnname) + ' '
                     + decode(external_type, 'double', 'double precision', external_type) AS ddl
                  , columnnum                                                             AS seq
                  , schemaname
                  , tablename
                 FROM svv_external_columns
                 WHERE columnnum > 1
                   AND part_key = 0
             UNION ALL
             SELECT ')'           AS ddl
                  , 100 + max_col AS seq
                  , schemaname
                  , tablename
                 FROM (
                      SELECT schemaname
                           , tablename
                           , max(columnnum) AS max_col
                          FROM svv_external_columns
                          WHERE part_key = 0
                          GROUP BY 1
                                 , 2
                      ) sub
             UNION ALL
             SELECT 'PARTITIONED BY (' + quote_ident(columnname) + ' ' + external_type AS ddl
                  , 100000 + part_key + columnnum                                      AS seq
                  , schemaname
                  , tablename
                 FROM svv_external_columns
                 WHERE part_key = 1

             UNION ALL
             SELECT ',' + quote_ident(columnname) + ' ' + external_type AS ddl
                  , 100000 + part_key + columnnum                       AS seq
                  , schemaname
                  , tablename
                 FROM svv_external_columns
                 WHERE part_key > 1
             UNION ALL
             SELECT ')'                 AS ddl
                  , 999999                                      AS seq
                  , schemaname
                  , tablename
                 FROM svv_external_columns
                 WHERE part_key = 1

             UNION ALL
             SELECT 'ROW FORMAT SERDE ' + quote_literal(serialization_lib)

                  , 1000000 AS seq
                  , schemaname
                  , tablename
                 FROM svv_external_tables
             UNION ALL
             SELECT 'WITH SERDEPROPERTIES ( ' + regexp_replace(
                     regexp_replace(regexp_replace(serde_parameters, '\\{|\\}', ''), '"', '\''), ':', '=') + ')' AS ddl
                  , 1000001                                                                                      AS seq
                  , schemaname
                  , tablename
                 FROM svv_external_tables
                 WHERE serde_parameters IS NOT NULL
             UNION ALL
             SELECT 'STORED AS INPUTFORMAT ' + quote_literal(input_format) + ' OUTPUTFORMAT '
                     + quote_literal(output_format)
                  , 1000001 AS seq
                  , schemaname
                  , tablename
                 FROM svv_external_tables
                 WHERE input_format IS NOT NULL
                   AND output_format IS NOT NULL
             UNION ALL
             SELECT 'LOCATION ' + quote_literal(location)
                  , 1000002 AS seq
                  , schemaname
                  , tablename
                 FROM svv_external_tables

             UNION ALL
             SELECT 'TABLE PROPERTIES (' + quote_literal(
                     regexp_replace(params, $$'EXTERNAL'='TRUE',|'transient_lastDdlTime'='[::digit::]*',$$, NULL))
                     + ')'  AS ddl
                  , 1000004 AS seq
                  , schemaname
                  , tablename
                 FROM (
                      SELECT schemaname
                           , tablename
                           , regexp_replace(regexp_replace(regexp_replace(parameters, '\\{|\\}', ''), '"', '\''), ':',
                                            '=') AS params
                          FROM svv_external_tables
                      ) tbl_params
                 WHERE params IS NOT NULL
             UNION ALL
             SELECT ';'        AS ddl
                  , 9999999999 AS seq
                  , schemaname
                  , tablename
                 FROM svv_external_tables
             ) gen
        WHERE ddl IS NOT NULL
        ORDER BY 1 DESC
               , 2 DESC
               , 3;
