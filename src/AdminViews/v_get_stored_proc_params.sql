/**********************************************************************************************
Purpose: List all stored procedures with their input parameters
History:
2020-04-15 joeharris76 Created
2023-02-20 saeedma8 added serverless prolang id
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_get_stored_proc_params
AS
WITH arguments 
AS (SELECT oid, arg_num
         , arg_names[arg_num]     AS arg_name
         , arg_types[arg_num - 1] AS arg_type
    FROM (SELECT GENERATE_SERIES(1, arg_count) AS arg_num
               , arg_names, arg_types, oid
          FROM (SELECT oid
                     , proargnames AS arg_names
                     , proargtypes AS arg_types
                     , pronargs    AS arg_count
                FROM pg_proc
                WHERE proowner != 1
                  AND prolang in (100356, 101857) ) t) t)
SELECT n.nspname                     AS schema_name
     , p.proname                     AS proc_name
     , p.oid::INT                    AS proc_id
     , a.arg_num                     AS order
     , NVL(a.arg_name, '')           AS parameter
     , FORMAT_TYPE(a.arg_type, NULL) AS data_type 
FROM pg_proc                p
     LEFT JOIN pg_namespace n   ON n.oid = p.pronamespace
     LEFT JOIN arguments    a   ON a.oid = p.oid
WHERE p.proowner != 1
  AND p.prolang in (100356, 101857)
ORDER BY 1,2,3,4;
