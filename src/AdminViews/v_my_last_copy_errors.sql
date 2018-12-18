/**********************************************************************************************
Purpose: View to help you see the issues with the last copy run in the session

Notes: 
                
History:
2018-01-18 meyersi Created View Script

**********************************************************************************************/

create or replace view admin.v_my_last_copy_errors as 
select query, 
       starttime,
       filename, 
       line_number,
       err_reason,
       colname,
       type column_type,
       col_length,
       raw_field_value
from stl_load_errors le
where le.query = pg_last_copy_id();
