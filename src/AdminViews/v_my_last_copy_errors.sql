/**********************************************************************************************
Purpose: View to help you see the issues with the last copy run in the session

Notes: 
                
History:
2018-01-18 meyersi Created View Script

**********************************************************************************************/

create view admin.v_my_last_copy_errors as 
select d.query, 
       substring(d.filename,14,20), 
       d.line_number as line, 
       substring(d.value,1,16) as value,
       substring(le.err_reason,1,48) as err_reason,
       le.colname,
       le.col_length,
       le.raw_field_value
from stl_loaderror_detail d, 
     stl_load_errors le
where d.query = le.query
and d.query = pg_last_copy_id();