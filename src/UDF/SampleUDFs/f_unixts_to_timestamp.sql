/* Purpose: Converts a UNIX timestamp to a UTC datetime with up to microseconds granularity. 

Internal dependencies: Pandas

External dependencies: None

*/

create or replace function f_unixts_to_timestamp(ts bigint, units char(2))
returns timestamp
stable
as $$
    import pandas
    if units == 'ss':
        return pandas.to_datetime(ts, unit='s')
    elif units == 'ms':
        return pandas.to_datetime(ts, unit='ms')
    elif units == 'us':
        return pandas.to_datetime(ts, unit='us')
$$ LANGUAGE plpythonu;

/* Example usage:

udf=# select f_unixts_to_timestamp(1349720105,'ss');
 f_unixts_to_timestamp 
-----------------------
 2012-10-08 18:15:05
(1 row)

udf=# select f_unixts_to_timestamp(1349720105123,'ms');
  f_unixts_to_timestamp  
-------------------------
 2012-10-08 18:15:05.123
(1 row)

udf=# select f_unixts_to_timestamp(1349720105123123,'us');
   f_unixts_to_timestamp    
----------------------------
 2012-10-08 18:15:05.123123
(1 row)

*/

