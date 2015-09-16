/* UDF: f_next_business_day.sql

Purpose: Returns the next business day with respect to US Federal Holidays and a M-F work week.

Internal dependencies: pandas

External dependencies: None

*/

create or replace function f_next_business_day(dt date)
returns date
stable
as $$
    import pandas
    from pandas.tseries.offsets import CustomBusinessDay
    from pandas.tseries.holiday import USFederalHolidayCalendar

    bday_us = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    return dt + bday_us
$$ LANGUAGE plpythonu;

/* Example usage:

udf=# select f_next_business_day('2015-09-04');
 f_next_business_day 
---------------------
 2015-09-08
(1 row)

udf=# select f_next_business_day('2015-09-05');
 f_next_business_day 
---------------------
 2015-09-08
(1 row)

udf=# select f_next_business_day('2015-09-08');
 f_next_business_day 
---------------------
 2015-09-09
(1 row)

*/
