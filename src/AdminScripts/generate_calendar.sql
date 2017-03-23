
/**********************************************************************************************
Purpose: Uses a python UDF and a CTAS statement to build a calendar dimension table template.
Columns:
id: Unique identifier of date represented as integer (YEAR*10000 + MONTH*100 + DAY)
date: Date represented as date data type
year: Year represented as integer without 0-padding (1900-2049)
month: Month represented as integer without 0-padding (1-12)
day: Day represeted as integer without 0-padding (1-31)
quarter: Quarter represented as integer (1-4)
week: Week represented as integer (1-53)
day_name: Day represented in plain english
month_name: Month represented in plain english
holiday_flag: Boolean for US Federal Holidays
weekend_flag: Boolean for Saturday and Sunday

Notes:
* Feel free to add additional columns to meet your reporting requirements, open issues for help
* Use a custom calendar if interested in holidays rather than US Federal Holidays
* Modify date literals and LIMIT clause if interested in ranges of dates beyond 1900->2049
* Requires 54787 rows in STL_SCAN to generate calendar from 2049-12-31 to 1900-01-01
** Use a different table with that many rows in the date_gen subquery if STL_SCAN is too small

History:
2017-01-19 zach-data (chriz-bigdata) created
**********************************************************************************************/

CREATE OR REPLACE FUNCTION f_holiday(dt DATE)
RETURNS bool
STABLE
AS $$
    import pandas as pd
    from pandas.tseries.holiday import USFederalHolidayCalendar as calendar
    holidays = calendar().holidays(start='1900-01-01', end='2049-12-31')
    return dt in holidays
$$ LANGUAGE plpythonu;

CREATE TABLE dim_calendar DISTSTYLE ALL SORTKEY (id) AS
SELECT
 (DATE_PART('y', date_gen.dt)*10000+DATE_PART('mon', date_gen.dt)*100+DATE_PART('day', date_gen.dt))::int AS "id",
 date_gen.dt AS "date",
 DATE_PART('y', date_gen.dt)::smallint AS "year",
 DATE_PART('mon', date_gen.dt)::smallint AS "month",
 DATE_PART('day', date_gen.dt)::smallint AS "day",
 DATE_PART('qtr', date_gen.dt)::smallint AS "quarter",
 DATE_PART('w', date_gen.dt)::smallint AS "week",
 CASE DATE_PART('dow', date_gen.dt)
  WHEN 0 THEN 'Sunday'
  WHEN 1 THEN 'Monday'
  WHEN 2 THEN 'Tuesday'
  WHEN 3 THEN 'Wednesday'
  WHEN 4 THEN 'Thursday'
  WHEN 5 THEN 'Friday'
  WHEN 6 THEN 'Saturday'
 END::VARCHAR(9) AS "day_name",
 CASE DATE_PART('mon', date_gen.dt)::smallint
  WHEN 1 THEN 'January'
  WHEN 2 THEN 'February'
  WHEN 3 THEN 'March'
  WHEN 4 THEN 'April'
  WHEN 5 THEN 'May'
  WHEN 6 THEN 'June'
  WHEN 7 THEN 'July'
  WHEN 8 THEN 'August'
  WHEN 9 THEN 'September'
  WHEN 10 THEN 'October'
  WHEN 11 THEN 'November'
  WHEN 12 THEN 'December'
 END::VARCHAR(9) AS "month_name",
 f_holiday(date_gen.dt)::boolean AS "holiday_flag",
 CASE  
  WHEN DATE_PART('dow', date_gen.dt)::smallint IN (0,6) THEN TRUE
  ELSE FALSE
 END::boolean AS "weekend_flag"
FROM
(SELECT 
    ('2050-01-01' - n)::date AS dt FROM (SELECT row_number() over () AS n FROM stl_scan LIMIT 54787)) date_gen;
