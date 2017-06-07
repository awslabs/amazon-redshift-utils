--DROP VIEW admin.v_get_compressed_sortkey_columns;
/**********************************************************************************************
Purpose: View to get a list of columns that have the SORTKEY compressed.

         Only the first column of the sort key is checked.
         If there is more than 1 column in the sort it is ok if columns >= 2 are compressed.
                 (AND a.attsortkeyord = 1)

         The reason it is not desirable to have the first column of a SORTKEY compressed
         is because it can cause the following alert event.

                Very selective query filter

         The AdminScript named perf_alert.sql can show queries that have this alert event.

History:
2016-10-21 michaelcunningham Created
**********************************************************************************************/
CREATE OR REPLACE VIEW admin.v_get_compressed_sortkey_columns
AS
SELECT  n.nspname AS schemaname, c.relname AS tablename, a.attname AS colname,
        CASE  WHEN format_encoding((a.attencodingtype)::integer) = 'none' THEN ''
              ELSE 'encode ' + format_encoding((a.attencodingtype)::integer)
              END AS colencoding
FROM    pg_namespace AS n
        INNER JOIN pg_class AS c
            ON n.oid = c.relnamespace
        INNER JOIN pg_attribute AS a
            ON c.oid = a.attrelid
WHERE   c.relkind = 'r'
AND     a.attnum > 0
AND     a.attsortkeyord = 1
AND     a.attencodingtype > 0
ORDER BY n.nspname, c.relname, a.attnum;
