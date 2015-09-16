/* UDF: f_null_syns.sql

Purpose: This function showcases python SET and BOOLEAN support as well as how an argument can be matched against synonyms, 
similar to a SQL IN condition.

Internal dependencies: None

External dependencies: None

2015-09-10: written by chriz@
*/

create or replace function f_null_syns(a varchar)
returns boolean
stable
as $$
    s = {"null", "invalid", "unknown", "n/a", "not applicable", "void", "nothing", "nonexistent", "null and void"}
    b = str(a).lower()
    if b in s:
        return True
    return False
$$ LANGUAGE plpythonu;

/* Example usage:

udf=# create table null_tbl (id int,a varchar);
CREATE TABLE

udf=# insert into null_tbl values (1,null),(2,'null'),(3,'VOID');
INSERT 0 3

udf=# \pset null <NULL>
Null display is "<NULL>".

udf=# select * from null_tbl;
 id |   a    
----+--------
  2 | null
  3 | VOID
  1 | <NULL>
(3 rows)

udf=# UPDATE null_tbl SET a = NULL WHERE f_null_syns(a) = TRUE;
UPDATE 2

udf=# select * from null_tbl;
 id |   a    
----+--------
  2 | <NULL>
  3 | <NULL>
  1 | <NULL>
(3 rows)
*/
