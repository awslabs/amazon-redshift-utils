/* Purpose: This UDF takes a URL as an argument, and parses out the field-value pairs.
Returns pairs in JSON for further parsing if needed.

External dependencies: None
*/

create or replace function f_parse_url_query_string(url varchar(max))
returns varchar(max)
stable
as $$
    from urlparse import urlparse, parse_qsl
    import json
    return json.dumps(dict(parse_qsl(urlparse(url)[4])))
$$ LANGUAGE plpythonu;

/* Example usage:

udf=# create table url_log (id int, url varchar(max));
CREATE TABLE

udf=# insert into url_log values (1,'http://example.com/over/there?name=ferret'),
udf-#     (2,'http://example.com/Sales/DeptData/Elites.aspx?Status=Elite'),
udf-#     (3,'http://example.com/home?status=Currently'),
udf-#     (4,'https://example.com/ops/search?utf8=%E2%9C%93&query=redshift');
INSERT 0 4

udf=# select id,trim(url) as url, f_parse_url_query_string(url) from url_log;
 id |                             url                              |        f_parse_url_query_string         
----+--------------------------------------------------------------+-----------------------------------------
  1 | http://example.com/over/there?name=ferret                    | {"name": "ferret"}
  2 | http://example.com/Sales/DeptData/Elites.aspx?Status=Elite   | {"Status": "Elite"}
  3 | http://example.com/home?status=Currently                     | {"status": "Currently"}
  4 | https://example.com/ops/search?utf8=%E2%9C%93&query=redshift | {"utf8": "\u2713", "query": "redshift"}
(4 rows)

udf=# select id,trim(url) as url FROM url_log WHERE json_extract_path_text(f_parse_url_query_string(url),'query') = 'redshift';
 id |                             url                              
----+--------------------------------------------------------------
  4 | https://example.com/ops/search?utf8=%E2%9C%93&query=redshift
(1 row)

*/
