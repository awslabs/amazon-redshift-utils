/* UDF: f_parse_xml.sql

Purpose: This function showcases how parsing XML is possible with UDFs.

Internal dependencies: None

External dependencies: None

*/

create or replace function f_parse_xml(xml varchar(max))
returns varchar(max)
stable
as $$
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml)
    for country in root.findall('country'):
        rank = country.find('rank').text
        name = country.get('name')
        return name + ':' + rank
$$ LANGUAGE plpythonu;

/* Example usage:

udf=# create table xml_log (id int, xml varchar(max));
CREATE TABLE

udf=# insert into xml_log values (1,'<data>
udf'#     <country name="Liechtenstein">
udf'#         <rank>1</rank>
udf'#         <year>2008</year>
udf'#         <gdppc>141100</gdppc>
udf'#         <neighbor name="Austria" direction="E"/>
udf'#         <neighbor name="Switzerland" direction="W"/>
udf'#     </country></data>');
INSERT 0 1

udf=# select f_parse_xml(xml) from xml_log;
   f_parse_xml   
-----------------
 Liechtenstein:1
(1 row)

*/
