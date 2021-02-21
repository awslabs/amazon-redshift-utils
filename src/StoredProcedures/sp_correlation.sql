
CREATE or replace PROCEDURE sp_correlation(source_schema_name in varchar(255), source_table_name in varchar(255), target_column_name in varchar(255), output_temp_table_name inout varchar(255)) AS $$
DECLARE
  v_sql varchar(max);
  v_generated_sql varchar(max);
  v_source_schema_name varchar(255)=lower(source_schema_name);
  v_source_table_name varchar(255)=lower(source_table_name);
  v_target_column_name varchar(255)=lower(target_column_name);
BEGIN
  EXECUTE 'drop table if exists ' || output_temp_table_name;
  v_sql = '
select
  ''create temp table '|| output_temp_table_name||' as select ''|| outer_calculation||
  '' from (select count(1) number_of_items, sum('||v_target_column_name||') sum_target, sum(pow('||v_target_column_name||',2)) sum_square_target, pow(sum('||v_target_column_name||'),2) square_sum_target,''||
  inner_calculation||
  '' from (select ''||
  column_name||
  '' from '||v_source_table_name||'))''
from
  (
  select
    distinct
    listagg(outer_calculation,'','') over () outer_calculation
    ,listagg(inner_calculation,'','') over () inner_calculation
    ,listagg(column_name,'','') over () column_name
  from
    (
    select
      case when atttypid=16 then ''decode(''||column_name||'',true,1,0)'' else column_name end column_name
      ,atttypid
      ,''cast(decode(number_of_items * sum_square_''||rn||'' - square_sum_''||rn||'',0,null,(number_of_items*sum_target_''||rn||'' - sum_target * sum_''||rn||
        '')/sqrt((number_of_items * sum_square_target - square_sum_target) * (number_of_items * sum_square_''||rn||
        '' - square_sum_''||rn||''))) as numeric(5,2)) ''||column_name outer_calculation
      ,''sum(''||column_name||'') sum_''||rn||'',''||
            ''sum(trip_count*''||column_name||'') sum_target_''||rn||'',''||
            ''sum(pow(''||column_name||'',2)) sum_square_''||rn||'',''||
            ''pow(sum(''||column_name||''),2) square_sum_''||rn inner_calculation
    from
      (
      select
        row_number() over (order by a.attnum) rn
        ,a.attname::VARCHAR column_name
        ,a.atttypid
      FROM pg_namespace AS n
        INNER JOIN pg_class AS c ON n.oid = c.relnamespace
        INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
      where a.attnum > 0
        and n.nspname = '''||v_source_schema_name||'''
        and c.relname = '''||v_source_table_name||'''
        and a.atttypid in (16,20,21,23,700,701,1700)
      )
    )
)';
  execute v_sql into v_generated_sql;
  execute  v_generated_sql;
END;
$$ LANGUAGE plpgsql;
