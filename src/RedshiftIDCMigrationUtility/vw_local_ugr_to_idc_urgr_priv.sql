CREATE OR REPLACE VIEW vw_local_ugr_to_idc_urgr_priv
as
with privs as (
select 1 as seq_no
, NULL as database_name 
, NULL as namespace_name
, NULL as relation_name
, NULL as column_name
, NULL as privilege_type
, NULL as identity_name
, NULL as identity_id
, NULL as identity_type
,'CREATE GROUP "' + groname + '";' as "existing_grants"
,'CREATE role "' + namespc  + ':' + groname + '";' as "idc_based_grants" 
from pg_group,SVV_IDENTITY_PROVIDERS where type='awsidc'
UNION ALL
select 1 as seq_no
, NULL as database_name 
, NULL as namespace_name
, NULL as relation_name
, NULL as column_name
, NULL as privilege_type
, NULL as identity_name
, NULL as identity_id
, NULL as identity_type
,'CREATE ROLE "' + role_name + '";' as "existing_grants"
,'CREATE role "' + namespc  + ':' + CONCAT(case when role_name like '%:%' then split_part(role_name,':',2) else role_name end, '";')  as "idc_based_grants" 
from svv_roles,SVV_IDENTITY_PROVIDERS where type='awsidc' and role_name not like '%:%'
UNION ALL
select 
2 as seq_no 
, database_name 
, NULL as namespace_name 
, NULL as relation_name
, NULL as column_name
, privilege_type 
, identity_name
, identity_id
, identity_type
,'GRANT '+ privilege_type + ' ON DATABASE ' + database_name + ' TO ' + replace(identity_type,'user','') + ' "' + identity_name + '"' + ' ;' as "existing_grants"
,'GRANT '+ privilege_type + ' ON DATABASE ' + database_name + ' TO ' + CONCAT(case when identity_type='user' then '' when identity_type='group' then 'role' else identity_type end, ' "') + namespc + ':' + CONCAT(case when identity_name like '%:%' then split_part(identity_name,':',2) else identity_name end, '";') as "idc_based_grants" 
from svv_database_privileges s, SVV_IDENTITY_PROVIDERS where identity_type<>'public' and type='awsidc'
UNION ALL
select
3 as seq_no
,current_database() as database_name
,namespace_name
,NULL as relation_name
,NULL as column_name
,privilege_type 
,identity_name
,identity_id 
,identity_type 
,'GRANT '+ privilege_type + ' ON SCHEMA ' + namespace_name + ' TO ' + replace(identity_type,'user','') + ' "' + identity_name + '"' + ' ;' as "existing_grants"
,'GRANT '+ privilege_type + ' ON SCHEMA ' + namespace_name + ' TO ' + CONCAT(case when identity_type='user' then '' when identity_type='group' then 'role' else identity_type end, ' "') + namespc + ':' + CONCAT(case when identity_name like '%:%' then split_part(identity_name,':',2) else identity_name end, '";') as "idc_based_grants" 
from SVV_SCHEMA_PRIVILEGES, SVV_IDENTITY_PROVIDERS where identity_type<>'public' and type='awsidc'
UNION ALL
select
4 as seq_no
,current_database() as database_name
,namespace_name
,relation_name
,NULL as column_name
,privilege_type 
,identity_name
,identity_id 
,identity_type 
,'GRANT '+ privilege_type + ' ON TABLE ' + namespace_name + '.' + relation_name + ' TO ' + replace(identity_type,'user','') + ' "' + identity_name + '"' + ' ;' as "existing_grants"
,'GRANT '+ privilege_type + ' ON TABLE ' + namespace_name + '.' + relation_name + ' TO ' + CONCAT(case when identity_type='user' then '' when identity_type='group' then 'role' else identity_type end, ' "') + namespc + ':' + CONCAT(case when identity_name like '%:%' then split_part(identity_name,':',2) else identity_name end, '";') as "idc_based_grants" 
from SVV_RELATION_PRIVILEGES, SVV_IDENTITY_PROVIDERS where identity_type<>'public' and type='awsidc'
UNION ALL
select 
5 as seq_no
,current_database() as database_name
,namespace_name
,relation_name
,column_name
,privilege_type
,identity_name
,identity_id
,identity_type
,'GRANT '+ privilege_type + ' (' + column_name + ') ' + 'ON TABLE ' + namespace_name + '.' + relation_name + ' TO ' + replace(identity_type,'user','') + ' "' + identity_name + '" ;' as "existing_grants"
,'GRANT '+ privilege_type + ' (' + column_name + ') ' + 'ON TABLE ' + namespace_name + '.' + relation_name + ' TO ' + CONCAT(case when identity_type='user' then '' when identity_type='group' then 'role' else identity_type end, ' "') + namespc + ':' + CONCAT(case when identity_name like '%:%' then split_part(identity_name,':',2) else identity_name end, '";') as "idc_based_grants" 
from svv_column_privileges, SVV_IDENTITY_PROVIDERS where identity_type<>'public' and type='awsidc'
UNION ALL
select 
6 as seq_no
,current_database() as database_name
,schema_name as namespace_name
,NULL as relation_name
,NULL as column_name
,privilege_type
,grantee_name as identity_name
,grantee_id as identity_id 
,grantee_type as identity_type
,'ALTER DEFAULT PRIVILEGES ' 
+ CONCAT(case when schema_name is NULL then ' FOR USER ' else ' IN SCHEMA ' end, ' ') + CONCAT(case when schema_name is NULL then owner_name else schema_name end, '') + ' GRANT ' + privilege_type + ' ON ' 
+  CONCAT(case when object_type='RELATION' then ' TABLES ' else object_type end, ' TO ') 
+ replace(identity_type,'user','')  
+ ' "' + grantee_name + '" ;' 
as "existing_grants"
,'GRANT '+ privilege_type + ' FOR ' 
+  CONCAT(case when object_type='RELATION' then 'TABLES ' when object_type='PROCEDURE' then 'PROCEDURES ' when object_type='FUNCTION' then 'FUNCTIONS ' else object_type end, ' IN SCHEMA ') 
+ schema_name + ' TO ROLE "'
+ namespc + ':' + CONCAT(case when identity_name like '%:%' then split_part(identity_name,':',2) else identity_name end, '";') as "idc_based_grants" 
from svv_default_privileges, SVV_IDENTITY_PROVIDERS where identity_type<>'public' and type='awsidc'

UNION ALL

select 
7 as seq_no
,current_database() as database_name
,namespace_name
,NULL as relation_name
,NULL as column_name
,privilege_type
,identity_name
,identity_id
,identity_type
,'GRANT '+ privilege_type + ' ON FUNCTION ' + namespace_name + '.' + function_name + '(' + argument_types +') TO ROLE' + identity_name +';' as "existing_grants"
,'GRANT '+ privilege_type + ' ON FUNCTION ' + namespace_name + '.' + function_name + '(' + argument_types +') TO ' +  

CONCAT(case when identity_type='user' then '' when identity_type='group' then 'role' else identity_type end, ' "') + namespc + ':' + CONCAT(case when identity_name like '%:%' then split_part(identity_name,':',2) else identity_name end, '";')
 as "idc_based_grants" 
from SVV_FUNCTION_PRIVILEGES,SVV_IDENTITY_PROVIDERS where identity_type<>'public' and type='awsidc'

UNION ALL

select 
8 as seq_no
,current_database() as database_name
,NULL as namespace_name
,NULL as relation_name
,NULL as column_name
,privilege_type
,identity_name
,identity_id
,identity_type
,'GRANT '+ privilege_type + ' ON DATASHARE ' + datashare_name + ' TO ROLE' + identity_name +';' as "existing_grants"

,'GRANT '+ privilege_type + ' ON DATASHARE ' + datashare_name + ' TO ' +  
CONCAT(case when identity_type='user' then '' when identity_type='group' then 'role' else identity_type end, ' "') + namespc + ':' + CONCAT(case when identity_name like '%:%' then split_part(identity_name,':',2) else identity_name end, '";')
 as "idc_based_grants" 
from SVV_DATASHARE_PRIVILEGES,SVV_IDENTITY_PROVIDERS where identity_type<>'public' and type='awsidc'

UNION ALL

select 
9 as seq_no
,current_database() as database_name
,namespace_name
,NULL as relation_name
,NULL as column_name
,privilege_type
,identity_name
,identity_id
,identity_type
,'GRANT '+ privilege_type + ' ON MODEL ' + namespace_name + '.' + model_name + ' TO ROLE' + identity_name +';' as "existing_grants"
,'GRANT '+ privilege_type + ' ON MODEL ' + namespace_name + '.' + model_name + ' TO ' +  

CONCAT(case when identity_type='user' then '' when identity_type='group' then 'role' else identity_type end, ' "') + namespc + ':' + CONCAT(case when identity_name like '%:%' then split_part(identity_name,':',2) else identity_name end, '";')
 as "idc_based_grants" 
from SVV_ML_MODEL_PRIVILEGES,SVV_IDENTITY_PROVIDERS where identity_type<>'public' and type='awsidc'

UNION ALL

select 
10 as seq_no
,current_database() as database_name
,NULL as namespace_name
,NULL as relation_name
,NULL as column_name
,NULL as privilege_type
,role_name as identity_name
,role_id as identity_id
,'role' as identity_type
,'GRANT ROLE '+ granted_role_name + ' TO ROLE ' + role_name +';' as "existing_grants"
,'GRANT ROLE "'+ namespc + ':' + granted_role_name + '" TO ROLE "' + namespc + ':' + role_name + '";' as "idc_based_grants" 
from SVV_ROLE_GRANTS, SVV_IDENTITY_PROVIDERS where identity_type<>'public' and type='awsidc' and role_name not like 'sys:%'

)
select database_name, namespace_name,relation_name,column_name,privilege_type, identity_name, identity_id, identity_type,
existing_grants,idc_based_grants
from privs order by seq_no;