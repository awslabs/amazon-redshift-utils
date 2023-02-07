# Schema and Tables creation related statements
CHECK_DB_OBJECTS="""
select count(1) as table_count from pg_class inner join pg_namespace on pg_class.relkind='r' and pg_class.relnamespace=pg_namespace.oid 
where pg_namespace.nspname='history' and pg_class.relname IN ('user_last_login', 'stg_user_last_login');
"""
CREATE_SCHEMA='CREATE SCHEMA IF NOT EXISTS history';
CREATE_STAGE_TABLE="""CREATE TABLE IF NOT EXISTS history.stg_user_last_login (
  "username" varchar(50) ,
  "lastlogin" TIMESTAMP WITHOUT TIME ZONE
);
"""
CREATE_TARGET_TABLE="""CREATE TABLE IF NOT EXISTS history.user_last_login (
  "username" varchar(50),
  "lastlogin" TIMESTAMP WITHOUT TIME ZONE
);
"""


# Queries to be executed. 
TRUNCATE_STAGE_TABLE = "truncate table history.stg_user_last_login;"

LOAD_STAGE_TABLE="""
insert into history.stg_user_last_login
select
     NVL(
       substring(NULLIF(regexp_substr(pui.usename, ':[^:]*'),''),2)
    ,  trim(pui.usename)
  ) as extracted_username
  ,max(start_time) as lastlogin
from sys_query_history sqh
left join pg_user_info pui
On sqh.user_id = pui.usesysid
group by 1
;
"""

UPDATE_TARGET_TABLE_FROM_STAGE="""
update history.user_last_login
set lastlogin = stg_user_last_login.lastlogin
from history.stg_user_last_login
where user_last_login.username = stg_user_last_login.username
and   user_last_login.lastlogin <> stg_user_last_login.lastlogin
;
"""

INSERT_TARGET_TABLE_FROM_STAGE="""
insert into history.user_last_login
select * from history.stg_user_last_login where username NOT IN 
  (select username from history.user_last_login)
;
"""
