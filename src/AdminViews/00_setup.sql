create schema admin;

grant all on schema admin to group public;
grant select on all tables in schema admin to group public;
