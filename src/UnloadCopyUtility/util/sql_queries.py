GET_DATABASE_NAME_OWNER_ACL = """
  SELECT
    datname AS name,
    datdba AS owner,
    datacl AS acl 
  FROM pg_database
  WHERE datname = '{db}';
"""
GET_SCHEMA_NAME_OWNER_ACL = """
  SELECT 
    pn.nspname AS name, 
    pu.usename AS owner,
    pn.nspacl AS acl
  FROM pg_namespace pn 
  LEFT JOIN pg_user pu 
  ON pn.nspowner = pu.usesysid 
  WHERE nspname = '{schema}';
"""
CREATE_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS {schema};
"""
GET_TABLE_NAME_OWNER_ACL = """
  SELECT
    pc.relname AS name,
    pu.usename AS owner,
    pc.relacl AS acl
  FROM pg_class pc 
  LEFT JOIN pg_namespace pn
  ON pc.relnamespace = pn.oid
  LEFT JOIN pg_user pu
  ON pc.relowner = pu.usesysid
  WHERE pc.relname = '{table}'
  AND pn.nspname = '{schema}'; 
"""
