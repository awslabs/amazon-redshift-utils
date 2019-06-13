"""
GenUnload : Generate unload commands given a config file (config.ini) which has information about table, schema, partition column & sort columns
-------------------------------------------------------------------------------------------------------------------------------------------
-- Satish Sathiya 05/23/2019

example:

python3 genunload.py

Readme.md has the requirements info.

"""

from __future__ import print_function

import os
import sys


import datetime
from datetime import timedelta
import boto3
import base64
import json
import argparse
import configparser
import pgpasslib
import pg8000
import re

ssl = True

__version__ = "1.0"
pg8000.paramstyle = "qmark"


def connect(host, port, db, dbuser, table, schema, column_list, partition_column, sort_keys, s3path, iamrole):

    # get password from .pgpass or environment
    try:
        pg_pwd = pgpasslib.getpass(host,port,db,dbuser)
        print(pg_pwd)
        if pg_pwd:
            pwd = pg_pwd
    except pgpasslib.FileNotFound as e:
        pass

    # Connect to the cluster
    try:
        if debug:
            print('Connecting to Redshift: %s' % host)

        conn = pg8000.connect(database=db, user=dbuser, password=pwd, host=host, port=port, ssl=ssl)
        conn.autocommit = True
    except:
        print('Redshift Connection Failed: exception %s' % sys.exc_info()[1])
        raise
    
    if debug:
        print('Successfully connected to Redshift cluster')
        
    # create a new cursor
    cursor = conn.cursor()

    check_table_exists(cursor, conn, table, schema)
    full_column_list, partition_keys, partition_column_type = get_column_list_partition_keys(cursor, conn, table, schema, column_list, partition_column)
    gen_unload(full_column_list, partition_keys, partition_column_type, schema, table, partition_column, sort_keys, s3path, iamrole)

    if execute:
        print("Executing unload commands !")
        execute_unload(cursor, conn)

    conn.commit()
    if debug:
        print("Done with the script !!")


def check_table_exists(cursor, conn, table, schema):

    # check if table exists
    if debug:
        print('Check for table exists: %s' % table)

    stmt = "SELECT EXISTS (SELECT 1 FROM   information_schema.tables WHERE  table_schema = '%s' AND table_name = '%s');" % (schema, table)
    cursor.execute(stmt)

    s = cursor.fetchone()
    s = ' '.join(map(str, s))

    if s == 'False':
        print('Table does not exist: %s' % table)
        exit(1)
    else:
        print('Table %s exists' % table)


def get_column_list_partition_keys(cursor, conn, table, schema, column_list, partition_column):

    # get partition column data type
    stmt = "select data_type from information_schema.columns where table_name = '%s' and table_schema = '%s' and column_name = '%s';" % (
    table, schema, partition_column)

    if debug:
        print('Collecting data type information for partition column: %s' % partition_column)

    cursor.execute(stmt)
    partition_column_type = cursor.fetchone()

    if partition_column_type is None:
        print('Please check your partition column: %s' % partition_column)
        exit(1)

    partition_column_type = ' '.join(map(str, partition_column_type))

    if any(re.findall(r'integer|numeric|decimal|bigint|real|double precision|smallint', partition_column_type,
                      re.IGNORECASE)):
        partition_column_type = 'numeric'
    elif any(re.findall(r'timestamp without time zone|date|character varying|character|timestamp with time zone|bool|boolean', partition_column_type, re.IGNORECASE)):
        partition_column_type = 'alphanumeric'

    # if column_list not set , then select all columns except for partition column
    if not column_list:
        stmt = "select column_name from information_schema.columns where table_name = '%s' and table_schema = '%s' order by ordinal_position;" % (table, schema)

        if debug:
            print('Collecting column list excluding partition column: %s' % partition_column)

        cursor.execute(stmt)
        column_list = cursor.fetchall()
        full_column_list = [x[0] for x in column_list]

        full_column_list.remove(partition_column)
        full_column_list = ','.join(map(str, full_column_list))
    else:
        full_column_list = column_list


    # get distinct partition keys using partition column
    stmt = "select distinct %s from %s.%s;" % (partition_column, schema, table)

    if debug:
        print('Collecting distinct partition keys for partition column: %s [skipping NULL values]' % partition_column)

    cursor.execute(stmt)
    keys = cursor.fetchall()
    partition_keys = [x[0] for x in keys]

    print('Column list = %s' % full_column_list)
    # print('Partition keys = %s' % partition_keys)
    return full_column_list, partition_keys, partition_column_type


def gen_unload(full_column_list, partition_keys, partition_column_type, schema, table, partition_column, sort_keys, s3path, iamrole):
    """
    :param full_column_list: list
    :param partition_keys: list
    :param partition_column_type: str
    :param schema: str
    :param table: str
    :param partition_column: str
    :param sort_keys: str
    :param s3path: str
    :param iamrole: str
    :return: str
    """

    s3path = s3path[:-1] if s3path.endswith('/') else s3path

    column_list_str = full_column_list

    if sort_keys:
        sql = 'SELECT ' + column_list_str + ' FROM ' + schema + '.' + table + ' WHERE ' + partition_column + '=' + '<>' + ' ORDER BY ' + sort_keys
    else:
        sql = 'SELECT ' + column_list_str + ' FROM ' + schema + '.' + table + ' WHERE ' + partition_column + '=' + '<>'

    part1 = 'UNLOAD ( ' + '\'' + sql + '\''
    part3 = 'IAM_ROLE \'' + iamrole + '\' FORMAT PARQUET ALLOWOVERWRITE;'
    unload_stmt = str()

    for key in partition_keys:
        if key is not None:
            if partition_column_type == 'numeric':
                temp = part1.replace('<>', str(key))
                unload_stmt = unload_stmt + temp + ') TO ' + '\'' + s3path + '/' + partition_column + '=' + str(key) + '/\' ' + part3 + '\n'
            elif partition_column_type == 'alphanumeric':
                temp = part1.replace('<>', '\\\'' + str(key) + '\\\'')
                unload_stmt = unload_stmt + temp + ') TO ' + '\'' + s3path + '/' + partition_column + '=' + str(key) + '/\' ' + part3 + '\n'

    if debug:
        print('Generating unload statements !')
    with open('unload.sql', 'w') as file:
        file.write(unload_stmt)


def execute_unload(cursor, conn):
    with open(os.path.dirname(__file__) + 'unload.sql', 'r') as sql:
        unload_commands = sql.read()

    for s in unload_commands.split(";"):
        stmt = s.strip()
        if s is not None and stmt != "":
            if debug is True:
                print(stmt)
            try:
                cursor.execute(stmt)
            except Exception as e:
                if re.search(".*column.*does not exist*", str(e)) is not None:
                    print('Check the column list !')
                    raise
                else:
                    print(e)
                    raise e
    conn.commit()
    print('Done with executing unload commands !')


def main():

    config = configparser.ConfigParser()
    config.read('config.ini')

    parser = argparse.ArgumentParser()

    global debug
    global execute

    host = config.get('cluster', 'identifier', fallback=None)
    dbuser = config.get('cluster', 'dbuser', fallback='dbadmin')
    db = config.get('cluster', 'database', fallback='dev')
    port = int(config.get('cluster', 'port', fallback='5439'))
    schema = config.get('cluster', 'schema', fallback='public')
    table = config.get('cluster', 'table', fallback=None)
    partition_column = config.get('cluster', 'partition_key', fallback=None)
    sort_keys = config.get('cluster', 'sort_keys', fallback=None)
    column_list = config.get('cluster', 'column_list', fallback='All')
    debug = config.getboolean('cluster', 'debug', fallback=False)
    execute = config.getboolean('cluster', 'execute', fallback=False)

    s3path = config.get('s3', 'path', fallback=None)

    iamrole = config.get('creds', 'iam_role', fallback=None)

    if host is None or dbuser is None or db is None or schema is None or table is None or partition_column is None or sort_keys is None or column_list is None or s3path is None or iamrole is None:
            parser.print_help()
            exit()

    connect(host, port, db, dbuser, table, schema, column_list, partition_column, sort_keys, s3path, iamrole)


if __name__ == "__main__":
    main()
