from botocore.vendored import requests
import logging
import sys
import os
import site
import pg8000
import threading
from threading import Thread
import sys
import time

host=sys.argv[1]
port=sys.argv[2]
database=sys.argv[3]
masteruser=sys.argv[4]
masterpass=sys.argv[5]
awskeyid=sys.argv[6]
awssecretkey=sys.argv[7]
sqlWhere=sys.argv[8]
user=sys.argv[9]
password=sys.argv[10]
sqlSchema=sys.argv[11]
sqlWait=sys.argv[12]

print(host)

numQueries = 1
if sqlWhere == "COPY":
    numQueries = 3
    
sqlColumn = ' querytxt  '
sqlTable = '( with built_list as ( select 1 rnk union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9 union select 10 union select 11 union select 12 union select 13 union select 14 union select 15 union select 16 union select 17 union select 18 union select 19 union select 20) select querytxt , sql_type from public.tpc_h_sqls join built_list b1 on 1=1 join built_list b2 on 1=1  join built_list b3 on 1=1 order by b1.rnk, b2.rnk, b3.rnk, tpch_sql_number ) t'


def execute(statement):
    conn = pg8000.connect(database=database, user=user, password=password, host=host, port=port)
    print('Successfully Connected to Cluster with regular user')
    # create a new cursor for methods to run through
    try:
        print('starting to execute')
        cursor = conn.cursor()
        print('right before execute')
        result = cursor.execute('set enable_result_cache_for_session to off')
        print('starting to execute 01')
        result = cursor.execute('set query_group=\'' + sqlWhere + '\'')
        result = cursor.execute('set search_path=\'' + sqlSchema + '\'')
        print('starting to execute 02')
        result = cursor.execute(statement)
        print('Finished running query')
        conn.close()
    except:
        logging.error("Query failed: " + statement)

rcount = 0

print("the record count is:", rcount)

if rcount >= 0:
    # Connect to the cluster
    try:
        print('Connecting to Redshift: %s' % host)
        conn = pg8000.connect(database=database, user=masteruser,password=masterpass, host=host, port=port)
        print('Successfully Connected to Cluster using master user')

        # create a new cursor for methods to run through
        cursorTables = conn.cursor()
        statementTables = "select " + sqlColumn + " from " + sqlTable + " where sql_type = '" + sqlWhere + "'  "
        result = cursorTables.execute(statementTables)
        print('number of sql stmt: %s' % cursorTables.arraysize)

        while True:
            threads = []
            queries = cursorTables.fetchmany(numQueries)
            
            if not queries:
                break

            for query in queries:
                querytxt=query[0].replace("$awskeyid",awskeyid).replace("$awssecretkey",awssecretkey)
                thread = Thread(target=execute, args=(querytxt,))
                print('query statement after execution')
                threads.append(thread)
                print('query statement after append thread')            
                thread.start()
                print('query statement after thread start')

            # wait for all threads to complete
            for thread in threads:
                thread.join()
                
            print('query statement before sleep start')
            time.sleep(int(sqlWait))
        
        conn.close()

    except:
        reason = 'Redshift Connection Failed: exception %s' % sys.exc_info()[1]
        print(reason)
else:
    print('already running query')
	
