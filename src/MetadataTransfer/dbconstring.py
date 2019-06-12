#!/usr/bin/python
from boto3 import client
from datetime import datetime


def connstring(dbname=None, dbhost=None, clusterid=None, dbport=5439, dbuser=None):
    try:
        response = client('redshift').get_cluster_credentials(DbUser=dbuser, DbName=dbname,
                                                              ClusterIdentifier=clusterid, AutoCreate=False)
        dbuser = response['DbUser']
        dbpwd = response['DbPassword']
        constring = "dbname='%s' port='%s' user='%s' password='%s' host='%s'" % (dbname, dbport, dbuser, dbpwd, dbhost)
        return constring
    except Exception as err:
        print "[%s] ERROR: Failed to generate connection string..." % (str(datetime.now()))
        print "[%s] ERROR: %s" % (str(datetime.now()), err)
        exit()
        return 'Failed'


def executequery(cursor, query, inlist=None):
    cursor.execute(query, inlist)
    return cursor.fetchall()


def cleanup(cursor, conn, cluster):
    if not conn.closed:
        conn.commit()
    if not cursor.closed:
        cursor.close()
    if not conn.closed:
        conn.close()
    if conn.closed:
        print "[%s] INFO: %s cluster connection closed" % (str(datetime.now()), cluster.title())
    else:
        print "[%s] ERROR: Failed to close %s cluster connection" % (str(datetime.now()), cluster)
