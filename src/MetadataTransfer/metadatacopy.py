#!/usr/bin/python3
import boto3
import redshift_connector
from redshiftfunc import getiamcredentials
from datetime import datetime
import queries
import argparse
from userprivs import executeddls
import log

__version__ = "1.0"

logger = log.setup_custom_logger('MetadataTransfer')
logger.info('Starting the MetadataTransferUtility')

def createobjs(objtype, query, objcon, srccursor, tgtcursor, tgtcluster):


    logger.debug("Executing query: %s" % query)
    
    srcres = srccursor.execute(query)
    srcobjlist_2 = srcres.fetchall()

    src_tup = ()

    # Convert from a tuple of lists ([a,b],[c,d])
    # into a tuple of tuples: ((a,b),(c,d))
    for tps in srcobjlist_2:
        src_tup = src_tup + (tuple(tps),)

    # Target
    tgtres = tgtcursor.execute(query)
    tgtobjlist_2 = tgtres.fetchall()

    tgt_tup = ()
    
    for tpt in tgtobjlist_2:
        tgt_tup = tgt_tup + (tuple(tpt),)

    objlist = set(src_tup) - set(tgt_tup)

    objcnt = 0
    objerr = False

    logger.info( "Starting creation of %s in target cluster" \
          % (objtype + 's'))

    if objlist:
        logger.info("Starting objlist...")
        if objtype == 'database':
            logger.info("Starting database...")
            objcon.commit()
            objcon.autocommit = True
        try:
            for i in objlist:
                logger.info("Starting objlist...")
                objname = i[0]
                objddl = i[1]
                
                logger.info("-> Object Name: " + i[0])
                tgtcursor.execute(objddl)
                objcnt += 1
                logger.info("%s '%s' created" % (objtype.title(), objname))
        except Exception as objerr:
            logger.error(objerr)
            if objtype != 'database':
                logger.error( "Creating %s '%s' failed. All created %ss will be rolled back. %s" % \
                      (objtype, objname, objtype, str(objerr).rstrip('\n')))
        finally:
            if objerr:
                objcnt = 0
                if objtype == 'database':
                    objcon.autocommit = False
                    logger.info( "%s '%s' failed to create successfully" \
                          % (objtype.title(), objname))
                else:
                    logger.info( "Rolling back transaction on target cluster '%s'" % (tgtcluster))
                    logger.info( "%d %s created successfully" % (objcnt, objtype + 's'))
                    objcon.rollback()
            else:
                logger.info( "Transaction committed" )
                logger.info( "%d %s created successfully in target cluster '%s'" \
                      % (objcnt, objtype + 's', tgtcluster))
                objcon.commit()
                if objtype == 'database':
                    objcon.autocommit = False
    else:
        logger.info( "All %s already exist!" % (objtype + 's'))
    return objerr


def objconfig(srccursor, tgtcursor, query, objtype, tgtdb, tgtconn):
    srcobjconfig = srccursor.execute(query)
    tgtobjconfig = tgtcursor.execute(query)

    srcobjconf = srcobjconfig.fetchall()
    src_tup = ()

    tgtobjconf = tgtobjconfig.fetchall()
    tgt_tup = ()

    for tps in srcobjconf:
        src_tup = src_tup + (tuple(tps),)

    for tps in tgtobjconf:
        tgt_tup = tgt_tup + (tuple(tps),)

    # Find privileges that are on source but not in target cluster
    usrobjconfig = list(set(src_tup) - set(tgt_tup))

    # Add users to their respective groups
    if objtype == 'usrtogrp':
        logger.info( "Adding users to groups in database '%s' on target" % (tgtdb))
        if usrobjconfig:
            for i in usrobjconfig:
                tgtcursor.execute(i[2])
                logger.info( "User '%s' added to group '%s'" % (i[0], i[1]))
            logger.info( "Users added to groups successfully" )
        else:
            logger.info( "All users already added to groups!" )

    # Add user settings usecreatedb,valuntil and useconnlimit from source to target cluster
    elif objtype == 'usrprofile':
        logger.info( "Copying user profiles to database '%s' on target" % tgtdb)
        if usrobjconfig:
            for i in usrobjconfig:
                tgtcursor.execute(i[1])
                logger.info( "User '%s' profile copied" % (i[0]))
            logger.info( "User profiles copied successfully" )
        else:
            logger.info( "All user profiles already copied!" )

    # Add useconfig settings from source to target cluster
    elif objtype == 'usrconfig':
        logger.info( "Copying user configs to database '%s' on target" % tgtdb)
        if usrobjconfig:
            for i in usrobjconfig:
                tgtcursor.execute(i[2])
                logger.info( "User '%s' config '%s' copied" % (i[0], i[1]))
            logger.info( "User configs copied successfully" )
        else:
            logger.info( "All user configs already copied!" )
    if usrobjconfig:
        tgtconn.commit()
        logger.info( "Transaction committed" )


def transferprivs(srccursor, tgtcursor, gettablequery, usrgrntquery, tgtdb):
    
    logger.info( "Starting transfer of user object privileges to database '%s' on target" % (tgtdb))
    
    # Get tables from target cluster to be used in extracting user privileges from source cluster
    logger.info(query)

    srctables = srccursor.execute(query)

    srctbls = srctables.fetchall()
    src_tup = ()

    for tps in srctbls:
        src_tup = src_tup + (tuple(tps),)


    tablelist = tuple([i for sub in tgttables for i in sub])

    if tablelist:
        tgtddl = executequery(tgtcursor, usrgrntquery, (tablelist,))
        
        srcddl = executequery(srccursor, usrgrntquery, (tablelist,))
        
        # Find difference between privileges on source and target clusters
        ddl = list(set(srcddl) - set(tgtddl))
        logger.info(ddl)
        if ddl:
            for i in ddl:
                # Copy user privileges from source cluster to target cluster
                tgtcursor.execute(i[2])
                logger.info( "[%s] INFO: %s" % (str(datetime.now()), i[2]))
            logger.info( "[%s] INFO: User object privileges copied to database '%s' on target successfully" \
                  % (str(datetime.now()), tgtdb))
        else:
            logger.info( "[%s] INFO: All user object privileges already in database '%s' on target!" \
                  % (str(datetime.now()), tgtdb))
    else:
        logger.info( "[%s] INFO: No tables found in database '%s' on target!" % (str(datetime.now()), tgtdb))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tgtcluster", help="<target cluster endpoint>")
    parser.add_argument("--tgtuser", help="<superuser on target cluster>")
    parser.add_argument("--srccluster", help="<source cluster endpoint>")
    parser.add_argument("--srcuser", help="<superuser on source cluster>")
    parser.add_argument("--srcdbname", help="<source cluster database>")
    parser.add_argument("--tgtdbname", help="<target cluster database>")
    parser.add_argument("--dbport", help="set database port", default=5439, type=int)
    args = parser.parse_args()

    srchost = args.srccluster
    srcclusterid = srchost.split('.')[0]
    srcuser = args.srcuser
    srcdbname = args.srcdbname
    tgtdbname = args.tgtdbname
    tgtuser = args.tgtuser
    tgthost = args.tgtcluster
    tgtclusterid = tgthost.split('.')[0]
    rsport = args.dbport

    if srchost is None or tgthost is None or srcuser is None or tgtuser is None:
        parser.print.help()
        exit()

    src_credentials = getiamcredentials(srchost,srcdbname,srcuser)
    logger.debug( ( "Source IAM User:%s , Expiration: %s " % (src_credentials['DbUser'], src_credentials['Expiration']  )   ) )

    tgt_credentials = getiamcredentials(tgthost,tgtdbname,tgtuser)
    logger.debug( ( "Target IAM User:%s , Expiration: %s " % (tgt_credentials['DbUser'], tgt_credentials['Expiration']  )   ) )

    # Extract temp credentials
    src_rs_user=src_credentials['DbUser']
    src_rs_pwd =src_credentials['DbPassword']

    tgt_rs_user=tgt_credentials['DbUser']
    tgt_rs_pwd =tgt_credentials['DbPassword']

    try:
        src_rs_conn = redshift_connector.connect(database=srcdbname, user=src_rs_user, password=src_rs_pwd, host=srchost, port=rsport, ssl=True)
        src_rs_conn.autocommit = True

        logger.info("Successfully connected to Redshift cluster: %s" % srchost)
        srccur: redshift_connector.Cursor = src_rs_conn.cursor()
    

        tgt_rs_conn = redshift_connector.connect(database=tgtdbname, user=tgt_rs_user, password=tgt_rs_pwd, host=tgthost, port=rsport, ssl=True)
        tgt_rs_conn.autocommit = True
        logger.info("Successfully connected to Redshift cluster: %s" % tgthost)
        tgtcur: redshift_connector.Cursor = tgt_rs_conn.cursor()
        
        #Set the Application Name
        set_name = "set application_name to 'MetadataTransferUtility-v%s'" % __version__

        srccur.execute(set_name)
        tgtcur.execute(set_name)

        logger.info( "Starting transfer of metadata from source cluster %s to target cluster %s" % \
              (srcclusterid.title(), tgtclusterid.title()))
        
        createobjs('database', queries.dblist, tgt_rs_conn, srccur, tgtcur, tgtclusterid)
        createobjs('schema', queries.schemalist, tgt_rs_conn, srccur, tgtcur, tgtclusterid)
        
        grperr = createobjs('group', queries.grouplist, tgt_rs_conn, srccur, tgtcur, tgtclusterid)
        usrerr = createobjs('user', queries.userlist, tgt_rs_conn, srccur, tgtcur, tgtclusterid)

        if not usrerr and not grperr:
            objconfig(srccur, tgtcur, queries.addusrtogrp, 'usrtogrp', tgtdbname, tgt_rs_conn)
            objconfig(srccur, tgtcur, queries.usrprofile, 'usrprofile', tgtdbname, tgt_rs_conn)
            objconfig(srccur, tgtcur, queries.usrconfig, 'usrconfig', tgtdbname, tgt_rs_conn)
        else:
            logger.info( "Error while creating users or groups. Please fix and retry" )

        logger.info("Executing language privileges...")
        executeddls(srccur, tgtcur, queries.languageprivs, tgtuser)
        logger.info("Executing database privileges...")
        executeddls(srccur, tgtcur, queries.databaseprivs, tgtuser)
        logger.info("Executing schema privileges...")
        executeddls(srccur, tgtcur, queries.schemaprivs, tgtuser)
        logger.info("Executing table privileges...")
        executeddls(srccur, tgtcur, queries.tableprivs, tgtuser)
        logger.info("Executing function privileges...")
        executeddls(srccur, tgtcur, queries.functionprivs, tgtuser)
        logger.info("Executing ACL privileges...")
        executeddls(srccur, tgtcur, queries.defaclprivs, tgtuser, 'defacl') 

        logger.info("Completed Metadata Transfer")

    except Exception as err:
        logger.error(err)
        exit()


if __name__ == "__main__":
    main()
