#!/usr/bin/python
import psycopg2
from datetime import datetime
import queries
import argparse
from dbconstring import executequery, cleanup, connstring
from userprivs import executeddls


def createobjs(objtype, query, objcon, srccursor, tgtcursor, tgtcluster):
    srcobjlist = executequery(srccursor, query)
    tgtobjlist = executequery(tgtcursor, query)
    objlist = list(set(srcobjlist) - set(tgtobjlist))
    objcnt = 0
    objerr = False
    print "[%s] INFO: Starting creation of %s in target cluster" \
          % (str(datetime.now()), objtype + 's')
    if objlist:
        if objtype == 'database':
            objcon.commit()
            objcon.set_session(autocommit=True)
        try:
            for i in objlist:
                objname = i[0]
                objddl = i[1]
                tgtcursor.execute(objddl)
                objcnt += 1
                print "[%s] INFO: %s '%s' created" % (str(datetime.now()), objtype.title(), objname)
        except Exception as objerr:
            if objtype != 'database':
                print "[%s] ERROR: Creating %s '%s' failed. All created %ss will be rolled back. %s" % \
                      (str(datetime.now()), objtype, objname, objtype, str(objerr).rstrip('\n'))
        finally:
            if objerr:
                objcnt = 0
                if objtype == 'database':
                    objcon.set_session(autocommit=False)
                    print "[%s] ERROR: %s '%s' failed to create successfully" \
                          % (str(datetime.now()), objtype.title(), objname)
                else:
                    print "[%s] ERROR: Rolling back transaction on target cluster '%s'" \
                          % (str(datetime.now()), tgtcluster)
                    print "[%s] ERROR: %d %s created successfully" % (str(datetime.now()), objcnt, objtype + 's')
                    objcon.rollback()
            else:
                print "[%s] INFO: Transaction committed" % (str(datetime.now()))
                print "[%s] INFO: %d %s created successfully in target cluster '%s'" \
                      % (str(datetime.now()), objcnt, objtype + 's', tgtcluster)
                objcon.commit()
                if objtype == 'database':
                    objcon.set_session(autocommit=False)
    else:
        print "[%s] INFO: All %s already exist!" % (str(datetime.now()), objtype + 's')
    return objerr


def objconfig(srccursor, tgtcursor, query, objtype, tgtdb, tgtconn):
    srcobjconfig = executequery(srccursor, query)
    tgtobjconfig = executequery(tgtcursor, query)

    # Find privileges that are on source but not in target cluster
    usrobjconfig = list(set(srcobjconfig) - set(tgtobjconfig))

    # Add users to their respective groups
    if objtype == 'usrtogrp':
        print "[%s] INFO: Adding users to groups in database '%s' on target" % (str(datetime.now()), tgtdb)
        if usrobjconfig:
            for i in usrobjconfig:
                tgtcursor.execute(i[2])
                print "[%s] INFO: User '%s' added to group '%s'" % (str(datetime.now()), i[0], i[1])
            print "[%s] INFO: Users added to groups successfully" % (str(datetime.now()))
        else:
            print "[%s] INFO: All users already added to groups!" % (str(datetime.now()))

    # Add user settings usecreatedb,valuntil and useconnlimit from source to target cluster
    elif objtype == 'usrprofile':
        print "[%s] INFO: Copying user profiles to database '%s' on target" % (str(datetime.now()), tgtdb)
        if usrobjconfig:
            for i in usrobjconfig:
                tgtcursor.execute(i[1])
                print "[%s] INFO: User '%s' profile copied" % (str(datetime.now()), i[0])
            print "[%s] INFO: User profiles copied successfully" % (str(datetime.now()))
        else:
            print "[%s] INFO: All user profiles already copied!" % (str(datetime.now()))

    # Add useconfig settings from source to target cluster
    elif objtype == 'usrconfig':
        print "[%s] INFO: Copying user configs to database '%s' on target" % (str(datetime.now()), tgtdb)
        if usrobjconfig:
            for i in usrobjconfig:
                tgtcursor.execute(i[2])
                print "[%s] INFO: User '%s' config '%s' copied" % (str(datetime.now()), i[0], i[1])
            print "[%s] INFO: User configs copied successfully" % (str(datetime.now()))
        else:
            print "[%s] INFO: All user configs already copied!" % (str(datetime.now()))
    if usrobjconfig:
        tgtconn.commit()
        print "[%s] INFO: Transaction committed" % (str(datetime.now()))


def transferprivs(srccursor, tgtcursor, gettablequery, usrgrntquery, tgtdb):
    print "[%s] INFO: Starting transfer of user object privileges to database '%s' on target" \
          % (str(datetime.now()), tgtdb)
    # Get tables from target cluster to be used in extracting user privileges from source cluster
    tgttables = executequery(tgtcursor, gettablequery)
    # print tgttables
    tablelist = tuple([i for sub in tgttables for i in sub])
    # print tablelist
    if tablelist:
        tgtddl = executequery(tgtcursor, usrgrntquery, (tablelist,))
        # print tgtddl
        srcddl = executequery(srccursor, usrgrntquery, (tablelist,))
        # print srcddl
        # Find difference between privileges on source and target clusters
        ddl = list(set(srcddl) - set(tgtddl))
        print ddl
        if ddl:
            for i in ddl:
                # Copy user privileges from source cluster to target cluster
                tgtcursor.execute(i[2])
                print "[%s] INFO: %s" % (str(datetime.now()), i[2])
            print "[%s] INFO: User object privileges copied to database '%s' on target successfully" \
                  % (str(datetime.now()), tgtdb)
        else:
            print "[%s] INFO: All user object privileges already in database '%s' on target!" \
                  % (str(datetime.now()), tgtdb)
    else:
        print "[%s] INFO: No tables found in database '%s' on target!" % (str(datetime.now()), tgtdb)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tgtcluster", help="<target cluster endpoint>")
    parser.add_argument("--tgtuser", help="<superuser on target cluster>")
    parser.add_argument("--srccluster", help="<source cluster endpoint>")
    parser.add_argument("--srcuser", help="<superuser on source cluster>")
    parser.add_argument("--srcdbname", help="<source cluster database>")
    parser.add_argument("--tgtdbname", help="<target cluster database>")
    parser.add_argument("--dbport", help="set database port", default=5439)
    args = parser.parse_args()

    srchost = args.srccluster
    srclusterid = srchost.split('.')[0]
    srcuser = args.srcuser
    srcdbname = args.srcdbname
    tgtdbname = args.tgtdbname
    tgtuser = args.tgtuser
    tgthost = args.tgtcluster
    tgtclusterid = tgthost.split('.')[0]

    if srchost is None or tgthost is None or srcuser is None or tgtuser is None:
        parser.print_help()
        exit()

    tgtconstring = connstring(dbname=tgtdbname, dbhost=tgthost, clusterid=tgtclusterid, dbuser=tgtuser)
    srcconstring = connstring(dbname=srcdbname, dbhost=srchost, clusterid=srclusterid, dbuser=srcuser)

    try:
        srccon = psycopg2.connect(srcconstring)
        srccur = srccon.cursor()
        tgtcon = psycopg2.connect(tgtconstring)
        tgtcur = tgtcon.cursor()

        print "[%s] INFO: Starting transfer of metadata from source cluster %s to target cluster %s" % \
              (str(datetime.now()), srclusterid.title(), tgtclusterid.title())
        createobjs('database', queries.dblist, tgtcon, srccur, tgtcur, tgtclusterid)
        createobjs('schema', queries.schemalist, tgtcon, srccur, tgtcur, tgtclusterid)
        grperr = createobjs('group', queries.grouplist, tgtcon, srccur, tgtcur, tgtclusterid)
        usrerr = createobjs('user', queries.userlist, tgtcon, srccur, tgtcur, tgtclusterid)

        if not usrerr and not grperr:
            objconfig(srccur, tgtcur, queries.addusrtogrp, 'usrtogrp', tgtdbname, tgtcon)
            objconfig(srccur, tgtcur, queries.usrprofile, 'usrprofile', tgtdbname, tgtcon)
            objconfig(srccur, tgtcur, queries.usrconfig, 'usrconfig', tgtdbname, tgtcon)
        else:
            print "[%s] ERROR: Error while creating users or groups. Please fix and retry" % (str(datetime.now()))

        executeddls(srccur, tgtcur, queries.languageprivs, tgtuser)
        executeddls(srccur, tgtcur, queries.databaseprivs, tgtuser)
        executeddls(srccur, tgtcur, queries.schemaprivs, tgtuser)
        executeddls(srccur, tgtcur, queries.tableprivs, tgtuser)
        executeddls(srccur, tgtcur, queries.functionprivs, tgtuser)
        executeddls(srccur, tgtcur, queries.defaclprivs, tgtuser, 'defacl') 

        cleanup(tgtcur, tgtcon, 'target')
        cleanup(srccur, srccon, 'source')

    except Exception as err:
        print "[%s] ERROR: %s" % (str(datetime.now()), err)
        exit()


if __name__ == "__main__":
    main()
