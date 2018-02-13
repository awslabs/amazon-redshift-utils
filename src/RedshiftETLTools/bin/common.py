import psycopg2
import os
import sys
import json
import base64
import boto3
import logging
import datetime

global nowString
nowString = "{:%Y-%m-%d_%H-%M-%S}".format(datetime.datetime.now())

def setupLogging(jobName, path):
    config = getConfig(path)
    logFileName = config['defLogConfigs']['logdirectory'] + jobName + "." + nowString + ".log"
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    handler = logging.FileHandler(logFileName)
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def getConfig(path):

    if path.startswith("s3://"):
        # download the configuration from s3
        s3Info = tokeniseS3Path(path)

        bucket = s3Client.get_bucket(s3Info[0])
        key = bucket.get_key(s3Info[1])

        configContents = key.get_contents_as_string()
        config = json.loads(configContents)
    else:
        with open(path) as f:
            config = json.load(f)
    return config

def returnConn(host, port, db, usr, pwd):
    connstring = '''dbname=%s user=%s password=%s
           host=%s port=%s''' % (db, usr, pwd, host, port)
    pcon = psycopg2.connect(connstring)
    pcon.autocommit = True
    return pcon

def rsConn(path):
    config = getConfig(path)
    srcConfig = config['redshiftCreds']
    srcHost = srcConfig['clusterEndpoint']
    srcPort = srcConfig['clusterPort']
    srcDB = srcConfig['db']
    srcUser = srcConfig['connectUser']
    srcPassword = srcConfig['connectPwd']
    return (returnConn(srcHost, srcPort, srcDB, srcUser, srcPassword))

def getDefaultParms(path):
    config = getConfig(path)
    defaultParms = config['defLoadCopyConfigs']
    addquotes = 'addquotes' if defaultParms['addquotes'] else ''
    gzip = 'gzip' if defaultParms['gzip'] else ''
    escape = 'escape' if defaultParms['escape'] else ''
    manifest = 'manifest' if defaultParms['manifest'] else ''
    allowoverwrite = 'allowoverwrite' if defaultParms['allowoverwrite'] else ''
    delimiter = defaultParms['delimiter']
    return  """%s %s %s %s %s delimiter '%s'""" %(addquotes, gzip, escape, manifest, allowoverwrite, delimiter)

def getDataDownloadPath(queryName, path):
    config = getConfig(path)
    return "%s/%s/%s/" % (config['s3Creds']['path'].rstrip("/"), queryName, nowString)

def getS3creds(path):
    config = getConfig(path)
    if config['s3Creds']['awsIamRole'] != "":
        accessRole = config['s3Creds']['awsIamRole']
        s3AccessCredentials = "aws_iam_role=%s" % accessRole
    else:
        awsAccessKeyId = config['s3Creds']['awsAccessKeyId']
        awsSecretAccessKey = config['s3Creds']['awsSecretAccessKey']
        s3AccessCredentials = "aws_access_key_id=%s;aws_secret_access_key=%s" % (awsAccessKeyId, awsSecretAccessKey)
    return s3AccessCredentials

def getQueryInfo(filePath):
    if os.path.basename(filePath).rsplit('.', 1)[1] == 'json':
        with open(filePath) as sqlFile:
            queryConfig = json.load(sqlFile)
            queryName = queryConfig['queryDetails']['queryName']
            sqlText = queryConfig['queryDetails']['query']
    else:
        queryName = os.path.basename(filePath).rsplit('.', 1)[0]
        with open(filePath) as f: sqlText = f.read()
    return queryName, sqlText

def execQuery(conn, query, logger):
    try:
        cur=conn.cursor()
        cur.execute(query)
        cur.close()
        return True

    except psycopg2.Error, e:
        try:
           print "Redshift Error [%d]: %s" % (e.args[0], e.args[1])
           logger.error("Redshift Error [%d]: %s" % (e.args[0], e.args[1]))
           return False
        except IndexError:
          print "Redshift Error: %s" % str(e)
          logger.error("Redshift Error: %s" % str(e))
          return False
        except TypeError, e:
          print(e)
          logger.error(e)
          return False
        except ValueError, e:
          print(e)
          logger.error(e)
          return False
    finally:
        cur.close()

def execSelect(conn, query, logger):
    try:
        cur=conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        cur.close()
        return results

    except psycopg2.Error, e:
        try:
           print "Redshift Error [%d]: %s" % (e.args[0], e.args[1])
           logger.error("Redshift Error [%d]: %s" % (e.args[0], e.args[1]))
           return False
        except IndexError:
          print "Redshift Error: %s" % str(e)
          logger.error("Redshift Error: %s" % str(e))
          return False
        except TypeError, e:
          print(e)
          logger.error(e)
          return False
        except ValueError, e:
          print(e)
          logger.error(e)
          return False
    finally:
        cur.close()

def returnRowCount(results):
  for i in results:
    return i[0]

def checkTableExists(conn, schemaName, tableName):
  sqlText = "select count(*) From pg_catalog.pg_tables where schemaname='" + schemaName + "' and tablename='"+ tableName +"'"
  results = execSelect(conn, sqlText)
  if returnRowCount(results)> 0:
    return True
  else:
    return False
