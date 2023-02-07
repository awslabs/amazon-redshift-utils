import argparse
import boto3
import json
import logging
import os
import pandas as pd
import re
import redshift_connector
from pandas import CategoricalDtype

from boto3 import client
from botocore.exceptions import ClientError
from contextlib import contextmanager
from io import StringIO
from report_gen import pdf_gen
from report_util import Report, styles
from tabulate import tabulate
from util import db_connect, init_logging, cluster_dict, bucket_dict, get_secret, create_json

g_stylesheet = styles()
g_columns = g_stylesheet.get('columns')


def launch_analysis_v2():
    """Package install and server init"""

    # add explicit instructions for user

    os.system("pip install -r requirements.txt")
    os.chdir(f'{os.getcwd()}/gui')

    # explicit version checking
    if os.system("node -v") != 0:
        print("Please install node before proceeding.")
        exit(-1)

    if os.system("npm install") != 0:
        print("Could not install npm packages. ")

    os.system("npm run start-backend &")
    os.system("npm start")


def run_replay_analysis(replay, cluster_endpoint, start_time, end_time, bucket_url, iam_role, user, tag='',
                        workload="",
                        is_serverless=False, secret_name=None, nlb_nat_dns=None, complete=True, stats=None,
                        summary=None):
    """End to end data collection, parsing, analysis and pdf generation

    @param replay: str, replay id from replay.py
    @param cluster_endpoint: str, target cluster endpoint
    @param start_time: datetime object, start time of replay
    @param end_time: datetime object, end time of replay
    @param bucket_url: str, S3 bucket location
    @param iam_role: str, IAM ARN for unload
    @param user: str, master username for cluster
    @param tag: str, optional identifier
    @param is_serverless: bool, serverless or provisioned cluster
    @param secret_name: str, name of the secret that stores admin username and password
    @param nlb_nat_dns: str, dns endpoint if specified will be used to connect instead of target cluster endpoint
    @param complete: bool, complete/incomplete replay run
    @param stats: dict, run details
    @param summary: str list, replay output summary from replay.py
    """

    logger = logging.getLogger("SimpleReplayLogger")
    s3_client = boto3.client('s3')
    cluster = cluster_dict(cluster_endpoint, is_serverless, start_time, end_time)
    cluster["is_serverless"] = is_serverless
    cluster["secret_name"] = secret_name
    cluster["host"] = nlb_nat_dns if nlb_nat_dns != None else cluster["host"]

    if type(bucket_url) is str:
        bucket = bucket_dict(bucket_url)

    logger.debug(bucket)

    logger.info(f"Running analysis for replay: {replay}")
    replay_path = f"{bucket['prefix']}analysis/{replay}"

    # unload from cluster
    queries = unload(bucket, iam_role, cluster, user, replay)
    info = create_json(replay, cluster, workload, complete, stats, tag)
    try:
        boto3.resource('s3').Bucket(bucket.get('bucket_name')).upload_file(info, f"{replay_path}/{info}")
    except ClientError as e:
        logger.error(f"{e} Could not upload info. Confirm IAM permissions include S3::PutObject.")

    if is_serverless:
        exit(0)
    else:
        report = Report(cluster, replay, bucket, replay_path, tag, complete)

        try:
            # iterate through query csv results and import
            for q in queries:
                get_raw_data(report, bucket, replay_path, q)

        except s3_client.exceptions.NoSuchKey as e:
            logger.error(f"{e} Raw data does not exist in S3. Error in replay analysis.")
            exit(-1)
        except Exception as e:
            logger.error(f"{e}: Data read failed. Error in replay analysis.")
            exit(-1)

        # generate replay_id_report.pdf and info.json
        logger.info(f"Generating report.")
        pdf = pdf_gen(report, summary)

        s3_resource = boto3.resource('s3')
        # upload to s3 and output presigned urls
        try:
            s3_resource.Bucket(bucket.get('bucket_name')).upload_file(pdf, f"{replay_path}/out/{pdf}")
            s3_resource.Bucket(bucket.get('bucket_name')).upload_file(info, f"{replay_path}/out/{info}")
            analysis_summary(bucket.get('url'), replay)
        except ClientError as e:
            logger.error(f"{e} Could not upload report. Confirm IAM permissions include S3::PutObject.")
            exit(-1)


def run_comparison_analysis(bucket, replay1, replay2):
    """ Compares two given replays using aggregated_data/ from S3

    @param bucket: str, S3 bucket location
    @param replay1: str, replay id 1
    @param replay2: str, replay id 2
    """

    print(f"Compare replays {replay1} and {replay2}. Upload to S3 bucket: {bucket}.")
    # iterate through S3 for aggregated data for replays
    # import data to dataframes
    # format comparison data
    # calculate percent difference, etc.
    # generate comparison report
    # print summary


@contextmanager
def initiate_connection(username, cluster):
    """ Initiate connection with Redshift cluster

    @param username: master username from replay.yaml
    @param cluster: cluster dictionary
    """

    response = None
    logger = logging.getLogger("SimpleReplayLogger")

    if cluster.get("is_serverless"):
        secret_name = get_secret(cluster.get('secret_name'), cluster.get("region"))
        response = {'DbUser': secret_name["admin_username"], 'DbPassword': secret_name["admin_password"]}
    else:
        rs_client = client('redshift', region_name=cluster.get("region"))
        # get response from redshift to get cluster credentials using provided cluster info
        try:
            response = rs_client.get_cluster_credentials(
                DbUser=username,
                DbName=cluster.get("database"),
                ClusterIdentifier=cluster.get("id"),
                DurationSeconds=900,
                AutoCreate=False,
            )
        except rs_client.exceptions.ClusterNotFoundFault:
            logger.error(
                f"Cluster {cluster.get('id')} not found. Please confirm cluster endpoint, account, and region.")
            exit(-1)
        except Exception as e:
            logger.error(
                f"Unable to connect to Redshift. Confirm IAM permissions include Redshift::GetClusterCredentials."
                f" {e}")
            exit(-1)

    if response is None or response.get('DbPassword') is None:
        logger.error(f"Failed to retrieve credentials for user {username} ")
        response = None

    # define cluster string/dict
    cluster_string = {
        "username": response["DbUser"],
        "password": response["DbPassword"],
        "host": cluster.get("host"),
        "port": cluster.get("port"),
        "database": cluster.get("database"),
    }

    conn = None
    try:
        logger.info(f"Connecting to {cluster.get('id')}")
        conn = db_connect(host=cluster_string["host"], port=int(cluster_string["port"]),
                          username=cluster_string["username"], password=cluster_string["password"],
                          database=cluster_string["database"])  # yield to reuse connection
        yield conn
    except redshift_connector.error.Error as e:
        logger.error(f"Unable to connect to Redshift. Please confirm credentials. {e} ")
        exit(-1)
    except Exception as e:
        logger.error(f"Unable to connect to Redshift. {e}")
        exit(-1)
    finally:
        if conn is not None:
            conn.close()


# def unload(unload_location, iam_role, cluster, user, path):
def unload(unload_location, iam_role, cluster, user, replay):
    """Iterates through sql/ and executes UNLOAD with queries on provided cluster

    @param unload_location: S3 bucket location for unloaded data
    @param iam_role: IAM ARN with unload permissions
    @param cluster: cluster dict
    @param user: str, master username for cluster
    @param path: replay path
    @return: str List, query file names
    """

    logger = logging.getLogger("SimpleReplayLogger")

    directory = r'sql/serverless'

    queries = []  # used to return query names
    with initiate_connection(username=user, cluster=cluster) as conn:  # initiate connection
        cursor = conn.cursor()
        logger.info(f"Querying {cluster.get('id')}. This may take some time.")

        for file in sorted(os.listdir(directory)):  # iterate local sql/ directory
            if not file.endswith('.sql'):  # validity check
                continue
            with open(f"{directory}/{file}", "r") as query_file:  # open sql file
                # get file name prefix for s3 files
                query_name = os.path.splitext(file)[0]  # get file/query name for reference
                logger.debug(f"Query: {query_name}")
                queries.append(query_name)
                query = query_file.read()  # read file contents as string

                # replace start and end times in sql with variables
                query = re.sub(r"{{START_TIME}}", f"'{cluster.get('start_time')}'", query)
                query = re.sub(r"{{END_TIME}}", f"'{cluster.get('end_time')}'", query)

                # format unload query with actual query from sql/
                unload_query = f"unload ($${query}$$) to '{unload_location.get('url')}/analysis/{replay}/raw_data/" \
                               f"{query_name}' iam_role '{iam_role}' CSV header allowoverwrite parallel off;"
                try:
                    cursor.execute(unload_query)  # execute unload
                except Exception as e:
                    logger.error(f"Could not unload {query_name} results. Confirm IAM permissions include UNLOAD "
                                 f"access for Redshift. {e}")
                    exit(-1)

    logger.info(f"Query results available in {unload_location.get('url')}")
    return queries


def get_raw_data(report, bucket, replay_path, query):
    """Reads and processes raw data from S3

    @param report: Report, report object
    @param bucket: dict, S3 bucket location
    @param replay_path: str, path of replay
    @param query: str, query name
    """

    logger = logging.getLogger("SimpleReplayLogger")
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket.get('bucket_name'), Key=f"{replay_path}/raw_data/{query}000")
    except Exception as e:
        logger.error(f"Unable to get raw data from S3. Results for {query} not found. {e}")
    df = pd.read_csv(response.get("Body")).fillna(0)
    logger.debug(f"Parsing results from '{query}' query.")
    if query == 'latency_distribution':
        report.feature_graph = df
    else:
        for t, vals in report.tables.items():
            if vals.get('sql') == query:
                vals['data'] = read_data(t, df, vals.get('columns'), report)


def read_data(table_name, df, report_columns, report):
    """Map raw data file to formatted table

    @param table_name: name of table
    @param df: DataFrame of raw data
    @param report_columns: List of column names for report table
    @param report: Report object
    @return: DataFrame of formatted data
    """

    logger = logging.getLogger("SimpleReplayLogger")

    if df.empty:
        logger.error("Data is empty. Failed to generate report.")
        exit(-1)
    cols = [g_columns[x] for x in report_columns]
    table_type = report.tables.get(table_name).get('type')

    report_table = None
    if table_type == 'breakdown':
        report_table = df[cols]
    elif table_type == 'metric':
        order = CategoricalDtype(['Query Latency', 'Compile Time', 'Queue Time', 'Execution Time',
                                  'Commit Queue Time', 'Commit Time'], ordered=True)
        df[g_columns.get('Measure')] = df[g_columns.get('Measure')].astype(order)
        frame = df.sort_values(g_columns.get('Measure'))
        report_table = frame[cols]
    elif table_type == 'measure':  # filter for specific measure type
        report_table = df[cols][df[g_columns.get("Measure")] == table_name]

    report_table = pd.DataFrame(report_table).round(2)  # round values in dataframe to thousandths place
    report_table.reindex(columns=report_columns)        # add columns names to dataframe

    # upload formatted dataframe to S3 as csv
    try:
        s3_resource = boto3.resource('s3')
        file = f"{table_name.replace(' ', '')}.csv"  # set filename for saving
        csv_buffer = StringIO()
        report_table.to_csv(csv_buffer)
        logger.debug(report.bucket)
        s3_resource.Object(report.bucket.get("bucket_name"),
                           f'{report.path}/aggregated_data/{file}').put(Body=csv_buffer.getvalue())
    except Exception as e:
        logger.error(f"Could not upload aggregated data. Please confirm bucket. Error occurred while processing "
                     f"data. {e}")
        exit(-1)
    return report_table


def create_presigned_url(bucket_name, object_name):
    """Creates a presigned url for a given object

    @param bucket_name: str, bucket name
    @param object_name: str, object name
    @return:
    """

    logger = logging.getLogger("SimpleReplayLogger")

    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=604800)
    except ClientError as e:
        logger.error(f"Unable to generate presigned url for object {object_name} in bucket {bucket_name}. {e}")
        return None

    return response


def analysis_summary(bucket_url, replay):
    """Print presigned url for report of given replay

    @param bucket_url: str, S3 bucket location
    @param replay: str, replay id
    """

    logger = logging.getLogger("SimpleReplayLogger")

    bucket = bucket_dict(bucket_url)
    logger.info(f"Simple Replay Workload Analysis: {replay}")
    replay_path = f'analysis/{replay}/out/'
    output_str = f"\nBelow is the presigned URLs for the analysis performed for replay: {replay}. " \
                 f"Click or copy/paste the link into your browser to download."
    r_url = create_presigned_url(bucket.get('bucket_name'), f'{replay_path}{replay}_report.pdf')
    output_str += f"\n\nReplay Analysis Report | Click to Download:\n{r_url}\n"
    logger.info(output_str)


def list_replays(bucket_url):
    """Iterates through S3 and aggregates list of successful replays

    @param bucket_url: str, S3 bucket location
    """

    logger = logging.getLogger("SimpleReplayLogger")

    table = []
    bucket = bucket_dict(bucket_url)
    try:
        resp = client("s3").list_objects_v2(Bucket=bucket.get('bucket_name'), Delimiter='/', Prefix='analysis/')
        if resp['KeyCount'] == 0:
            logger.error(f"No replays available in S3. Please run a replay with replay analysis to access replays "
                         f"from the command line.")
            exit(-1)

    except Exception as e:
        logger.error(f"Unable to access replays in S3. Please confirm bucket. {e}")
        exit(-1)

    s3 = boto3.resource('s3')
    print(f"Listed below are all the replay reports located in the S3 bucket: {bucket_url}.\n")

    for x in resp['CommonPrefixes']:
        try:
            s3.Object(bucket.get('bucket_name'), f'{x.get("Prefix")}out/info.json').load()
        except ClientError as e:
            if e.response['Error']['Code'] == "404":  # if info.json does not exist in folder, do not add to list
                continue
            else:
                logger.error(f"Unable to access replay. {e}")

        content_object = s3.Object(bucket.get('bucket_name'), f'{x.get("Prefix")}out/info.json')
        file_content = content_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)

        table.append([json_content['replay_id'],
                      json_content['id'],
                      json_content['start_time'],
                      json_content['end_time'],
                      json_content['replay_tag']])
    # use tabulate lib to format output
    print(tabulate(table, headers=["Replay", "Cluster ID", "Start Time", "End Time", "Replay Tag"]))


def list_sql(bucket_url, replay):
    """List presigned urls for raw_data/ files in S3 for a given replay

    @param bucket_url: str, S3 bucket location
    @param replay: str, replay id
    """

    logger = logging.getLogger("SimpleReplayLogger")
    bucket = bucket_dict(bucket_url)
    try:
        resp = client("s3").list_objects_v2(Bucket=bucket.get('bucket_name'),
                                            Delimiter='/',
                                            Prefix=f'analysis/{replay}/raw_data/')
    except Exception as e:
        logger.error(f"Unable to access raw data in S3. Please confirm bucket. {e}")
        exit(-1)
    output_str = f"Below are presigned URLs for the raw data for replay id: {replay}. " \
                 f"Click or copy/paste the link into your browser to download."
    for x in resp['Contents']:
        try:
            prefix = x.get('Key')
            d_url = create_presigned_url(bucket.get('bucket_name'), x.get('Key'))
            query_result = prefix.split('/')[-1]
            output_str += f"\nQuery Results: {query_result}\n{d_url}"
        except Exception as e:
            logger.error(f"Unable to access raw data in S3. Please confirm bucket. {e}")

    logger.info(output_str)


def main():
    logger = init_logging()

    parser = argparse.ArgumentParser(
        description="This script analyzes a Redshift cluster and outputs a summary report with statistics"
                    "its performance."
    )

    parser.add_argument('-b', '--bucket', nargs=1, type=str, help='location of replay outputs')
    parser.add_argument('-r1', '--replay_id1', nargs='?', type=str, default='', help='replay id 1')
    parser.add_argument('-r2', '--replay_id2', nargs='?', type=str, default='', help='replay id 2, required for '
                                                                                     'comparison')
    parser.add_argument('-s', '--sql', action='store_true', help='sql')

    args = parser.parse_args()

    if not (args.bucket or args.replay_id1 or args.replay_id2):
        launch_analysis_v2()
    elif args.bucket and not (args.replay_id1 or args.replay_id2):
        list_replays(args.bucket[0])
    elif args.bucket and args.replay_id1 and not args.replay_id2:
        if args.sql:
            list_sql(args.bucket[0], args.replay_id1)
        else:
            analysis_summary(args.bucket[0], args.replay_id1)
    elif args.bucket and args.replay_id1 and args.replay_id2:
        if args.replay_id1 == args.replay_id2:
            logger.error("Cannot compare same replay, please choose two distinct replay ids.")
            exit(-1)
        else:
            print(f"Compare replays {args.replay_id1} and {args.replay_id2}.")
            run_comparison_analysis(args.bucket[0], args.replay_id1, args.replay_id2)
    else:
        print("Please enter valid arguments.")
        exit(-1)


if __name__ == '__main__':
    main()
