import re
from datetime import datetime, timezone

import boto3
import json
from urllib.parse import urlparse

import botocore
import pandas as pd
from boto3 import client
from botocore.exceptions import *


def bucket_dict(bucket_url):
    bucket, path = None, None
    try:
        parsed = urlparse(bucket_url)
        bucket, path = parsed.netloc, parsed.path
    except ValueError as e:
        print(f'{e}\nPlease enter a valid S3 url following one of the following style conventions: '
              f'S3://bucket-name/key-name')
        exit(-1)
    if path.startswith('/'):
        path = path[1:]
    if not path == '' and not path.endswith('/'):
        path = f"{path}/"
    if path == 'replays/':
        path = ''
    return {'url': bucket_url,
            'bucket_name': bucket,
            'prefix': path}


def list_replays(bucket_url, session):
    """Iterates through S3 and aggregates list of successful replays
    @param bucket_url: str, S3 bucket location
    """
    table = []
    bucket = bucket_dict(bucket_url)
    try:
        if not session:
            resp = client("s3").list_objects_v2(Bucket=bucket.get('bucket_name'), Delimiter='/', Prefix='analysis/')
        else:
            resp = session.client("s3").list_objects_v2(Bucket=bucket.get('bucket_name'), Delimiter='/', Prefix='analysis/')
        if resp['KeyCount'] == 0:
            print(f"No replays available in S3. Please run a replay with replay analysis to access replays "
                  f"from the command line.")
            return None, table

    except Exception as e:
        return None, e
    except botocore.errorfactory.NoSuchBucket as e:
        return None, e

    s3 = boto3.resource('s3')

    for x in resp['CommonPrefixes']:
        try:
            s3.Object(bucket.get('bucket_name'), f'{x.get("Prefix")}info.json').load()
            content_object = s3.Object(bucket.get('bucket_name'), f'{x.get("Prefix")}info.json')
            file_content = content_object.get()['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
            json_content["bucket"] = bucket.get('bucket_name')
            table.append(json_content)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":  # if info.json does not exist in folder, do not add to list
                continue
            else:
                print(f"Unable to access replay. {e}")

    # use tabulate lib to format output
    return bucket.get("bucket_name"), table


def remove_comments(string):
    pattern = r"(\".*?\"|\'.*?\')|(/\*.*?\*/|//[^\r\n]*$)"
    # first group captures quoted strings (double or single)
    # second group captures comments (//single-line or /* multi-line */)
    regex = re.compile(pattern, re.MULTILINE|re.DOTALL)
    def _replacer(match):
        # if the 2nd group (capturing comments) is not None,
        # it means we have captured a non-quoted (real) comment string.
        if match.group(2) is not None:
            return "" # so we will return empty to remove the comment
        else: # otherwise, we will return the 1st group
            return match.group(1) # captured quoted-string
    return regex.sub(_replacer, string)


def hash_query(st):
    # first group captures quoted strings (double or single)
    # second group captures comments (//single-line or /* multi-line */)
    if "xid" in st:
        return hash(st[(st.find("xid")):(st.find("replay_start") - 3)])

    return 0


def calc_diff(replay_start, timestamp):
    if len(timestamp.split(".")[-1]) < 6:
        for i in range(6 - len(timestamp.split(".")[-1])):
            timestamp = timestamp + "0"

    start = datetime.fromisoformat(replay_start)
    stamp = datetime.fromisoformat(timestamp)

    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)

    return ((stamp - start).total_seconds()) * 1000


def filter_data(data, replay, query_types=None, users=None, duration=None):
    if users is None:
        users = []
    if query_types is None:
        query_types = []

    replay_data = data.loc[data['sid'] == replay['sid']]

    if len(users) != 0:
        replay_data = replay_data.loc[replay_data['user_name'].isin(users)]
    if len(query_types) != 0:
        replay_data = replay_data.loc[replay_data['query_type'].isin(query_types)]

    if duration is not None and duration != [0, 0]:
        if {'start_diff', 'end_diff'}.issubset(data.columns):
            start = replay_data[replay_data['start_diff'].between(duration[0], duration[1])]
            end = replay_data[replay_data['end_diff'].between(duration[0], duration[1])]
            replay_data = pd.concat([start, end], join="inner", ignore_index=True).drop_duplicates()

        elif {'time_diff'}.issubset(data.columns):
            replay_data = replay_data[replay_data['start_diff'].between(duration[0], duration[1])]

        else:
            start_x = replay_data[replay_data['start_diff_x'].between(duration[0], duration[1])]
            start_y = replay_data[replay_data['start_diff_y'].between(duration[0], duration[1])]
            end_x = replay_data[replay_data['end_diff_x'].between(duration[0], duration[1])]
            end_y = replay_data[replay_data['end_diff_y'].between(duration[0], duration[1])]
            replay_data = pd.concat([start_x, start_y, end_x, end_y], join="inner", ignore_index=True).drop_duplicates()

    return replay_data
