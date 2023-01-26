from string import ascii_uppercase

import numpy as np
import pandas as pd
from flask import *
from pandas.errors import EmptyDataError

from utils import *
from utils import remove_comments

app = Flask(__name__)

# global variables maintained during each analysis session
boto3_session = None            # boto3 session object for making requests
selected_replays = None         # List of dictionaries, each replay dict = replay/info.json from S3
selected_query_data = None      # Pandas dataframe, with sysqueryhistory data compiled from each selected replay
selected_error_data = None      # Pandas dataframe, with replayerrors data compiled from each selected replay
selected_copy_data = None       # df with sysloadhistory data joined with sysqueryhistory from each selected replay


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    return response


def request_s3_data(filename):
    """Iterates through selected replays to compile data results of given file in one data frame
    Analysis uses replayerrors000, sys_query_history000, and sys_load_history000 as of 08/08/22
    @param filename: str, filename of unloaded sys view data in S3
    """

    global boto3_session
    s3_client = boto3_session.client('s3')

    global selected_replays
    if not selected_replays:
        return None

    df = pd.DataFrame()

    for replay in selected_replays:
        temp = None     # temporary df for individual replay
        try:
            response = s3_client.get_object(Bucket=replay['bucket'],
                                            Key=f"analysis/{replay['replay_id']}/raw_data/{filename}")
            temp = pd.read_csv(response.get("Body")).fillna(0)
        except Exception as e:
            temp = pd.DataFrame()

        temp['sid'] = replay['sid']     # associate short id with entries in df

        # calculate relative execution timestamp to replay start for server side filtering, in milliseconds
        if {'start_time', 'end_time'}.issubset(temp.columns):
            temp["start_diff"] = temp['start_time'].apply(lambda x: calc_diff(replay['start_time'], x)).astype(np.int64)
            temp["end_diff"] = temp['end_time'].apply(lambda x: calc_diff(replay['start_time'], x)).astype(np.int64)
        elif {'timestamp'}.issubset(temp.columns):
            temp["time_diff"] = temp['timestamp'].apply(lambda x: calc_diff(replay['start_time'], x)).astype(np.int64)

        df = pd.concat([df, temp], ignore_index=True)

    for i in ['elapsed_time', 'queue_time', 'execution_time']:    # convert microseconds to milliseconds
        if {i}.issubset(df.columns):
            df[i] = df[i].apply(lambda x: x / 1000)

    if {'query_text'}.issubset(df.columns):
        if "query" in filename:     # filter out non-replay executed statements
            df = df[df['query_text'].str.contains("replay_start")]
        df['query_hash'] = df['query_text'].apply(lambda x: hash_query(x))
        df['query_text'] = df['query_text'].apply(lambda x: remove_comments(x))

    if "load" in filename and selected_query_data is not None:
        df = pd.merge(left=df, right=selected_query_data, on=['query_id', 'sid'], how='inner')

    # export df to csv locally for debugging
    df.to_csv(f"{filename}.csv")

    return df


@app.route('/getprofile')
def profiles():
    # clear global variables on home page load for new analysis
    global selected_replays
    global boto3_session
    global selected_query_data
    global selected_error_data
    boto3_session = None
    selected_replays = None
    selected_query_data = None
    selected_error_data = None

    profile_session = boto3.Session()
    return jsonify({"success": True, "profiles": profile_session.available_profiles}), 201


@app.route('/profile')
def use_profile():
    global boto3_session
    boto3_session = boto3.Session(profile_name=request.args.get("name"))

    return jsonify({"success": True}), 201


@app.route('/role')
def assume_role():
    global boto3_session

    sts_client = boto3.client('sts')
    try:
        response = sts_client.assume_role(
            RoleArn=request.args.get("arn"),
            RoleSessionName='SimpleReplayAnalysisSession'
        )
    except Exception as e:
        return jsonify({"success": False, "message": e.__str__()}), 403

    credentials = response['Credentials']

    boto3_session = boto3.Session(aws_access_key_id=credentials['AccessKeyId'],
                                  aws_secret_access_key=credentials['SecretAccessKey'],
                                  aws_session_token=credentials['SessionToken'], )
    return jsonify({"success": True}), 201


@app.route('/search', methods=['GET', 'POST'])
def search_s3():
    global boto3_session
    if not boto3_session:
        boto3_session = boto3.Session()

    if request.method == 'GET':
        bucket = request.args.get("uri")
        bucket, data = list_replays(bucket, session=boto3_session)
        if bucket is None:
            if data is ClientError:
                return jsonify({"success": False, "message": data.__str__()}), 403
            else:
                return jsonify({"success": False, "message": data.__str__()}), 404

        return jsonify({"success": True, "bucket": bucket, "replays": data}), 201


@app.route('/submit_replays', methods=['GET', 'POST'])
def replays():
    global selected_replays

    if request.method == 'POST':
        if len(request.json) == 0:
            return jsonify({"success": True, "message": "No replays selected."}), 400
        # request.json is a list of dicts
        selected_replays = request.json
        return jsonify({"success": True, "message": "Replays saved."}), 201

    if request.method == 'GET':
        if selected_replays:
            selected_replays = sorted(selected_replays, key=lambda x: x['replay_id'])

            for i, replay in enumerate(selected_replays):
                replay['sid'] = "Replay " + ascii_uppercase[i]

            return jsonify({"success": True, "replays": selected_replays}), 201

    return jsonify({"success": True, "replays": selected_replays}), 201


@app.route('/time_range')
def time_range():
    global selected_replays
    global selected_query_data

    if selected_replays is None:
        return jsonify({"success": False}), 400

    if selected_query_data is None:
        selected_query_data = request_s3_data("sys_query_history000")

    users = selected_query_data["user_name"].unique()
    time_max = 0
    for replay in selected_replays:
        dur_s = sum(x * int(t) for x, t in zip([3600, 60, 1], replay['duration'].split(":")))
        if dur_s > time_max:
            time_max = dur_s
    return jsonify({"success": False, "time": time_max * 1000, "users": users.tolist()}), 201


@app.route('/compare_throughput')
def compare_throughput():
    global selected_replays
    global selected_query_data
    global boto3_session

    if not selected_replays:
        return jsonify({"success": False}), 400
    if selected_query_data is None:
        selected_query_data = request_s3_data("sys_query_history000")

    q_types = [d['label'] for d in json.loads(request.args.get('qtype'))]
    users = [d['label'] for d in json.loads(request.args.get('user'))]
    # duration = [int(request.args.get('start')), int(request.args.get('end'))]

    replay_throughput = []

    for replay in selected_replays:

        replay_data = filter_data(selected_query_data, replay, q_types, users)

        replay_data["time"] = replay_data['end_diff'].apply(lambda x: 1000*int(x/1000))
        if not replay_data.empty:
            data = {'rel_time': replay_data.groupby(by="time").size().index,
                    'freq': replay_data.groupby(by="time").size().values}
            tp = pd.DataFrame(data)
            replay_throughput.append({'replay': replay['sid'], 'values': json.loads(tp.to_json(orient="records"))})

    return jsonify({"success": True,
                    "data": replay_throughput
                    }), 201


@app.route('/agg_metrics')
def agg_metrics():
    global selected_query_data
    global selected_replays

    q_types = [d['label'] for d in json.loads(request.args.get('qtype'))]
    users = [d['label'] for d in json.loads(request.args.get('user'))]
    duration = [int(request.args.get('start')),int(request.args.get('end'))]

    if selected_query_data is None:
        selected_query_data = request_s3_data("sys_query_history000")

    metrics = pd.DataFrame(columns=['sid', 'p25', 'p50', 'p75', 'p99', 'avg', 'std'])

    for replay in selected_replays:
        replay_data = filter_data(selected_query_data, replay, q_types, users, duration)
        entry = {'sid': replay['sid'],
                 'p25': replay_data['execution_time'].quantile(q=0.25),
                 'p50': replay_data['execution_time'].quantile(q=0.50),
                 'p75': replay_data['execution_time'].quantile(q=0.75),
                 'p99': replay_data['execution_time'].quantile(q=0.99),
                 'avg': replay_data['execution_time'].mean(),
                 'std': replay_data['execution_time'].std()}

        metrics = metrics.append(entry, ignore_index=True)

    return jsonify({"success": True, "data": json.loads(metrics.to_json(orient="records"))}), 201


@app.route('/query_latency')
def query_latency():
    q_types = [d['label'] for d in json.loads(request.args.get('qtype'))]
    users = [d['label'] for d in json.loads(request.args.get('user'))]
    # duration = [int(request.args.get('start')), int(request.args.get('end'))]

    global selected_query_data
    global selected_replays

    if selected_query_data is None:
        selected_query_data = request_s3_data("sys_query_history000")

    all_bins = []
    all_hist = []
    df = None
    for replay in selected_replays:
        df = filter_data(selected_query_data, replay, q_types, users)
        hist, bins = np.histogram(a=df['elapsed_time'].to_numpy(), bins=3)
        all_bins.append(bins)
        all_hist.append({"replay": replay['sid'], "counts": hist})

    avg_bins = pd.DataFrame(data=all_bins).mean().to_numpy().astype(int)
    bar_chart = []
    for i, entry in enumerate(all_hist):
        values = []
        for j, count in enumerate(entry['counts']):
            values.append({"count": int(count), "bin": int(avg_bins[j])})
        bar_chart.append({'replay': entry['replay'], 'values': values})

    return jsonify({"success": True, "data": bar_chart}), 201


@app.route('/top_queries')
def top_queries():
    global selected_query_data

    if selected_query_data is not None:
        selected_query_data = selected_query_data.sort_values(by="elapsed_time", ascending=False)
        return jsonify({"success": True, "data": json.loads(selected_query_data.to_json(orient="records"))}), 201

    df = request_s3_data("sys_query_history000")

    if df is not None:
        df = df.sort_values(by="elapsed_time", ascending=False)

        return jsonify({"success": True, "data": json.loads(df.to_json(orient="records"))}), 201

    return jsonify({"success": False}), 400


@app.route('/perf_diff')
def perf_diff():
    baseline = request.args.get('baseline')
    target = request.args.get('target')

    global selected_query_data
    if selected_query_data is None:
        selected_query_data = request_s3_data("sys_query_history000")


    baseline_data = selected_query_data.loc[selected_query_data['sid'] == baseline]
    baseline_data = baseline_data[baseline_data['query_hash'] != 0]
    target_data = selected_query_data.loc[selected_query_data['sid'] == target]
    target_data = target_data[target_data['query_hash'] != 0]

    df = pd.merge(left=baseline_data, right=target_data, on='query_hash', how='inner', suffixes=('_b', '_t'))

    df['elapsed_diff'] = ((df['elapsed_time_b'] - df['elapsed_time_t']) / df['elapsed_time_b']) * 100
    df['exec_diff'] = ((df['execution_time_b'] - df['execution_time_t']) / df['execution_time_b']) * 100
    df = df.reindex(df['elapsed_diff'].abs().sort_values(ascending=False).index)

    return jsonify({"success": True, "data": json.loads(df.to_json(orient="records"))}), 201


@app.route('/err_table')
def err_table():
    global selected_error_data

    if selected_error_data is not None:
        return jsonify({"success": True, "data": json.loads(selected_error_data.to_json(orient="records"))}), 201

    df = request_s3_data("replayerrors000")
    if df is not None:
        return jsonify({"success": True, "data": json.loads(df.to_json(orient="records"))}), 201

    return jsonify({"success": False}), 400


@app.route('/err_distribution')
def err_distribution():
    global selected_replays
    global boto3_session

    if selected_replays is None:
        return jsonify({"success": False}), 400

    replay_throughput = []
    x_max = 0
    y_max = 0
    s3_client = boto3_session.client('s3')

    for i in selected_replays:

        try:
            response = s3_client.get_object(Bucket=i['bucket'],
                                            Key=f"analysis/{i['replay_id']}/raw_data/replayerrors000")
        except Exception as e:
            continue
        try:
            df = pd.read_csv(response.get("Body")).fillna(0)
        except EmptyDataError:
            df = pd.DataFrame()

        if not df.empty:
            data = {'category': df.groupby(by="category").size().index,
                    'freq': df.groupby(by="category").size().values
                    }
            tp = pd.DataFrame(data)

            replay_throughput.append({'replay': i['sid'], 'values': json.loads(tp.to_json(orient="records"))})

    return jsonify({"success": True,
                    "data": replay_throughput,
                    'xmax': x_max,
                    'ymax': int(y_max)
                    }), 201


@app.route('/copy_agg')
def copy_agg():
    global selected_copy_data
    global selected_replays
    global selected_query_data

    users = [d['label'] for d in json.loads(request.args.get('user'))]
    duration = [int(request.args.get('start')), int(request.args.get('end'))]

    if selected_query_data is None:
        selected_query_data = request_s3_data("sys_query_history000")
    if selected_copy_data is None:
        selected_copy_data = request_s3_data("sys_load_history000")

    if selected_copy_data.empty:
        return jsonify({"success": True, "data": []}), 201

    metrics = pd.DataFrame(columns=['sid', 'loaded_rows', 'loaded_bytes', 'source_file_count'])

    for replay in selected_replays:
        df = filter_data(selected_copy_data, replay, users=users, duration=duration)

        if df.empty:
            continue

        entry = {'sid': replay['sid'],
                 'loaded_rows': df['loaded_rows'].sum(),
                 'loaded_bytes': df['loaded_bytes'].sum(),
                 'source_file_count': df['source_file_count'].sum()}

        metrics = metrics.append(entry, ignore_index=True)

    return jsonify({"success": True, "data": json.loads(metrics.to_json(orient="records"))}), 201


@app.route('/copy_diff')
def copy_diff():
    global selected_query_data
    global selected_copy_data
    global selected_replays

    if selected_query_data is None:
        selected_query_data = request_s3_data("sys_query_history000")
    if selected_copy_data is None:
        selected_copy_data = request_s3_data("sys_load_history000")

    if selected_copy_data.empty:
        return jsonify({"success": True, "data": []}), 201

    baseline = request.args.get('baseline')
    target = request.args.get('target')

    baseline_data = selected_copy_data.loc[selected_copy_data['sid'] == baseline]
    baseline_data = baseline_data[baseline_data['query_hash'] != 0]
    target_data = selected_copy_data.loc[selected_copy_data['sid'] == target]
    target_data = target_data[target_data['query_hash'] != 0]

    if baseline_data.empty or target_data.empty:
        return jsonify({"success": True, "data": []}), 201

    df = pd.merge(left=baseline_data, right=target_data, on='query_hash', how='outer', suffixes=('_b', '_t'))

    df.to_csv("help.csv")

    return jsonify({"success": True, "data": json.loads(df.to_json(orient="records"))}), 201


if __name__ == '__main__':
    app.run(debug=True)
