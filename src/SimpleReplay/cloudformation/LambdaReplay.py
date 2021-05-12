import boto3
import time
import traceback
import botocore.exceptions as be
import json

def handler(event, context):
    print(event)
    action = event['Input'].get('action')
    instance_id = event['Input'].get('instance_id')
    extract_bucket = event['Input'].get('extract_bucket')
    replay_bucket = event['Input'].get('replay_bucket')
    command_id = event['Input'].get('command_id')
    iam_role = event['Input'].get('iam_role')
    security_group_id = event['Input'].get('security_group_id')
    subnet_group = event['Input'].get('subnet_group')
    extract_account = event['Input'].get('extract_account')
    db = event['Input'].get('db')
    user = event['Input'].get('user')
    node_type = event['Input'].get('node_type')
    number_of_nodes = event['Input'].get('number_of_nodes')

    parameter_group_name = event['Input'].get('parameter_group_name')
    sql_id = event['Input'].get('sql_id')
    source_clusterid = event['Input'].get('source_clusterid')
    cluster_type = event['Input'].get('cluster_type')
    clusterid = source_clusterid + "-" + cluster_type if cluster_type else source_clusterid
    snapshotid = "ra3-migration-evaluation-snapshot-" + source_clusterid if source_clusterid else ""

    try:
        client = boto3.client('redshift')
        extract_prefix = json.loads(get_config_from_s3(extract_bucket, 'config/extract_prefix.json')) if extract_bucket else {'prefix':'','extract_output':''}
        prefix = extract_prefix['prefix']
        extract_output = extract_prefix['extract_output']
        if action == "cluster_status":
            res = {'status': cluster_status(client, clusterid)}
        elif action == "update_parameter_group":
            res = {'status': update_parameter_group(client, parameter_group_name, extract_bucket)}
        elif action == "create_cluster":
            res = {
                'status': create_cluster(client, clusterid, snapshotid, extract_bucket, iam_role, parameter_group_name,
                                         subnet_group, security_group_id, extract_account, node_type, number_of_nodes)}
        elif action =="classic_resize_cluster":
            res = {'status': classic_resize_cluster(client, clusterid, node_type, number_of_nodes)}
        elif action == "resume_cluster":
            client.resume_cluster(ClusterIdentifier=clusterid)
            res = {'status': 'initiated'}
        elif action == "pause_cluster":
            client.pause_cluster(ClusterIdentifier=clusterid)
            res = {'status': 'initiated'}
        elif action == "setup_redshift_objects":
            res = {'sql_id': setup_redshift_objects(replay_bucket, clusterid, db, user)}
        elif action == "run_replay":
            command_id = run_replay(client, replay_bucket, instance_id, clusterid, cluster_type, prefix, extract_output)
            res = {'command_id': command_id}
        elif action == "replay_status":
            res = {'status': replay_status(command_id, instance_id)}
        elif action == "unload_stats":
            script = "call unload_detailed_query_stats('" + prefix + "')"
            sql_id = run_sql(clusterid, db, user, script)
            res = {'sql_id': sql_id}
        elif action == "load_stats":
            script = "call load_detailed_query_stats('" + prefix + "')"
            sql_id = run_sql(clusterid, db, user, script, False, 'async')
            res = {'sql_id': sql_id}
        elif action == "sql_status":
            res = {'status': sql_status(sql_id)}
        else:
            raise ValueError("Invalid Task: " + action)
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        raise
    print(res)
    return res

def cluster_status(client, clusterid):
    try:
        desc = client.describe_clusters(ClusterIdentifier=clusterid)['Clusters'][0]
        if isinstance(desc, dict):
            status = desc.get('ClusterStatus') + desc.get('ClusterAvailabilityStatus') + (desc.get('RestoreStatus').get('Status') if desc.get('RestoreStatus') else "")
        else:
            status = 'Unavailable'
    except be.ClientError as e:
        msg = e.response['Error']['Code']
        if msg == 'ClusterNotFound':
            status = 'nonExistent'
        else:
            print(desc)
            raise
    return status

def get_config_from_s3(bucket, key):
    obj = boto3.client('s3').get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')

def update_parameter_group(client, parameter_group_name, bucket):
    key = "config/parameter_group.json"
    target_parameter_group = client.describe_cluster_parameters(ParameterGroupName=parameter_group_name)[
        "Parameters"]
    target_parameters = {}
    for i in target_parameter_group:
        target_parameters[i['ParameterName']] = i['ParameterValue']
    source_parameter_group = json.loads(get_config_from_s3(bucket, key))["Parameters"]
    modified_parameter_group = []
    for i in source_parameter_group:
        source_parameter_value = i['ParameterValue'].replace(" ", "")
        target_parameter_value = target_parameters[i['ParameterName']].replace(" ", "")
        if source_parameter_value != target_parameter_value:
            modified_parameter_group.append(i)
    if modified_parameter_group:
        client.modify_cluster_parameter_group(
            ParameterGroupName=parameter_group_name,
            Parameters=modified_parameter_group)
    return "Initiated"

def classic_resize_cluster(client, clusterid, node_type, number_of_nodes):
    client.resize_cluster(ClusterIdentifier=clusterid, NodeType=node_type, NumberOfNodes=int(number_of_nodes),
                          ClusterType='single-node' if int(number_of_nodes) == 1 else 'multi-node', Classic=True)
    return "Initiated"

def create_cluster(client, clusterid, snapshotid, bucket, iam_role, parameter_group_name, subnet_group,
                   security_group_id, extract_account, node_type="", number_of_nodes=""):
    key = "config/config.json"
    source_parameter_group = json.loads(get_config_from_s3(bucket, key))
    source_node_type = source_parameter_group["node_type"]
    source_number_of_nodes = source_parameter_group["number_of_nodes"]
    if not node_type:
        node_type = source_node_type
        number_of_nodes = source_number_of_nodes

    try:
        client.restore_from_cluster_snapshot(NumberOfNodes=int(number_of_nodes),
                                             NodeType=node_type,
                                             ClusterIdentifier=clusterid,
                                             SnapshotIdentifier=snapshotid,
                                             OwnerAccount=extract_account,
                                             Port=int(source_parameter_group["port"]),
                                             ClusterSubnetGroupName=subnet_group,
                                             PubliclyAccessible=source_parameter_group[
                                                                    "publicly_accessible"] == 'True',
                                             ClusterParameterGroupName=parameter_group_name,
                                             VpcSecurityGroupIds=[security_group_id],
                                             EnhancedVpcRouting=source_parameter_group[
                                                                    "enhanced_vpc_routing"] == 'True',
                                             IamRoles=[iam_role])
        status = 'Initiated'
    except be.ClientError as e:
        msg = e.response['Error']['Code']
        if msg == 'ClusterAlreadyExists':
            status = msg
        elif msg == 'InvalidParameterValue':
            client.restore_from_cluster_snapshot(NumberOfNodes=int(source_number_of_nodes),
                                                 NodeType=source_node_type,
                                                 ClusterIdentifier=clusterid,
                                                 SnapshotIdentifier=snapshotid,
                                                 OwnerAccount=extract_account,
                                                 Port=int(source_parameter_group["port"]),
                                                 ClusterSubnetGroupName=subnet_group,
                                                 PubliclyAccessible=source_parameter_group[
                                                                        "publicly_accessible"] == 'True',
                                                 ClusterParameterGroupName=parameter_group_name,
                                                 VpcSecurityGroupIds=[security_group_id],
                                                 EnhancedVpcRouting=source_parameter_group[
                                                                        "enhanced_vpc_routing"] == 'True',
                                                 IamRoles=[iam_role])
            status = 'NeedClassicResize'
        else:
            raise
    return status


def setup_redshift_objects(bucket, clusterid, db, user):
    key = 'config/setup_redshift_objects.sql'
    script = get_config_from_s3(bucket, key)
    sql_id = run_sql(clusterid, db, user, script)
    return sql_id


def run_sql(clusterid, db, user, script, with_event=True, run_type='sync'):
    res = boto3.client("redshift-data").execute_statement(Database=db, DbUser=user, Sql=script,
                                                          ClusterIdentifier=clusterid, WithEvent=with_event)
    query_id = res["Id"]
    statuses = ["STARTED", "FAILED", "FINISHED"] if run_type == 'async' else ["FAILED", "FINISHED"]
    done = False
    while not done:
        status = sql_status(query_id)
        if status in statuses:
            break
    return query_id


def sql_status(query_id):
    res = boto3.client("redshift-data").describe_statement(Id=query_id)
    status = res["Status"]
    if status == "FAILED":
        raise Exception('Error:' + res["Error"])
    return status.strip('"')


def run_replay(client, replay_bucket, instance_id, clusterid, cluster_type, prefix, extract_output):
    desc = client.describe_clusters(ClusterIdentifier=clusterid)['Clusters'][0]
    cluster_endpoint = desc.get('Endpoint') .get('Address') + ":" + str(desc.get('Endpoint') .get('Port')) + "/" + desc.get('DBName')
    command = "sh /amazonutils/amazon-redshift-utils/src/SimpleReplay/cloudformation/run_replay.sh " + prefix + " " + extract_output + " " + cluster_type + " " + cluster_endpoint
    print(command)
    response = boto3.client('ssm').send_command(InstanceIds=[instance_id],
                                                DocumentName='AWS-RunShellScript',
                                                OutputS3BucketName=replay_bucket,
                                                OutputS3KeyPrefix=prefix,
                                                TimeoutSeconds = 86400, # 1 day
                                                Parameters={'commands': [command], 'executionTimeout': ['86400']})
    command_id = response['Command']['CommandId']
    return command_id


def replay_status(command_id, instance_id):
    result = boto3.client('ssm').get_command_invocation(CommandId=command_id, InstanceId=instance_id)
    if result['Status'] == "Failed":
        raise Exception('Error:' + result['StandardErrorContent'])
    elif result['Status'] == "TimedOut":
        print(result)
        raise Exception('Error: exceeded one day time out')
    return result['Status']
