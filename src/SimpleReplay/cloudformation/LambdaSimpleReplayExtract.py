import boto3
import time
import traceback
import botocore.exceptions as be
import json
def handler(event,context):
    print(event)
    action = event['Input'].get('action')
    instance_id = event['Input'].get('instance_id')
    bucket = event['Input'].get('bucket')
    start_time = event['Input'].get('start_time')
    end_time = event['Input'].get('end_time')
    command_id = event['Input'].get('command_id')
    account = event['Input'].get('account')
    endpoint = event['Input'].get('endpoint')
    clusterid= endpoint.split('.')[0] if endpoint else ""
    snapshotid = "ra3-migration-evaluation-snapshot-" + clusterid
    PREFIX = 'config/'
    client = boto3.client('redshift')
    try:
        if action == "cluster_status":
            res = {'status': cluster_status(client, clusterid)}
        elif action == "create_snapshot":
            res = {'snapshotid': create_snapshot(client, clusterid,snapshotid)}
        elif action == "snapshot_status":
            r = client.describe_cluster_snapshots(SnapshotIdentifier=snapshotid)
            res = {'status': r['Snapshots'][0]['Status']}
        elif action == "authorize_snapshot":
            res = {'status': authorize_snapshot(account, client, snapshotid) if account!='N/A' else account}
        elif action == "upload_params":
            res = upload_params(client, endpoint,snapshotid,bucket,PREFIX)
        elif action == "run_extract":
            extract_prefix = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(time.time()))
            cmd = "sh /amazonutils/amazon-redshift-utils/src/SimpleReplay/cloudformation/run_extract.sh " + extract_prefix + " " + start_time + " " + end_time
            print(cmd)
            command_id = run_shell_command(bucket, cmd, extract_prefix + "/extract_logs", instance_id)
            res = {'command_id': command_id}
        elif action == "command_status":
            res = {'status': shell_command_status(command_id, instance_id)}
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
            status = desc.get('ClusterStatus') + desc.get('ClusterAvailabilityStatus')
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

def run_shell_command(bucket, extract_command, extract_prefix, instance_id):
    response = boto3.client('ssm').send_command(InstanceIds=[instance_id],DocumentName='AWS-RunShellScript',OutputS3BucketName=bucket,OutputS3KeyPrefix=extract_prefix,Parameters={'commands': [extract_command]})
    command_id = response['Command']['CommandId']
    return command_id

def shell_command_status(command_id, instance_id):
    result = boto3.client('ssm').get_command_invocation(CommandId=command_id,InstanceId=instance_id)
    if result['Status'] == "Failed":
        raise Exception('Error:' + result['StandardErrorContent'])
    return result['Status']

def upload_params(client, endpoint,snapshotid,bucket,prefix):

  port, db = endpoint.split(':')[1].split('/')
  cluster = client.describe_clusters(ClusterIdentifier=endpoint.split('.')[0])['Clusters'][0]
  security_groups = [item['VpcSecurityGroupId'] for item in cluster.get('VpcSecurityGroups')]
  parameter_group = cluster.get('ClusterParameterGroups')[0].get('ParameterGroupName')
  parameter_group_dict = client.describe_cluster_parameters(ParameterGroupName=parameter_group)
  s3_put(parameter_group_dict, bucket, prefix + "parameter_group.json")

  config_dict = {'availability_zone': cluster.get('AvailabilityZone'),
          'enhanced_vpc_routing': str(cluster.get('EnhancedVpcRouting')),
          'parameter_group': parameter_group,
          'port': port,
          'db':db,
          'publicly_accessible': str(cluster.get('PubliclyAccessible')),
          'security_groups': ','.join(security_groups),
          'snapshot_id': snapshotid,
          'cluster_subnet_group': cluster.get('ClusterSubnetGroupName'),
          'node_type': cluster.get('NodeType'),
          'number_of_nodes': str(cluster.get('NumberOfNodes'))
          }
  s3_put(config_dict, bucket, prefix + "config.json")
  return config_dict


def s3_put(dict_obj, bucket, key):
    s3 = boto3.client('s3')
    s3.put_object(Body=json.dumps(dict_obj), Bucket=bucket, Key=key)

def create_snapshot(client, clusterid, snapshotid):
    try:
        client.create_cluster_snapshot(
            ClusterIdentifier=clusterid,
            SnapshotIdentifier=snapshotid,
            ManualSnapshotRetentionPeriod=3
        )
    except be.ClientError as e:
        msg = e.response['Error']['Code']
        if msg == 'ClusterSnapshotAlreadyExists':
            print(msg)
        else:
            raise
    return snapshotid

def authorize_snapshot(account, client, snapshotid):
    try:
        client.authorize_snapshot_access(
            SnapshotIdentifier=snapshotid,
            AccountWithRestoreAccess=account)
    except be.ClientError as e:
        msg = e.response['Error']['Code']
        if msg=='AuthorizationAlreadyExists':
            print(msg)
        else:
            raise
    return snapshotid
