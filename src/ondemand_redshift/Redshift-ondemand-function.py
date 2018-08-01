import boto3
import time
from datetime import date , timedelta
import json
client = boto3.client('redshift')
def lambda_handler(event, context):
    # In case of deleting the cluster there will be 'action'
    # that will be passed in the event
    if 'action' in event:
        instance=event['Cluster']
        lamb = boto3.client('lambda')
        # In order to uniquely identify the snapshot
        # the date and time is appended to the snapshot name
        snapshotIdentifier = time.strftime(instance+'-%Y-%m-%d-%H-%M-%S');
        # Creating the manual snapshot and deleting the cluster
        response = client.delete_cluster(
                    ClusterIdentifier=instance,
                    SkipFinalClusterSnapshot=False,
                    FinalClusterSnapshotIdentifier=snapshotIdentifier
                )
        create_event = boto3.client('events')
        # Creating the event for restoring the cluster back from snapshot at the scheduled time 
        create_rule_response = create_event.put_rule(
            Name='Ondemand-createCluster-rule',
            ScheduleExpression='cron('+event['creationMin']+' '+ event['creationHour'] +' * * ? *)',
            State='ENABLED'
        )
        lamb = boto3.client('lambda')
        response_ARN=lamb.get_function(FunctionName='Redshift-ondemand-function')
        response_ARN=(response_ARN['Configuration']['FunctionArn'])
        create_rule_responseARN = create_rule_response['RuleArn']
        try:
            lamb.add_permission(FunctionName='Redshift-ondemand-function',StatementId='Ondemand_create_redshift',Action='lambda:InvokeFunction',Principal='events.amazonaws.com',SourceArn=create_rule_responseARN)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                print('Skipped adding permission to lambda . StatementId "Ondemand_create_redshift" already exist')
            else:
                print ("Unexpected error: %s") % e
        # Creating the target to trigger the Lambda function at scheduled time with necessary parameters retrieved while deleting the cluster
        create_event.put_targets(
            Rule='Ondemand-createCluster-rule',
            Targets=[
                {
                    'Id': 'MyCloudWatchEventsTargetCreate',
                    'Arn': response_ARN,
                    'Input': json.dumps(response, default=str)
                }
            ]
        )
        print('Creating the snapshot and then deleting the cluster')
    # This block will be executed to restore the cluster from snapshot
    else:
        instance=event['Cluster']['ClusterIdentifier']
        count=0;
        snapshots = client.describe_cluster_snapshots(
        ClusterIdentifier=instance,
        SnapshotType = 'manual'
        );
        snapshotID=instance
        arnList=[];
        lamb = boto3.client('lambda')
        for roleArn in event['Cluster']['IamRoles']:
            arnList.append(roleArn['IamRoleArn'])
        #Retrieving the last manual snapshot created from cluster and then restoring the cluster from snapshot
        #passing in necessary parameters like ClusterSubnetGroupName, Publically accessible etc
        #that we retrieved while we were deleting the cluster and saving in the put_target 'Input' parameter
        for snapshot in snapshots['Snapshots']:
            if(snapshot['SnapshotIdentifier'].startswith(snapshotID)):
                response = client.restore_from_cluster_snapshot(
                ClusterIdentifier=instance,
                SnapshotIdentifier=snapshot['SnapshotIdentifier'],
                ClusterSubnetGroupName=event['Cluster']['ClusterSubnetGroupName'],
                PubliclyAccessible=event['Cluster']['PubliclyAccessible'],
                EnhancedVpcRouting=event['Cluster']['EnhancedVpcRouting'],
                ClusterParameterGroupName=event['Cluster']['ClusterParameterGroups'][0]['ParameterGroupName'],
                IamRoles=arnList
                )
                count=count+1;
                print('Creating the cluster from snapshot')
                break;
        if(count==0):
            print('No snapshot found or there was some error while trying to create cluster')
