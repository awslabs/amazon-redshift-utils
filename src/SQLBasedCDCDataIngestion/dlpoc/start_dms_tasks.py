import boto3
cf_client = boto3.client("cloudformation")
dms_client = boto3.client("dms")

def get_dms_task_arn():
    """
    get arns of dms tasks created by cdk
    """
    response = cf_client.describe_stacks(
        StackName='DMS'
        )
    dms_outputs=response['Stacks'][0]['Outputs']
    cdc_task_arn=list(filter(lambda output: output['OutputKey'] == 'CDCLoadTaskArn', dms_outputs))[0]['OutputValue']
    full_task_arn=list(filter(lambda output: output['OutputKey'] == 'FullLoadTaskArn', dms_outputs))[0]['OutputValue']
    return {"cdc": cdc_task_arn, "full": full_task_arn}
def start_dms_tasks(tasks):
    """
    Start CDC and Full loads in that order
    """

    
    response_cdc = dms_client.start_replication_task(
        ReplicationTaskArn=tasks['cdc'],
        StartReplicationTaskType= 'start-replication')   
    
    print(f"CDC Load task is {response_cdc['ReplicationTask']['Status']}")
    
    response_full = dms_client.start_replication_task(
        ReplicationTaskArn=tasks['full'],
        StartReplicationTaskType= 'resume-processing')
    
    print(f"Full Load task is {response_full['ReplicationTask']['Status']}")

start_dms_tasks(get_dms_task_arn())