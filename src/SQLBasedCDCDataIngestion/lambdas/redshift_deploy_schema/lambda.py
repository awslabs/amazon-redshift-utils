import traceback
import boto3
import json
import time
import os
import cfnresponse 

# create redshift data client
redshift_data_client = boto3.client('redshift-data')

# get cluster specific parameters
cluster_identifier = os.environ['cluster_identifier']
secret_arn = os.environ['secret_arn']
database = os.environ['database']

# error class for failed redshift SQL
class FailedRedshiftSQL(Exception):
    pass

def execute_sqls():
    """
    Execute schema.sql statements one by one on redshift cluster
    using redshift data API
    """
    # read schema file
    file = open('schema.sql', 'r')
    script_file = file.read()
    file.close
    try:
        # split file in commands based on ;
        commands=script_file.split(';')
        for order, command in enumerate(commands, 1):
            statement_name = f'schema statement #{order}'
            response = redshift_data_client.execute_statement(
                ClusterIdentifier=cluster_identifier,
                Database=database,
                SecretArn=secret_arn,
                Sql=command,
                StatementName = statement_name ,
                WithEvent=False
                )
            # generate list of statements ran
            check_execution(statement_name)
    except Exception as e:
      print("Cannot Execute." + str(e) + traceback.format_exc())
      raise e
    return "Success"

def check_execution(statement_name):
    """
    Check if all statement names executed successfuly on newly created cluster
    """
    try:
        status='CHECKING'
        # wait for statement to finish
        while status!='FINISHED':
            response = redshift_data_client.list_statements(
                StatementName=statement_name,
                Status='ALL')
            status=response['Statements'][0]['Status']
            # if statement fails raise an error
            if status in ['ABORTED','FAILED']:
                raise FailedRedshiftSQL(f"failed {statement_name} statement with status {status}")
            print(f"{statement_name} - {status}")
            # wait for a second before rechecking
            time.sleep(1)
    except Exception as e:
        print("Cannot Execute." + str(e) + traceback.format_exc())
        raise e
    return "Success"
def get_cfn_response_data(message):
    """
    Format message before passing to AWS CloudFormation
    """
    response_data = {}
    data = {'Message': message}
    response_data['Data'] = data
    return response_data  

def lambda_handler(event, context):
    try:
        if event['RequestType'] == 'Create':
            try:
                return execute_sqls()               
            # if statements failed rollback cluster creation
            except Exception as e:
                cfnresponse.send(event, context, cfnresponse.FAILED, get_cfn_response_data('failed: '+str(e)))
                raise Exception(e)
            else: 
                # delete or update of cluster do not involve schema changes
                print('Delete/Update CF initiated') 
                cfnresponse.send(event, context, cfnresponse.SUCCESS, get_cfn_response_data('delete'))
    except Exception as e:
        print(e)
        cfnresponse.send(event, context, cfnresponse.FAILED, get_cfn_response_data('failed: '+str(e)))
        raise Exception(e)