import boto3
from os import environ
from datetime import date 
from datetime import timedelta

# get all environment variables
glue_database = environ['glue_database']
redshift_database = environ['redshift_database']
redshift_role_arn = environ['redshift_role_arn']
cluster_identifier= environ['cluster_identifier']
secret_arn= environ['secret_arn']

# create all clients
glue_client=boto3.client("glue")
redshift_data_client = boto3.client('redshift-data')

def get_yeterday_partition():
    """
    get partition in YYYYMMDD format - the same as DMS
    """
    return str(date.today() - timedelta(days = 1)).replace("-", "")

def execute_statement(sql_statement, table):
    """
    Executes sql statement using Redshift Data API
    """
    statement_name = f'unload  table - {table} partition - {get_yeterday_partition()}'
    response = redshift_data_client.execute_statement(
        ClusterIdentifier=cluster_identifier,
        Database=redshift_database,
        SecretArn=secret_arn,
        Sql=sql_statement,
        StatementName = statement_name ,
        WithEvent=True)
        
def create_unload_statement(table_name, table_location):
    """
    Creates unload statement for yesterdays date.  Note: change in bucket name for output 
    from -raw- to -optimized-
    """
    optimized_location = table_location.replace("-raw-", "-optimized-")
    yesterday_partition =  get_yeterday_partition()
    sql_statement = f"""
    UNLOAD ('select  * from {glue_database}.{table_name} where partition_0=\\'{yesterday_partition}\\'') 
    TO '{optimized_location}changedate={yesterday_partition}/part_'  
    iam_role '{redshift_role_arn}'
    FORMAT PARQUET
    PARALLEL OFF
    MAXFILESIZE 200 MB
    ALLOWOVERWRITE"""

    return sql_statement


def lambda_handler(event, context):
    
    # get paginator just in case of many tables
    paginator = glue_client.get_paginator('get_tables')
    
    # filter for tables starting with raw_cdc_ as per convention
    response_iterator = paginator.paginate(
        DatabaseName=glue_database,
        Expression='raw_cdc_*',
        PaginationConfig={
            'PageSize': 10
            })
            
    for page in response_iterator:
        for table in page["TableList"]:
            table_name=table['Name']
            
            # for each table create and execute unload statement
            sql_statement = create_unload_statement(table['Name'], table['StorageDescriptor']['Location'])
            execute_statement(sql_statement, table_name)
    
    return "Success"