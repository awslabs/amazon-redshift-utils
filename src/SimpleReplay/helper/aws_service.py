import base64
import datetime
import json
import logging
import boto3
from botocore.exceptions import ClientError
import asyncio
import functools

logger = logging.getLogger("WorkloadReplicatorLogger")


def redshift_get_serverless_workgroup(workgroup_name, region):
    rs_client = boto3.client("redshift-serverless", region_name=region)
    return rs_client.get_workgroup(workgroupName=workgroup_name)


def redshift_describe_clusters(cluster_id, region):
    rs_client = boto3.client("redshift", region_name=region)
    response = rs_client.describe_clusters(ClusterIdentifier=cluster_id)
    return response


def redshift_describe_logging_status(source_cluster_endpoint):
    """
    Get the audit log location for the cluster via the API
    """
    logger.debug(f"Retrieving log location for {source_cluster_endpoint}")
    region = source_cluster_endpoint.split(".")[2]
    result = boto3.client("redshift", region).describe_logging_status(
        ClusterIdentifier=source_cluster_endpoint.split(".")[0]
    )
    return result


def redshift_get_cluster_credentials(
    region,
    user,
    database_name,
    cluster_id,
    duration=900,
    auto_create=False,
    additional_client_args={},
):
    rs_client = boto3.client("redshift", region, **additional_client_args)
    try:
        response = rs_client.get_cluster_credentials(
            DbUser=user,
            DbName=database_name,
            ClusterIdentifier=cluster_id,
            DurationSeconds=duration,
            AutoCreate=auto_create,
        )
    except Exception as e:
        if e == rs_client.exceptions.ClusterNotFoundFault:
            logger.error(
                f"Cluster {cluster_id} not found. Please confirm cluster endpoint, account, and region."
            )
        else:
            logger.error(f"Error while getting cluster credentials: {e}", exc_info=True)
        exit(-1)
    return response


def redshift_execute_query(
    redshift_cluster_id, redshift_user, redshift_database_name, region, query
):
    """
    Executes redshift query and gets response for query when finished
    """
    # get query id
    redshift_data_api_client = boto3.client("redshift-data", region)
    response_execute_statement = redshift_data_api_client.execute_statement(
        Database=redshift_database_name,
        DbUser=redshift_user,
        Sql=query,
        ClusterIdentifier=redshift_cluster_id,
    )
    query_id = response_execute_statement["Id"]

    query_done = False

    while not query_done:
        response_describe_statement = redshift_data_api_client.describe_statement(Id=query_id)
        query_status = response_describe_statement["Status"]

        if query_status == "FAILED":
            logger.debug(f"SQL execution failed. Query ID = {query_id}")
            raise Exception

        elif query_status == "FINISHED":
            query_done = True
            # log result if there is a result (typically from Select statement)
            if response_describe_statement["HasResultSet"]:
                response_get_statement_result = redshift_data_api_client.get_statement_result(
                    Id=query_id
                )
    return response_get_statement_result

def execute_query_sync(redshift_data_api_client, redshift_database_name, redshift_user, query, cluster_id):
    response_execute_statement = redshift_data_api_client.execute_statement(
        Database=redshift_database_name,
        DbUser=redshift_user,
        Sql=query,
        ClusterIdentifier=cluster_id,
    )
    query_id = response_execute_statement["Id"]
    query_done = False
    while not query_done:
        response_describe_statement = redshift_data_api_client.describe_statement(Id=query_id)
        query_status = response_describe_statement["Status"]
        if query_status == "FAILED":
            logger.debug(f"SQL execution failed. Query ID = {query_id}")
            raise Exception
        elif query_status == "FINISHED":
            query_done = True
            # log result if there is a result (typically from Select statement)
            if response_describe_statement["HasResultSet"]:
                response_get_statement_result = redshift_data_api_client.get_statement_result(
                    Id=query_id
                )
                return response_get_statement_result
    return None  # Handle the case where there's no result

async def redshift_execute_query_async(
    redshift_cluster_id, redshift_user, redshift_database_name, region, query
):
    """
    Executes redshift query asynchronusly and gets response for query when finished 
    """
    loop = asyncio.get_event_loop()
    redshift_data_api_client = boto3.client("redshift-data", region_name=region)
    return await loop.run_in_executor(None, execute_query_sync, redshift_data_api_client, redshift_database_name, redshift_user, query, redshift_cluster_id)


def cw_describe_log_groups(log_group_name=None, region=None):
    cloudwatch_client = boto3.client("logs", region)
    if log_group_name:
        return cloudwatch_client.describe_log_groups(logGroupNamePrefix=log_group_name)
    else:
        response_pg_1 = cloudwatch_client.describe_log_groups()
        logs = response_pg_1

        token = response_pg_1.get("nextToken", "")
        while token != "":
            response_itr = cloudwatch_client.describe_log_groups(nextToken=token)
            logs["logGroups"].extend(response_itr["logGroups"])
            token = response_itr["nextToken"] if "nextToken" in response_itr.keys() else ""
    return logs


def cw_describe_log_streams(log_group_name, region):
    cloudwatch_client = boto3.client("logs", region)
    return cloudwatch_client.describe_log_streams(logGroupName=log_group_name)


def cw_get_paginated_logs(log_group_name, log_stream_name, start_time, end_time, region):
    log_list = []
    cloudwatch_client = boto3.client("logs", region)
    paginator = cloudwatch_client.get_paginator("filter_log_events")
    pagination_config = {"MaxItems": 10000}
    convert_to_millis_since_epoch = (
        lambda time: int(
            (time.replace(tzinfo=None) - datetime.datetime.utcfromtimestamp(0)).total_seconds()
        )
        * 1000
    )
    start_time_millis_since_epoch = convert_to_millis_since_epoch(start_time)
    end_time_millis_since_epoch = convert_to_millis_since_epoch(end_time)
    response_iterator = paginator.paginate(
        logGroupName=log_group_name,
        logStreamNames=[log_stream_name],
        startTime=start_time_millis_since_epoch,
        endTime=end_time_millis_since_epoch,
        PaginationConfig=pagination_config,
    )
    next_token = None
    while next_token != "":
        for response in response_iterator:
            next_token = response.get("nextToken", "")
            for event in response["events"]:
                log_list.append(event["message"])
        pagination_config.update({"StartingToken": next_token})
        response_iterator = paginator.paginate(
            logGroupName=log_group_name,
            logStreamNames=[log_stream_name],
            startTime=start_time_millis_since_epoch,
            endTime=end_time_millis_since_epoch,
            PaginationConfig=pagination_config,
        )
    return log_list


def s3_upload(local_file_name, bucket, key=None):
    s3 = boto3.resource("s3")
    k = key if key else local_file_name
    s3.meta.client.upload_file(local_file_name, bucket, k)
    return s3.Object(bucket, k)


def s3_put_object(file_content, bucket, key):
    s3 = boto3.client("s3")
    s3.put_object(Body=file_content, Bucket=bucket, Key=key)


def s3_resource_put_object(bucket, prefix, body):
    s3_resource = boto3.resource("s3")
    s3_resource.Object(bucket, prefix).put(Body=body)


async def s3_get_bucket_contents(bucket, prefix):
    s3_client = boto3.client("s3")
    loop = asyncio.get_event_loop()
    bucket_objects = []
    continuation_token = ""
    while True:
        if continuation_token != "":
            f_list_bounded = functools.partial(
                s3_client.list_objects_v2,
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=continuation_token,
            )
        else:
            f_list_bounded = functools.partial(
                s3_client.list_objects_v2, Bucket=bucket, Prefix=prefix
            )
        response = await loop.run_in_executor(executor=None, func=f_list_bounded)
        bucket_objects.extend(response.get("Contents", []))
        if response["IsTruncated"]:
            continuation_token = response["NextContinuationToken"]
        else:
            break
    return bucket_objects


def sync_s3_get_bucket_contents(bucket, prefix):
    conn = boto3.client("s3")

    # get first set of
    response = conn.list_objects_v2(Bucket=bucket, Prefix=prefix)
    bucket_objects = response.get("Contents", [])

    if "NextContinuationToken" in response:
        prev_key = response["NextContinuationToken"]
        while True:
            response = conn.list_objects_v2(
                Bucket=bucket, Prefix=prefix, ContinuationToken=prev_key
            )
            bucket_objects.extend(response["Contents"])
            if "NextContinuationToken" not in response:
                break
            prev_key = response["NextContinuationToken"]
    return bucket_objects


def s3_generate_presigned_url(client_method, bucket_name, object_name):
    s3_client = boto3.client("s3")
    response = s3_client.generate_presigned_url(
        client_method,
        Params={"Bucket": bucket_name, "Key": object_name},
        ExpiresIn=604800,
    )
    return response


def s3_copy_object(src_bucket, src_prefix, dest_bucket, dest_prefix):
    boto3.client("s3").copy_object(
        Bucket=dest_bucket,
        Key=dest_prefix,
        CopySource={"Bucket": src_bucket, "Key": src_prefix},
    )


def s3_get_object(bucket, filename):
    s3 = boto3.resource("s3")
    return s3.Object(bucket, filename)


def s3_client_get_object(bucket, key):
    s3 = boto3.client("s3")
    return s3.get_object(Bucket=bucket, Key=key)


def glue_get_table(database, table, region):
    table_get_response = boto3.client("glue", region).get_table(
        DatabaseName=database,
        Name=table,
    )
    return table_get_response


def glue_get_partition_indexes(database, table, region):
    index_response = boto3.client("glue", region).get_partition_indexes(
        DatabaseName=database,
        TableName=table,
    )
    return index_response


def glue_create_table(new_database, table_input, region):
    boto3.client("glue", region).create_table(DatabaseName=new_database, TableInput=table_input)


def glue_get_database(name, region):
    boto3.client("glue", region).get_database(Name=name)


def glue_create_database(name, description, region):
    boto3.client("glue", region).create_database(
        DatabaseInput={
            "Name": name,
            "Description": description,
        }
    )


def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    client = boto3.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if "SecretString" in get_secret_value_response:
            return json.loads(get_secret_value_response["SecretString"])
        else:
            return json.loads(base64.b64decode(get_secret_value_response["SecretBinary"]))
    except ClientError as e:
        logger.error(
            f"Exception occurred while getting secret from Secrets manager {e}", exc_info=True
        )
        raise e