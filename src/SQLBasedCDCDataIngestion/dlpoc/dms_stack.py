from aws_cdk import core
import aws_cdk.aws_dms as dms
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_iam as iam
import aws_cdk.aws_secretsmanager as secretsmanager
import json



class DMSStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, vpc: ec2.IVpc, rds_secret: secretsmanager.ISecret, dms_sg: ec2.ISecurityGroup ,raw_bucket: s3.IBucket,  **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
            
        replication_subnet_group = dms.CfnReplicationSubnetGroup(
            self, "ReplicationSubnetGroup",
            replication_subnet_group_description="Replication Subnet Group for DL",
            replication_subnet_group_identifier="replicationsubnetgroupdl",
            subnet_ids=[subnet.subnet_id for subnet in vpc.private_subnets])
        
        replication_subnet_group.to_string

        dms_s3_role = iam.Role(self, "DMS S3 Role", assumed_by=iam.ServicePrincipal("dms.amazonaws.com"))
        raw_bucket.grant_read_write(dms_s3_role)
        dms_replication_instance=dms.CfnReplicationInstance(
            self, "DMS Replication Instance",
            replication_instance_class="dms.t2.large",
            allocated_storage=50,
            auto_minor_version_upgrade=True,
            engine_version="3.4.2",
            replication_subnet_group_identifier="replicationsubnetgroupdl",
            vpc_security_group_ids=[dms_sg.security_group_id]
            )
        dms_replication_instance.add_depends_on(replication_subnet_group)
        

        source_endpoint = dms.CfnEndpoint(
            self, "Source Endpoint",
            endpoint_identifier="sourcerdsdatabase",
            endpoint_type="source",
            username=rds_secret.secret_value_from_json("username").to_string(),
            password=rds_secret.secret_value_from_json("password").to_string(),
            database_name=rds_secret.secret_value_from_json("dbname").to_string(),
            server_name=rds_secret.secret_value_from_json("host").to_string(),
            port=5432,
            engine_name=rds_secret.secret_value_from_json("engine").to_string()
            )

        target_endpoint_cdc = dms.CfnEndpoint(
            self, "Target Endpoint CDC",
            endpoint_identifier="targetendpointcdc",
            endpoint_type="target",
            engine_name="s3",
            extra_connection_attributes="AddColumnName=true;timestampColumnName=changets;dataFormat=parquet;DatePartitionEnabled=true;DatePartitionSequence=YYYYMMDD;DatePartitionDelimiter=NONE;bucketFolder=CDC",
            s3_settings=dms.CfnEndpoint.S3SettingsProperty(
                bucket_name=raw_bucket.bucket_name,
                service_access_role_arn=dms_s3_role.role_arn)
            )
        table_mappings = {
            "rules": [
                {
                    "rule-type": "selection",
                    "rule-id": "1",
                    "rule-name": "1",
                    "object-locator": {
                        "schema-name": "%",
                        "table-name": "%"
                        },
                    "rule-action": "include",
                    "filters": []
                }]
            }
        target_endpoint_full=dms.CfnEndpoint(
            self, "Target Endpoint Full",
            endpoint_identifier="targetendpointfull",
            endpoint_type="target",
            engine_name="s3",
            extra_connection_attributes=f"AddColumnName=true;timestampColumnName=changets;dataFormat=parquet;bucketFolder=full",
            s3_settings=dms.CfnEndpoint.S3SettingsProperty(
                bucket_name=raw_bucket.bucket_name,
                service_access_role_arn=dms_s3_role.role_arn)
            )
        table_mappings = {
            "rules": [
                {
                    "rule-type": "selection",
                    "rule-id": "1",
                    "rule-name": "1",
                    "object-locator": {
                        "schema-name": "%",
                        "table-name": "%"
                        },
                    "rule-action": "include",
                    "filters": []
                }]
            }
        
        replication_task_cdc = dms.CfnReplicationTask(
            self, "Replication Task CDC",
            migration_type="cdc",
            replication_task_identifier="cdcload",
            replication_instance_arn=dms_replication_instance.ref,
            source_endpoint_arn=source_endpoint.ref,
            target_endpoint_arn=target_endpoint_cdc.ref,
            table_mappings=json.dumps(table_mappings)
            )
        replication_task_full = dms.CfnReplicationTask(
            self, "Replication Task Full",
            migration_type="full-load",
            replication_task_identifier="fullload",
            replication_instance_arn=dms_replication_instance.ref,
            source_endpoint_arn=source_endpoint.ref,
            target_endpoint_arn=target_endpoint_full.ref,
            table_mappings=json.dumps(table_mappings)
            )

        core.CfnOutput(self, "Full Load Task Arn", value=replication_task_full.ref)
        core.CfnOutput(self, "CDC Load Task Arn", value=replication_task_cdc.ref)
