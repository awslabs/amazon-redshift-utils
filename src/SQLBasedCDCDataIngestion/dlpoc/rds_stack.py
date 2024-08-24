from aws_cdk import core
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_rds as rds


class RdsStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, vpc: ec2.IVpc, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Parameters to enable PostgreSQL logical replication
        parameter_group_replication = rds.ParameterGroup(
            scope=self, id="Parameter Group Replication",
            engine=rds.DatabaseInstanceEngine.postgres(version=rds.PostgresEngineVersion.VER_11_8),
            parameters={"rds.logical_replication": "1"})

        # PostgreSQL RDS instance
        self.postgresql_rds = rds.DatabaseInstance(
            scope=self, id="Postgresql RDS",
            database_name="test",
            engine=rds.DatabaseInstanceEngine.postgres(version=rds.PostgresEngineVersion.VER_11_8),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE2, ec2.InstanceSize.SMALL),
            vpc=vpc, multi_az=False,
            allocated_storage=100, storage_type=rds.StorageType.GP2,
            deletion_protection=False,
            delete_automated_backups=True, backup_retention=core.Duration.days(1),
            parameter_group=parameter_group_replication
        )

        # Secret
        self.secret = self.postgresql_rds.secret

        # CFN Outputs
        core.CfnOutput(
            scope=self, id="Database Secret Arn",
            value=self.postgresql_rds.secret.secret_arn
        )
        core.CfnOutput(
            scope=self, id="Database Endpoint Address",
            value=self.postgresql_rds.db_instance_endpoint_address
        )
        
        # PostgreSQL RDS Connections
        self.dms_sg = ec2.SecurityGroup(
            scope=self, id="DMS SG",
            vpc=vpc, allow_all_outbound=True,
            description="Security Group for DMS"
        )
        self.postgresql_rds.connections.allow_default_port_from(self.dms_sg)
