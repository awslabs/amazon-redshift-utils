import os

from aws_cdk import core
from aws_cdk.aws_s3_assets import Asset
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_iam as iam
import aws_cdk.aws_secretsmanager as secretsmanager

DIRNAME = os.path.dirname(__file__)


class BastionStack(core.Stack):
    def __init__(
            self, scope: core.Construct, id: str,
            vpc: ec2.IVpc, dms_sg: ec2.ISecurityGroup,
            secret: secretsmanager.ISecret, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Allow SSH Access
        dms_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4("52.95.4.3/32"), connection=ec2.Port.tcp(22),
            description="SSH access"
        )
        dms_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4("3.231.255.131/32"), connection=ec2.Port.tcp(22),
            description="SSH access"
        )

        # Create Bastion
        self.bastion_host = ec2.BastionHostLinux(
            scope=self, id="BastionHost",
            vpc=vpc, subnet_selection=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            instance_name="BastionHost", instance_type=ec2.InstanceType(instance_type_identifier="t3.micro"),
            security_group=dms_sg
        )

        # Permission for using Secrets Manager
        self.bastion_host.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("SecretsManagerReadWrite")
        )

        # Script asset
        asset_script = Asset(scope=self, id="pgbench_script", path=os.path.join(DIRNAME, "bootstrap.sh"))
        local_bootstrap_path = self.bastion_host.instance.user_data.add_s3_download_command(
            bucket=asset_script.bucket,
            bucket_key=asset_script.s3_object_key
        )

        # Execute script asset
        self.bastion_host.instance.user_data.add_execute_file_command(
            file_path=local_bootstrap_path,
            arguments=f"-s {secret.secret_full_arn}"
        )

        # Read permissions to assets
        asset_script.grant_read(self.bastion_host.role)
