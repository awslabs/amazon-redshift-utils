from aws_cdk import core
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_redshift as redshift
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_lambda_python as lambda_python
from aws_cdk import custom_resources as custom_resources
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_iam as iam

class RedshiftStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, vpc: ec2.IVpc, raw_bucket: s3.IBucket, optimized_bucket: s3.IBucket, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        # Redshift default database
        self.default_database_name="demo"
        
        # role for redshift to use spectrum
        self.redshift_spectrum_role = iam.Role(
            self,"RedshiftSpectrumRole", 
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"))

        # policy to access glue catalog
        self.redshift_spectrum_role.add_to_policy(
            iam.PolicyStatement(
                actions = ["glue:BatchCreatePartition","glue:UpdateDatabase","glue:CreateTable",
                    "glue:DeleteDatabase","glue:GetTables","glue:GetPartitions","glue:BatchDeletePartition",
                    "glue:UpdateTable","glue:BatchGetPartition","glue:DeleteTable","glue:GetDatabases",
                    "glue:GetTable","glue:GetDatabase","glue:GetPartition","glue:CreateDatabase",
                    "glue:BatchDeleteTable","glue:CreatePartition","glue:DeletePartition","glue:UpdatePartition"],
                effect=iam.Effect.ALLOW,
                resources=["*"]
            )
        )
        # policy to read data files from raw and optimized buckets
        self.redshift_spectrum_role.add_to_policy(
            iam.PolicyStatement(
                actions = ["s3:GetObject","s3:ListBucketMultipartUploads","s3:ListBucket","s3:GetBucketLocation","s3:ListMultipartUploadParts"],
                effect=iam.Effect.ALLOW,
                resources=[raw_bucket.bucket_arn, optimized_bucket.bucket_arn, f"{raw_bucket.bucket_arn}/*",f"{optimized_bucket.bucket_arn}/*" ]
            )
        )

        # policy to write and overwriter data files in optimized bucket
        self.redshift_spectrum_role.add_to_policy(
            iam.PolicyStatement(
                actions = ["s3:PutObject","s3:DeleteObjectVersion","s3:DeleteObject"],
                effect=iam.Effect.ALLOW,
                resources=[f"{optimized_bucket.bucket_arn}/*"]
            )
        )
        # create SG for other hosts to access Redshift
        self.access_redshift_sg= ec2.SecurityGroup(
            self, "Access Redshift SG", vpc=vpc,
            allow_all_outbound=True,
            description="Access Redshift SG")
        
        # create SG for Redshift
        redshift_sg = ec2.SecurityGroup(
            self, "Redshift SG", vpc=vpc,
            allow_all_outbound=True,
            description="Redshift SG")
        
        # spin up redshift in isolated subnets without access to IGW or NAT
        private_sng = redshift.ClusterSubnetGroup(
            self, "Subnet Group",
            vpc=vpc,
            removal_policy=core.RemovalPolicy.DESTROY,
            description="Private Subnet Group",
            vpc_subnets=ec2.SubnetSelection(subnets=vpc.private_subnets))

        # redshir cluster for demo database
        self.redshift_cluster = redshift.Cluster(
            self, "Redshift",
            master_user=redshift.Login(master_username="root"),
            vpc=vpc,
            cluster_name="redshifteltdemo",
            cluster_type=redshift.ClusterType.MULTI_NODE,
            default_database_name=self.default_database_name,
            number_of_nodes=2,
            node_type=redshift.NodeType.DC2_LARGE,
            removal_policy=core.RemovalPolicy.DESTROY,
            security_groups=[redshift_sg],
            subnet_group=private_sng,
            roles=[self.redshift_spectrum_role])
        
        core.Tags.of(self.redshift_cluster.secret).add(key="RedshiftDataFullAccess", value="dataAPI")
        # allow access_redshift_sg to access redshift
        self.redshift_cluster.connections.allow_default_port_from(self.access_redshift_sg)

        
        # python lambda to deploy initial schema
        redshift_deploy_schema_lambda = lambda_python.PythonFunction(
            self, "RedshiftDeploySchema",
            entry = "./lambdas/redshift_deploy_schema",
            index = "lambda.py",
            handler = "lambda_handler",
            runtime = _lambda.Runtime.PYTHON_3_8,
            timeout =  core.Duration.seconds(900),
            tracing = _lambda.Tracing.ACTIVE
        )
        redshift_deploy_schema_lambda.add_environment(key="cluster_identifier", value=self.redshift_cluster.cluster_name)
        redshift_deploy_schema_lambda.add_environment(key="secret_arn", value=self.redshift_cluster.secret.secret_arn)
        redshift_deploy_schema_lambda.add_environment(key="database", value=self.default_database_name)

        self.redshift_cluster.secret.grant_read(redshift_deploy_schema_lambda)
        
        # allow lambda to execute sql statements against the cluster
        redshift_deploy_schema_lambda.add_to_role_policy(
            statement=iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["redshift-data:ExecuteStatement"],
                resources=[f"arn:aws:redshift:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:cluster:{self.redshift_cluster.cluster_name}"]
            )
        )
        # allow lambda to check status of executed statements
        redshift_deploy_schema_lambda.add_to_role_policy(
            statement=iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["redshift-data:ListStatements"],
                resources=["*"]
            )
        )

        core.CfnOutput(self, "Redshift Cluster ID ", value=self.redshift_cluster.cluster_name)
        core.CfnOutput(self, "Redshift Secret Arn ", value=self.redshift_cluster.secret.secret_arn)
        
        # custom resource to deploy schema
        provider = custom_resources.Provider(self, "Provider", on_event_handler= redshift_deploy_schema_lambda)
        custom_resource = core.CustomResource(
            self, "CustomResource",
            service_token= provider.service_token)