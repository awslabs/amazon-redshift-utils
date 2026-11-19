from aws_cdk import core
import aws_cdk.aws_s3 as s3

class S3Stack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.s3_raw = s3.Bucket(
            self, "S3Raw",
            bucket_name=f"bucket-raw-{core.Aws.ACCOUNT_ID}-{core.Aws.REGION}",
            removal_policy=core.RemovalPolicy.DESTROY)
        
        self.s3_optimized = s3.Bucket(
            self, "S3Optimized",
            bucket_name=f"bucket-optimized-{core.Aws.ACCOUNT_ID}-{core.Aws.REGION}",
            removal_policy=core.RemovalPolicy.DESTROY)
        

