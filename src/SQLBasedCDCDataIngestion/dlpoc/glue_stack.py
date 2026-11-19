import json

from aws_cdk import core
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_glue as glue
import aws_cdk.aws_iam as iam

TABLES = [
    "pgbench_accounts",
    "pgbench_branches",
    "pgbench_history",
    "pgbench_tellers"
]


class GlueStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str,
                 raw_bucket: s3.IBucket, optimized_bucket: s3.IBucket,
                 **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Glue Database
        self.glue_database = glue.Database(scope=self, id="CDC Database", database_name="cdc_database")

        # IAM Role for Glue Crawler
        glue_crawler_role = iam.Role(
            scope=self, id="Glue Crawler Role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        )
        glue_crawler_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
        )
        raw_bucket.grant_read_write(glue_crawler_role)
        optimized_bucket.grant_read_write(glue_crawler_role)

        # S3 Targets for Glue Crawlers        
        full_s3_targets = glue.CfnCrawler.TargetsProperty(
            s3_targets=[glue.CfnCrawler.S3TargetProperty(path=f"s3://{raw_bucket.bucket_name}/full/public/{table}/") for table in TABLES]
        )
        
        cdc_s3_targets = glue.CfnCrawler.TargetsProperty(
            s3_targets=[glue.CfnCrawler.S3TargetProperty(path=f"s3://{raw_bucket.bucket_name}/CDC/public/{table}/") for table in TABLES]
        )
        
        optimized_s3_targets = glue.CfnCrawler.TargetsProperty(
            s3_targets=[glue.CfnCrawler.S3TargetProperty(path=f"s3://{optimized_bucket.bucket_name}/CDC/public/{table}/") for table in TABLES]
        )

        # Schedule for Glue Crawlers
        raw_crawler_schedule = glue.CfnCrawler.ScheduleProperty(schedule_expression="cron(0 0 * * ? *)")
        optimized_crawler_schedule = glue.CfnCrawler.ScheduleProperty(schedule_expression="cron(0 3 * * ? *)")

        # Schema Change Policy for Glue Crawlers
        crawler_schema_policy = glue.CfnCrawler.SchemaChangePolicyProperty(
            delete_behavior="DELETE_FROM_DATABASE",
            update_behavior="LOG"
        )

        # Custom Crawler Configuration
        crawler_config = {
            "Version": 1.0,
            "CrawlerOutput": {
                "Partitions": {
                    "AddOrUpdateBehavior": "InheritFromTable"
                }
            }
        }


            # Raw Full load crawler
        glue.CfnCrawler(
            scope=self,
            id=f"Full Load Crawler",
            role=glue_crawler_role.role_arn,
            targets=full_s3_targets,
            configuration=json.dumps(crawler_config),
            database_name=self.glue_database.database_name,
            schedule=None,
            schema_change_policy=crawler_schema_policy,
            table_prefix="raw_full_"
        )

        # Raw CDC crawler
        glue.CfnCrawler(
            scope=self,
            id=f"CDC Crawler",
            role=glue_crawler_role.role_arn,
            targets=cdc_s3_targets,
            configuration=json.dumps(crawler_config),
            database_name=self.glue_database.database_name,
            schedule=raw_crawler_schedule,
            schema_change_policy=crawler_schema_policy,
            table_prefix="raw_cdc_"
        )

        # Optimized crawler
        glue.CfnCrawler(
            scope=self,
            id="Optimized Crawler",
            role=glue_crawler_role.role_arn,
            targets=optimized_s3_targets,
            configuration=json.dumps(crawler_config),
            database_name=self.glue_database.database_name,
            schedule=optimized_crawler_schedule,
            schema_change_policy=crawler_schema_policy,
            table_prefix="optimized_"
        )
