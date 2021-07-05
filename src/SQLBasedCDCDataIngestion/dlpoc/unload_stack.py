from aws_cdk import core
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as events_targets
from aws_cdk import aws_lambda_python as lambda_python
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_sns as sns
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_sns_subscriptions as sns_subscriptions
from aws_cdk import aws_redshift as redshift
class UnloadStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, email_address: str , glue_database: glue.IDatabase, redshift_database: str, redshift_cluster: redshift.ICluster, redshift_role: iam.IRole, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # lambda to reduce number of cdc files and improve performance
        unload_cdc_optimized_lambda = lambda_python.PythonFunction(
            self, "RedshiftDeploySchema",
            entry = "./lambdas/unload_cdc_optimized",
            index = "lambda.py",
            handler = "lambda_handler",
            runtime = _lambda.Runtime.PYTHON_3_8,
            timeout =  core.Duration.seconds(900),
            tracing = _lambda.Tracing.ACTIVE
        )
        unload_cdc_optimized_lambda.add_environment(key="glue_database", value=glue_database.database_name)
        unload_cdc_optimized_lambda.add_environment(key="redshift_database", value=redshift_database)
        unload_cdc_optimized_lambda.add_environment(key="cluster_identifier", value=redshift_cluster.cluster_name)
        unload_cdc_optimized_lambda.add_environment(key="secret_arn", value=redshift_cluster.secret.secret_arn)
        unload_cdc_optimized_lambda.add_environment(key="redshift_role_arn", value=redshift_role.role_arn)
        
        # priv to list all tables in glue database
        unload_cdc_optimized_lambda.add_to_role_policy(
            statement= iam.PolicyStatement(
                actions=["glue:GetTables"],
                effect=iam.Effect.ALLOW,
                resources=[f"arn:aws:glue:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:catalog",
                           f"arn:aws:glue:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:database/{glue_database.database_name}",
                           f"arn:aws:glue:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:table/{glue_database.database_name}/*"]
            )
        )
        # priv to execute statement against Redshift cluster
        unload_cdc_optimized_lambda.add_to_role_policy(
            statement=iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["redshift-data:ExecuteStatement"],
                resources=[f"arn:aws:redshift:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:cluster:{redshift_cluster.cluster_name}"]
            )
        )
        # priv to read a secret to connect to the Redshift cluster
        redshift_cluster.secret.grant_read(unload_cdc_optimized_lambda)
        # rule to trigger optimization once day
        unload_cdc_optimized_rule = events.Rule(
            self, "UnloadCDCOptimizedRule",
            schedule=events.Schedule.expression('cron(0 1 * * ? *)'),
            rule_name="UnloadCDCOptimizedRule",
            enabled=True)
        unload_cdc_optimized_rule.add_target(
            target=events_targets.LambdaFunction(
                handler=unload_cdc_optimized_lambda)
            )

        # SNS topic for all Redsift Data API failures
        redshift_data_failure_topic = sns.Topic(
            self, "RedshiftDataFailureTopic",
            display_name="RedshiftDataFailureTopic",
            topic_name="RedshiftDataFailureTopic")
        redshift_data_failure_topic.add_subscription(
            subscription=sns_subscriptions.EmailSubscription(
                email_address=email_address)
            )
        
        # Rule to identify Redshift Data API failures and send them to SNS topic
        events.Rule(
            self, "FailedRedshiftDataStatementRule",
            event_pattern= events.EventPattern(
                source=["aws.redshift-data"],
                detail= {"state": ["FAILED"]}
                ),
            enabled=True,
            rule_name="FailedRedshiftDataStatementRule",
            targets=[events_targets.SnsTopic(topic=redshift_data_failure_topic)]
        )
