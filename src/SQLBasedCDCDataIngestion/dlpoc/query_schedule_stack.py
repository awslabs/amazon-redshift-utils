from typing import Dict

from aws_cdk import core
import aws_cdk.aws_redshift as redshift
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as targets
import aws_cdk.aws_glue as glue
import aws_cdk.aws_iam as iam


class QueryScheduleStack(core.Stack):
    def __init__(
            self, scope: core.Construct, id: str,
            glue_database: glue.IDatabase, redshift_database: str,
            redshift_cluster: redshift.ICluster, redshift_role: iam.IRole, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # AwsApi target to call the Redshift Data API
        def create_event_target(query: str) -> targets.AwsApi:
            return targets.AwsApi(
                action="executeStatement",
                service="RedshiftData",
                parameters={
                  "ClusterIdentifier": f"{redshift_cluster.cluster_name}",
                  "Sql": query,
                  "Database": f"{redshift_database}",
                  "SecretArn": f"{redshift_cluster.secret.secret_arn}",
                  "WithEvent": True
                },
                policy_statement=redshift_data_policy_statement
            )

        # IAM policy for events
        redshift_data_policy_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["redshift-data:ExecuteStatement", "secretsmanager:GetSecretValue"],
            resources=[
                f"arn:aws:redshift:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:cluster:{redshift_cluster.cluster_name}",
                f"{redshift_cluster.secret.secret_arn}"
            ]
        )

        # Redshift queries
        queries = dict()
        queries['call_sp'] = """ CALL refresh_account_change_summary(); """

        # Event rules for query scheduling
        def create_event_rule(query_id: str, schedule: events.Schedule):
            events_rule = events.Rule(
                scope=self, id=f"{query_id}_rule",
                enabled=True, schedule=schedule,
            )
            events_rule.add_target(create_event_target(queries[query_id]))

        create_event_rule(query_id='call_sp', schedule=events.Schedule.cron(hour="0", minute="0"))
