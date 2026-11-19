#!/usr/bin/env python3

from aws_cdk import core

from dlpoc.vpc_stack import VpcStack
from dlpoc.rds_stack import RdsStack
from dlpoc.bastion_stack import BastionStack
from dlpoc.s3_stack import S3Stack
from dlpoc.dms_stack import DMSStack
from dlpoc.redshift_stack import RedshiftStack
from dlpoc.glue_stack import GlueStack
from dlpoc.unload_stack import UnloadStack
from dlpoc.query_schedule_stack import QueryScheduleStack

email_address="shveda@amazon.com"

app = core.App()
dl_vpc = VpcStack(app, "DLVPC")
rds_stack = RdsStack(app, "RDS", vpc=dl_vpc.vpc)
bastion_stack = BastionStack(app, "Bastion", vpc=dl_vpc.vpc, dms_sg=rds_stack.dms_sg, secret=rds_stack.secret)
s3_stack = S3Stack(app, "S3")
dms_stack = DMSStack(app, "DMS", vpc=dl_vpc.vpc, rds_secret=rds_stack.secret, raw_bucket=s3_stack.s3_raw, dms_sg=rds_stack.dms_sg)
redshift_stack = RedshiftStack(app, "Redshift", vpc=dl_vpc.vpc, raw_bucket=s3_stack.s3_raw, optimized_bucket=s3_stack.s3_optimized)
glue_stack = GlueStack(scope=app, id="Glue", raw_bucket=s3_stack.s3_raw, optimized_bucket=s3_stack.s3_optimized)
unload_stack = UnloadStack(
    app, "Unload", email_address=email_address, 
    glue_database = glue_stack.glue_database, redshift_database=redshift_stack.default_database_name,
    redshift_cluster=redshift_stack.redshift_cluster, redshift_role=redshift_stack.redshift_spectrum_role)
query_schedule_stack = QueryScheduleStack(
    scope=app, id="QuerySchedule",
    glue_database=glue_stack.glue_database, redshift_database=redshift_stack.default_database_name,
    redshift_cluster=redshift_stack.redshift_cluster, redshift_role=redshift_stack.redshift_spectrum_role)
app.synth()
