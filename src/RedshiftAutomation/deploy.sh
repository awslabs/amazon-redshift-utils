#!/bin/bash
#set -x

ver=1.4

for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do aws s3 cp dist/lambda-redshift-util-runner-$ver.zip s3://awslabs-code-$r/LambdaRedshiftRunner/lambda-redshift-util-runner-$ver.zip --acl public-read --region $r; done
