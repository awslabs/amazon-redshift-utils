#!/bin/bash
#set -x

ver=1.6

for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do aws s3 cp dist/lambda-redshift-util-runner-$ver.zip s3://awslabs-code-$r/LambdaRedshiftRunner/lambda-redshift-util-runner-$ver.zip --acl public-read --region $r; done

for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do aws s3 cp deploy.yaml s3://awslabs-code-$r/LambdaRedshiftRunner/deploy.yaml --acl public-read --region $r; done

for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do aws s3 cp deploy-function-and-schedule.yaml s3://awslabs-code-$r/LambdaRedshiftRunner/deploy-function-and-schedule.yaml --acl public-read --region $r; done
