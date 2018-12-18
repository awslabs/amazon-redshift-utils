#!/bin/bash
#set -x

ver=`cat package.json | grep "version" | cut -d: -f2 | sed -e "s/[\"\,]//g" | tr -d '[:space:]'`
token=RedshiftSnapshotManager

for r in `aws ec2 describe-regions --query Regions[*].RegionName --output text`; do
    aws s3 cp dist/$token-$ver.zip s3://awslabs-code-$r/$token/$token-$ver.zip --acl public-read --region $r;
    aws s3 cp deploy.yaml s3://awslabs-code-$r/$token/deploy.yaml --acl public-read --region $r;
done
