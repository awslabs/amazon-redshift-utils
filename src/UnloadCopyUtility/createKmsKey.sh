#!/bin/bash

if [ "$1" == "" ]; then
  echo "Usage createKmsKeys.sh <aws region>"
  echo "  <aws region> - AWS Region Short Name, such as 'us-east-1' or 'eu-west-1'"
  exit -1
fi

keyArn=$(aws kms create-key --description RedshiftUnloadCopyUtility --key-usage "ENCRYPT_DECRYPT" --region $1 --query KeyMetadata.Arn)

if [ $? -gt 0 ]; then
	echo "Coudln't create new KMS Master Key $keyArn, exiting!"
	exit 1
fi

echo "Created new KMS Master Key $keyArn"

keyId=$(echo $keyArn | cut -d/ -f2 | tr -d '"')

aws kms create-alias --target-key-id $keyId --alias-name "alias/RedshiftUnloadCopyUtility" --region $1

if [ $? -eq 0 ]; then
	echo "Created new KMS Master Key $keyArn with alias alias/RedshiftUnloadCopyUtility"
else
	echo "Coudln't create alias alias/RedshiftUnloadCopyUtility for new KMS Master Key $keyArn"
	exit 1
fi
