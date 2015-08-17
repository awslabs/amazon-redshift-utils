#!/bin/bash

key=alias/RedshiftUnloadCopyUtility

aws kms encrypt --key-id $key --plaintext $1 --region $2 --query CiphertextBlob --output text
