#!/bin/bash
set +x

ver=`cat package.json | grep version | cut -d: -f2 | sed -e "s/\"//g" | sed -e "s/ //g" | sed -e "s/\,//g"`

function usage {
	echo "build.sh (deploy <role-arn> (schedule <config file>))"
	exit -1
}

function json_escape {
  echo -n "$1" | python -c 'import json,sys; print json.dumps(sys.stdin.read())'
}

if [ "$AWS_REGION" == "" ]; then
  echo "You must set environment variable AWS_REGION to continue"
  exit -1
fi

# grab args
action=$1
role=$2
following_action=$3
config_file=$4

# defaults
ram=128
timeout=60
debug=0
function_name=RedshiftUtilsSnapshotManager
zipfile=RedshiftSnapshotManager-$ver.zip
zipfile_ref=fileb://dist/$zipfile

if [ "$action" == "" ]; then
	action="None"
elif [ "$action" == "schedule" ]; then
	# arguments out of order
	action="schedule"
	config_file=$2
fi

schedule_minutes=15

if [ "$action" == "assemble" ]; then 
	zip -r $zipfile *.js package.json node_modules/ && mv $zipfile dist
fi

if [ "$action" == "deploy" ]; then
	if [ "$role" == "" ]; then
		echo "Cannot deploy without a Role specified"
		exit -1
	fi
	
	echo "Checking for an existing version of this function in AWS Lambda..."
	existing_code_location=`aws lambda get-function --function-name $function_name --query Configuration.FunctionArn`
	
	exists=0
	if [ "$existing_code_location" != "" ]; then
		exists=1
	fi

	if [ $exists == 1 ]; then
		aws lambda update-function-code --function-name $function_name --zip-file $zipfile_ref
	else
		aws lambda create-function --function-name $function_name --runtime nodejs4.3 --role $role --handler index.handler --timeout $timeout --memory-size $ram --publish --zip-file $zipfile_ref
	fi
fi

if [ "$action" == "schedule" -o "$following_action" == "schedule" ]; then
	python schedule.py $config_file
	
	# Allow CloudWatch Events to invoke our Lambda Function
	#aws lambda add-permission --function-name $function_name --statement-id $function_nameCWEventsPermission-`date +"%Y-%m-%dt%H%M%S"` --action 'lambda:InvokeFunction' --principal events.amazonaws.com --source-arn $event_rule_arn
fi