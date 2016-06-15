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
	action="None"
fi

schedule_minutes=15

zip -r $zipfile *.js package.json node_modules/ && mv $zipfile dist

existing_code_location=`aws lambda get-function --function-name $function_name --query Code.Location`

exists=0
if [ $existing_code_location != "" ]; then
	exists=1
fi

if [ "$action" == "deploy" ]; then
	if [ $exists == 1 ]; then
		aws lambda update-function-code --function-name $function_name --zip-file $zipfile_ref
	else
		aws lambda create-function --function-name $function_name --runtime nodejs4.3 --role $role --handler index.handler --timeout $timeout --memory-size $ram --publish --zip-file $zipfile_ref
	fi
fi

if [ "$following_action" == "schedule" ]; then
	config=`cat $config_file | tr -d '\n' | sed -e "s/[[:blank:]]//g"`
	
	# run the configuration through json escape
	processed_config=`json_escape $config `
	
	if [ "$config" == "" ]; then
		usage
	fi
	
	# add the CloudWatch Event
	rule_name=$function_name-$schedule_minutes-mins
	create_rule_cmd="aws events put-rule --name $rule_name --state ENABLED --schedule-expression \"rate($schedule_minutes minutes)\" --query \"RuleArn\" | sed -e \"s/\\\"//g\""
	echo $create_rule_cmd
	event_rule_arn=`$create_rule_cmd`
	
	# now add the Lambda target
	create_target_cmd="aws events put-targets --rule $rule_name --targets Arn=$event_rule_arn,Input=$processed_config"
	
	# Allow CloudWatch Events to invoke our Lambda Function
	aws lambda add-permission --function-name $function_name --statement-id $function_nameCWEventsPermission-`date +"%Y-%m-%dt%H%M%S"` --action 'lambda:InvokeFunction' --principal events.amazonaws.com --source-arn $event_rule_arn
fi