#!/usr/bin/env python27

import os
import sys
import boto3
import json
import shortuuid

schedule_mins = 15
function_name = "RedshiftUtilsSnapshotManager"

def schedule(args):
    config = args[0]
    
    cw = boto3.client('events', region_name=os.environ['AWS_REGION'])
    lb = boto3.client('lambda', region_name=os.environ['AWS_REGION'])

    # read the supplied configuration file
    config_file = open(config, 'r')
    json_config = json.load(config_file)
    namespace = json_config['namespace']
    
    rulename = "%s-%s-%s-mins" % (function_name, namespace, schedule_mins)
    
    print "\nChecking whether rule '%s' already exists..." % (rulename)
    
    rule_exists = True
    action = 'Created'
    
    try:
        existing_rule = cw.describe_rule(Name=rulename)
        action = 'Updated'
        print "Rule already exists, so updating..."
    except Exception as e:
        rule_exists = False
        print "Rule does not exist, so creating..."
        
    # create the cloudwatch event rule
    rule = cw.put_rule(Name=rulename,
    ScheduleExpression='rate(%s minutes)' % (schedule_mins),
    State='ENABLED')
    
    rule_arn = rule["RuleArn"]
    
    print "%s CloudWatch rule definition: %s" % (action, rule_arn)
    
    if not rule_exists: 
    
        # trust CW to invoke function
        permission = lb.add_permission(FunctionName=function_name, 
        StatementId=shortuuid.uuid(), 
        Action='lambda:InvokeFunction', 
        Principal='events.amazonaws.com', 
        SourceArn=rule_arn)
        
        print "Updated Lambda policy so that it can be triggered by CloudWatch rule."
        
        # add the CW target
        function_arn = lb.get_function(FunctionName=function_name)["Configuration"]["FunctionArn"]
        
        target = {}
        target["Id"] = "%s" % (namespace)
        target["Arn"] = function_arn
        target["Input"] = json.dumps(json_config)
        target = cw.put_targets(Rule=rulename, Targets=[target])
           
        print "Linked Lambda function %s to rule.\n" % (function_arn)
    
if __name__ == "__main__":
   schedule(sys.argv[1:])    
