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
    function_arn = args[1].replace('"', '')
    cw = boto3.client('events', region_name=os.environ['AWS_REGION'])

    # read the supplied configuration file
    config_file = open(config, 'r')
    json_config = json.load(config_file)
    namespace = json_config['namespace']
    
    rulename = "%s.%s-%s-mins" % (function_name, namespace, schedule_mins)
    
    # create the cloudwatch event rule
    rule = cw.put_rule(Name=rulename,
    ScheduleExpression='rate(%s minutes)' % (schedule_mins),
    State='ENABLED')
    
    print "Created CloudWatch Rule Definition %s\n" % (rule["RuleArn"])
    
    # add the CW target
    target = {}
    target["Id"] = "%s" % (namespace)
    target["Arn"] = function_arn
    target["Input"] = json.dumps(json_config)
    target = cw.put_targets(Rule=rulename, Targets=[target])
    
    print "Linked Lambda Function %s to Rule\n" % (function_arn)
    
if __name__ == "__main__":
   schedule(sys.argv[1:])    
