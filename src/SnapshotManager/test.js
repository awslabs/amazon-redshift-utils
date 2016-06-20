/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var lambda = require('./index');

event = {
	"clusterIdentifier" : "energy-demo",
	"region" : "eu-west-1",
	"namespace" : "test",
	"snapshotIntervalHours" : 2,
	"snapshotRetentionDays" : 7
};

function context() {};
context.done = function(status, message) {
	console.log("Context Closure Status: " + JSON.stringify(status) + "\n" + JSON.stringify(message));

	if (status && status !== null) {
		process.exit(-1);
	} else {
		process.exit(0);
	}
};

lambda.handler(event, context);