/* jshint node: true */
/* jshint -W097 */
/*
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
'use strict';

var constants = require("./constants");

var setRegion = process.env['AWS_REGION'];
if (!setRegion) {
	console.log("Setting default region us-east-1");
	setRegion = "us-east-1";
}
var AWS = require("aws-sdk");
AWS.config.apiVersions = {
	redshift : '2012-12-01'
};
AWS.config.update({
	region : setRegion
});

var redshift = new AWS.Redshift();
var async = require('async');
var moment = require('moment');

function getFriendlyDate(t) {
	return t.format(constants.dateFormat);
}

function getSnapshotId(config) {
	return config.namespace + "-" + config.targetResource + "-" + getFriendlyDate(moment());
}

function getTags(config, forDate) {
	var tags = [ {
		Key : constants.createdByName,
		Value : constants.createdByValue
	}, {
		Key : constants.namespaceTagName,
		Value : config.namespace
	} ];

	if (forDate) {
		tags.push({
			Key : constants.createdAtName,
			Value : getFriendlyDate(forDate)
		});
	}
	return tags;
}

// function to query for snapshots created since the configuration start time
function getSnapshots(config, callback) {

	var snapStartTime;
	var intervalExpression;

	if (config.snapshotInterval) {
		snapStartTime = moment().subtract(config.snapshotInterval.duration, config.snapshotInterval.units);
		intervalExpression = config.snapshotInterval.duration + " " + config.snapshotInterval.units;
	} else {
		snapStartTime = moment().subtract(config.snapshotIntervalHours, 'hours');
		intervalExpression = config.snapshotIntervalHours + " hours";
	}

	exports.getSnapshotsInInterval(config.targetResource, config.namespace, snapStartTime, undefined, undefined,
			function(err, snapshots) {
				if (err) {
					callback(err);
				} else {
					console.log("Resolved " + snapshots.length + " Snapshots in the last " + intervalExpression);

					callback(null, config, snapshots);
				}
			});
}
exports.getSnapshots = getSnapshots;

function getSnapshotsInInterval(targetResource, namespace, startTime, endTime, extraSearchCriteria, callback) {
	var startSearchLabel = startTime ? startTime.format() : "any";
	var endSearchLabel = endTime ? endTime.format() : "any";

	console.log("Requesting '" + namespace + "' Snapshots for " + targetResource + " in interval (" + startSearchLabel
			+ " - " + endSearchLabel + ")");

	var describeParams = {
		ClusterIdentifier : targetResource,
		// only search for snapshots in the current namespace
		TagKeys : [ constants.namespaceTagName ],
		TagValues : [ namespace ]
	};

	// add time search criteria
	if (startTime) {
		describeParams.StartTime = startTime.toDate();
	}

	if (endTime) {
		describeParams.EndTime = endTime.toDate();
	}

	// add any provided extra search items
	if (extraSearchCriteria) {
		Object.keys(extraSearchCriteria).map(function(item) {
			describeParams[item] = extraSearchCriteria[item];
		});
	}

	// console.log(JSON.stringify(describeParams));

	redshift.describeClusterSnapshots(describeParams, function(err, data) {
		// got a set of snapshots, or not, so call the callback with the snap list
		if (err) {
			callback(err);
		} else {
			callback(null, data.Snapshots);
		}
	});
}
exports.getSnapshotsInInterval = getSnapshotsInInterval;

// function to create a manual snapshot
function createSnapshot(config, callback) {
	var now = moment();

	var newSnapshotId = getSnapshotId(config);

	console.log("Creating new Snapshot " + newSnapshotId + " for " + config.targetResource);

	var params = {
		ClusterIdentifier : config.targetResource,
		SnapshotIdentifier : newSnapshotId,
		Tags : getTags(config, now)
	};

	redshift.createClusterSnapshot(params, function(err, data) {
		if (err) {
			callback(err);
		} else {
			console.log("Created new Manual Snapshot " + data.Snapshot.SnapshotIdentifier);
			callback(null, config);
		}
	});
}
exports.createSnapshot = createSnapshot;

// function to convert an automatic snapshot into a manual snapshot
function convertAutosnapToManual(config, snapshot, callback) {
	console.log("Converting Automatic Snapshot " + snapshot.SnapshotIdentifier + " to manual for long term retention");

	redshift.copyClusterSnapshot({
		SourceSnapshotIdentifier : snapshot.SnapshotIdentifier,
		TargetSnapshotIdentifier : getSnapshotId(config),
	}, function(err, data) {
		if (err) {
			callback(err);
		} else {
			// tag the converted snapshot
			var params = {
				ResourceName : data.SnapshotIdentifier,
				Tags : getTags(config)
			};
			redshift.createTags(params, function(err, data) {
				if (err) {
					callback(err);
				} else {
					console.log("Converted latest Snapshot " + snapshot.SnapshotIdentifier + " to new Manual Snapshot "
							+ data.SnapshotIdentifier);
					callback(null, config);
				}
			});
		}
	});
}
exports.convertAutosnapToManual = convertAutosnapToManual;

// function to cleanup snapshots older than the specified retention period
function cleanupSnapshots(config, callback) {
	if (!config.snapshotRetentionDays && !config.snapshotRetention) {
		// customer may not have configured a snapshots retention - ok
		console.log("No Snapshot retention limits configured - all Snapshots will be retained");
		callback(null);
	} else {

		var snapEndTime;
		var intervalExpression;

		if (config.snapshotRetention) {
			snapEndTime = moment().subtract(config.snapshotRetention.duration, config.snapshotRetention.units);
			intervalExpression = config.snapshotRetention.duration + " " + config.snapshotRetention.units + " ("
					+ snapEndTime.format() + ")";
		} else {
			snapEndTime = moment().subtract(config.snapshotRetentionDays, 'days');
			intervalExpression = config.snapshotRetentionDays + " days (" + snapEndTime.format() + ")";
		}

		console.log("Cleaning up '" + config.namespace + "' Snapshots older than ");

		exports.getSnapshotsInInterval(config.targetResource, config.namespace, undefined, snapEndTime, {
			SnapshotType : "manual"
		}, function(err, snapshots) {
			if (err) {
				callback(err);
			} else {
				if (snapshots && snapshots.length > 0) {
					console.log("Cleaning up " + snapshots.length + " previous Snapshots");

					async.map(snapshots, function(item, mapCallback) {
						console.log("Deleting Snapshot " + item.SnapshotIdentifier);

						// delete this manual snapshot
						redshift.deleteClusterSnapshot({
							SnapshotIdentifier : item.SnapshotIdentifier,
							SnapshotClusterIdentifier : config.targetResource
						}, function(err, data) {
							if (err) {
								mapCallback(err);
							} else {
								callback(null);
							}
						});
					}, function(err, results) {
						if (err) {
							callback(err);
						} else {
							callback(null);
						}
					});
				} else {
					// there are no snapshots older than the retention period - so all ok
					console.log("No old snapshots found to clear");
					callback(null);
				}
			}
		});
	}
}
exports.cleanupSnapshots = cleanupSnapshots;

/*
 * function which determines if we need to create or convert snapshots based on
 * the snapshots that currently exist
 */
function createOrConvertSnapshots(config, snapshotList, callback) {
	if (snapshotList.length === 0) {
		// create a new snapshot
		createSnapshot(config, callback);
	} else {
		// convert the latest auto snapshot created within the specified
		// period to a manual snapshot
		var manualExists = false;
		snapshotList.map(function(item) {
			if (item.SnapshotType === "manual") {
				manualExists = true;
			}
		});
		// only do an auto conversion if there are no manual snapshots in
		// the period
		if (!manualExists) {
			convertAutosnapToManual(config, snapshotList[snapshotList.length - 1], callback);
		} else {
			callback(null, config);
		}
	}
}
exports.createOrConvertSnapshots = createOrConvertSnapshots;

function validateConfig(config, callback) {
	var error;

	if (!config.namespace || config.namespace === "") {
		error = "You must provide a configuration namespace";
	}

	if (config.snapshotInterval && constants.validTimeUnits.indexOf(config.snapshotInterval.units) == -1) {
		error = "Snapshot Interval Units must be one of: " + JSON.stringify(constants.validTimeUnits);
	}

	if (config.snapshotRetention && constants.validTimeUnits.indexOf(config.snapshotRetention.units) == -1) {
		error = "Snapshot Retention Units must be one of: " + JSON.stringify(constants.validTimeUnits);
	}

	callback(error);
}
exports.validateConfig = validateConfig;

// processor entry point
function run(config, callback) {
	var checksPassed = true;
	var error;
	if (!config.targetResource) {
		checksPassed = false;
		error = "Unable to resolve target resource from provided config.targetResource";
	}

	if (!config.namespace) {
		checksPassed = false;
		error = "Configuration namespace must be provided";
	}

	console.log("Configuration Provided: " + JSON.stringify(config));

	if (!checksPassed) {
		console.log(JSON.stringify(config));
		callback(error);
	} else {
		validateConfig(config, function(err) {
			if (err) {
				callback(err);
			} else {
				async.waterfall([
				// check for whether we have a snapshot taken within the required period
				getSnapshots.bind(undefined, config),
				// process the list of automatic snapshots, and determine if we need to
				// create additional snaps
				createOrConvertSnapshots.bind(undefined),
				// now cleanup the existing snapshots
				cleanupSnapshots.bind(undefined) ], function(err) {
					callback(err);
				});
			}
		});
	}
}
exports.run = run;