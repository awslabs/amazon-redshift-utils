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
	return config.namespace + "-" + config.clusterIdentifier + "-" + getFriendlyDate(moment());
}

function getTags(config, forDate) {
	return [ {
		Key : constants.createdByName,
		Value : constants.createdByValue
	}, {
		Key : constants.createdAtName,
		Value : getFriendlyDate(forDate)
	}, {
		Key : constants.namespaceTagName,
		Value : config.namespace
	} ];
}

// function to query for snapshots within the specified period
function getSnapshots(config, callback) {
	var snapStartTime = moment().subtract(config.snapshotIntervalHours, 'hours');

	console.log("Requesting Snapshots for " + config.clusterIdentifier + " since " + snapStartTime.format());

	redshift.describeClusterSnapshots({
		ClusterIdentifier : config.clusterIdentifier,
		StartTime : snapStartTime.toDate(),
		// only search for snapshots in the current namespace and created by this
		// tool
		TagKeys : [ constants.namespaceTagName ],
		TagValues : [ config.namespace ]
	}, function(err, data) {
		// got a set of snapshots, or not, so call the callback with the snap list
		// and the configuration
		if (err) {
			callback(err);
		} else {
			console.log("Resolved " + data.Snapshots.length + " Snapshots in the last " + config.snapshotIntervalHours
					+ " hours");

			callback(null, config, data.Snapshots);
		}
	});
}
exports.getSnapshots = getSnapshots;

// function to create a manual snapshot
function createSnapshot(config, callback) {
	var now = moment();

	var newSnapshotId = getSnapshotId(config);

	console.log("Creating new Snapshot " + newSnapshotId + " for " + config.clusterIdentifier);

	var params = {
		ClusterIdentifier : config.clusterIdentifier,
		SnapshotIdentifier : newSnapshotId,
		Tags : getTags(config, now)
	};

	redshift.createClusterSnapshot(params, function(err, data) {
		if (err) {
			callback(err);
		} else {
			console.log("Created New Manual Snapshot " + data.Snapshot.SnapshotIdentifier);
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
					console.log("Converted latest Snapshot " + snapshot.SnapshotIdentifier + " to new manual snapshot "
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
	// customer may not have configured a snapshots retention
	if (!config.snapshotRetentionDays) {
		console.log("No snapshot retention limits configured - all snapshots will be retained");
		callback(null);
	}

	var snapEndTime = moment().subtract(config.snapshotRetentionDays, 'days');

	console.log("Cleaning up snapshots older than " + config.snapshotRetentionDays + " days (" + snapEndTime.format()
			+ ")");

	redshift.describeClusterSnapshots({
		ClusterIdentifier : config.clusterIdentifier,
		EndTime : snapEndTime.toDate(),
		SnapshotType : "manual",
		TagKeys : [ constants.createdByName ],
		TagValues : [ constants.createdByValue ]
	}, function(err, data) {
		if (err) {
			callback(err);
		} else {
			if (data.Snapshots && data.Snapshots.length > 0) {
				console.log("Cleaning up " + data.Snapshots.length + " previous snapshots");

				async.map(data.Snapshots, function(item, mapCallback) {
					console.log("Deleting Snapshot " + item.SnapshotIdentifier);

					// delete this manual snapshot
					redshift.deleteClusterSnapshot({
						SnapshotIdentifier : item.SnapshotIdentifier,
						SnapshotClusterIdentifier : config.clusterIdentifier
					}, function(err, data) {
						if (err) {
							mapCallback(err);
						} else {
							console.log("Deleted Cluster Snapshot ");
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

	if (config.snapshotIntervalHours < 1) {
		error = "Minimum Snapshot Interval is 1 hour";
	}

	if (config.snapshotRetentionDays < 1) {
		error = "Minimum Snapshot Retention is 1 day";
	}

	if (!config.namespace || config.namespace === "") {
		error = "You must provide a configuration namespace";
	}
	callback(error);
}
exports.validateConfig = validateConfig;

// processor entry point
function run(config, callback) {
	var checksPassed = true;
	var error;
	if (!config.clusterIdentifier) {
		checksPassed = false;
		error = "Unable to resolve Cluster Identifier from provided configuration";
	}

	if (!config.namespace) {
		checksPassed = false;
		error = "Configuration namespace must be provided";
	}

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