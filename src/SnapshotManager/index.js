var snapshotManager = require("./snapshotManager");

exports.handler = function(event, context) {
	snapshotManager.run(event, function(err) {
		context.done(err);
	});
};