#!/bin/bash
# set -x

version=1.3
ARCHIVE=dist/lambda-redshift-util-runner-$version.zip

if [ -f $ARCHIVE ]; then
	rm $ARCHIVE
fi

if [ ! -d dist ]; then
    mkdir dist
fi

if [ ! -d lib ]; then
	mkdir lib
fi

# import the column encoding utility
if [ ! -d lib/ColumnEncodingUtility ]; then
	mkdir lib/ColumnEncodingUtility
fi

cp ../ColumnEncodingUtility/analyze-schema-compression.py lib/ColumnEncodingUtility/analyze_schema_compression.py
echo "Imported Column Encoding Utility"

# import the Analyze/Vacuum utility
if [ ! -d lib/AnalyzeVacuumUtility ]; then
	mkdir lib/AnalyzeVacuumUtility
fi

cp ../AnalyzeVacuumUtility/lib/analyze_vacuum.py lib/AnalyzeVacuumUtility/analyze_vacuum.py
echo "Imported Analyze/Vacuum Utility"

# import the SystemTablePersistence utility
if [ ! -d lib/SystemTablePersistence ]; then
	mkdir lib/SystemTablePersistence
fi

cp ../SystemTablePersistence/snapshot_system_stats.py lib/SystemTablePersistence/snapshot_system_stats.py
cp -R ../SystemTablePersistence/lib lib/SystemTablePersistence
echo "Imported System Table Persistence Utility"

if [ ! -d lib/amazon-redshift-monitoring ]; then
    cd lib && git clone https://github.com/awslabs/amazon-redshift-monitoring
else
    cd lib/amazon-redshift-monitoring && git pull
fi
echo "Imported Redshift Advance Monitoring"

cd -

# build the combined lambda package
zip -r $ARCHIVE *.py config.json ../aws_utils.py ../config_constants.py lib/AnalyzeVacuumUtility lib/ColumnEncodingUtility lib/SystemTablePersistence lib/pg8000 lib/shortuuid lib/amazon-redshift-monitoring/redshift_monitoring.py lib/amazon-redshift-monitoring/sql/ lib/amazon-redshift-monitoring/monitoring-queries.json
