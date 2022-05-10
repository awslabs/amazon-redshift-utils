#!/bin/bash

VERSION=1.6

ARCHIVE=dist/lambda-redshift-util-runner-$VERSION.zip

TMP_DIR=$(mktemp -d /tmp/redshift-automation-XXXXXX)
VIRTUALENV=$TMP_DIR/virtual-env
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Creating virtual environment in $VIRTUALENV"

python3 -m venv $VIRTUALENV
source $VIRTUALENV/bin/activate

set -euf -o pipefail

# add required dependencies
pip install -r $SCRIPT_DIR/requirements.txt -t $TMP_DIR/lib

if [ -f $SCRIPT_DIR/dist/$ARCHIVE ]; then
    echo "Removing existing Archive ../dist/$ARCHIVE"
    rm -Rf $SCRIPT_DIR/dist/$ARCHIVE
fi

if [ ! -d $SCRIPT_DIR/dist ]; then
    mkdir $SCRIPT_DIR/dist
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

if [ ! -d lib/WorkloadManagementScheduler ]; then
        mkdir lib/WorkloadManagementScheduler
fi

cp ../WorkloadManagementScheduler/wlm_scheduler.py lib/WorkloadManagementScheduler/wlm_scheduler.py
echo "Imported Workload Management"

if [ ! -d lib/amazon-redshift-monitoring ]; then
    cd lib && git clone https://github.com/awslabs/amazon-redshift-monitoring
else
    cd lib/amazon-redshift-monitoring && git pull
fi
echo "Imported Redshift Advance Monitoring"

cd -

# build the combined lambda package

echo "Building package $SCRIPT_DIR/$ARCHIVE"

pushd $TMP_DIR
zip -r $SCRIPT_DIR/$ARCHIVE lib/
zip -r $SCRIPT_DIR/$ARCHIVE *.py
popd

