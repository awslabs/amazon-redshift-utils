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

# Install required python libraries
pip install -r $SCRIPT_DIR/requirements.txt -t $TMP_DIR/lib

if [ -f $SCRIPT_DIR/dist/$ARCHIVE ]; then
    echo "Removing existing Archive ../dist/$ARCHIVE"
    rm -Rf $SCRIPT_DIR/dist/$ARCHIVE
fi

if [ ! -d $SCRIPT_DIR/dist ]; then
    mkdir $SCRIPT_DIR/dist
fi


# import the column encoding utility
if [ ! -d $TMP_DIR/lib/ColumnEncodingUtility ]; then
        mkdir $TMP_DIR/lib/ColumnEncodingUtility
fi

cp ../ColumnEncodingUtility/analyze-schema-compression.py $TMP_DIR/lib/ColumnEncodingUtility/analyze_schema_compression.py
echo "Imported Column Encoding Utility"

# import the Analyze/Vacuum utility
if [ ! -d $TMP_DIR/lib/AnalyzeVacuumUtility ]; then
        mkdir $TMP_DIR/lib/AnalyzeVacuumUtility
fi

cp ../AnalyzeVacuumUtility/lib/analyze_vacuum.py $TMP_DIR/lib/AnalyzeVacuumUtility/analyze_vacuum.py
echo "Imported Analyze/Vacuum Utility"

# import the SystemTablePersistence utility
if [ ! -d $TMP_DIR/lib/SystemTablePersistence ]; then
        mkdir $TMP_DIR/lib/SystemTablePersistence
fi

cp ../SystemTablePersistence/snapshot_system_stats.py $TMP_DIR/lib/SystemTablePersistence/snapshot_system_stats.py
cp -R ../SystemTablePersistence/lib $TMP_DIR/lib/SystemTablePersistence
echo "Imported System Table Persistence Utility"

if [ ! -d $TMP_DIR/lib/WorkloadManagementScheduler ]; then
        mkdir $TMP_DIR/lib/WorkloadManagementScheduler
fi

cp ../WorkloadManagementScheduler/wlm_scheduler.py $TMP_DIR/lib/WorkloadManagementScheduler/wlm_scheduler.py
echo "Imported Workload Management"

if [ ! -d $TMP_DIR/lib/amazon-redshift-monitoring ]; then
    cd $TMP_DIR/lib && git clone https://github.com/awslabs/amazon-redshift-monitoring
else
    cd $TMP_DIR/lib/amazon-redshift-monitoring && git pull
fi
echo "Imported Redshift Advance Monitoring"

# Return to the starting directory
cd $SCRIPT_DIR

# Build the combined lambda package in the virtualenv
# Copy the python files from RedshiftAutomation to the temp directory

find $SCRIPT_DIR -maxdepth 1 -name \*.py -exec cp {} $TMP_DIR \;

# Include the common files from the root directory
find $SCRIPT_DIR/../ -maxdepth 1 -name \*.py -exec cp {} $TMP_DIR \;

# Zip up the python scripts and libraries for the package
cd $TMP_DIR

# Create the zip file with the lib packages
zip -r $SCRIPT_DIR/$ARCHIVE ./lib

# Add the python files
find $TMP_DIR -maxdepth 1 -name \*.py -exec zip -uj $SCRIPT_DIR/$ARCHIVE {} \;

echo "Built Lambda package $SCRIPT_DIR/$ARCHIVE"

