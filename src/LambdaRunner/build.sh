#!/bin/bash
# set -x

version=1.1
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

if [ ! -d lib/ColumnEncodingUtility ]; then
	mkdir lib/ColumnEncodingUtility
fi

cp ../ColumnEncodingUtility/analyze-schema-compression.py lib/ColumnEncodingUtility/analyze_schema_compression.py

if [ ! -d lib/AnalyzeVacuumUtility ]; then
	mkdir lib/AnalyzeVacuumUtility
fi

cp ../AnalyzeVacuumUtility/lib/analyze_vacuum.py lib/AnalyzeVacuumUtility/analyze_vacuum.py


zip -r $ARCHIVE *.py config.json lib
