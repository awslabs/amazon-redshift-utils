#!/bin/bash

ARCHIVE=lambda-redshift-util-runner.zip

if [ -f ../$ARCHIVE ]; then
	rm ../$ARCHIVE
fi 
mkdir lib
cp ../ColumnEncodingUtility/analyze-schema-compression.py ./lib/analyze_schema_compression.py

zip -r ../$ARCHIVE *.py config.json lib
