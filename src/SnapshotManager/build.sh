#!/bin/bash
set +x

ver=`cat package.json | grep version | cut -d: -f2 | sed -e "s/\"//g" | sed -e "s/ //g" | sed -e "s/\,//g"`

# defaults
function_name=RedshiftUtilsSnapshotManager
zipfile=RedshiftSnapshotManager-$ver.zip

 zip -r $zipfile *.js package.json node_modules/ && mv $zipfile dist