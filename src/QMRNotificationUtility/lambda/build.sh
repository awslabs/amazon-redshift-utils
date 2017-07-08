#!/bin/bash

VERSION=1.4

ARCHIVE=qmr-action-notification-utility-$VERSION.zip

TMP_DIR=$(mktemp -d /tmp/lambda-XXXXXX)
VIRTUALENV=$TMP_DIR/virtual-env
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

virtualenv $VIRTUALENV
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

cp $SCRIPT_DIR/lambda_function.py $TMP_DIR/lambda_function.py

pushd $TMP_DIR
zip -r $SCRIPT_DIR/dist/$ARCHIVE lib/
zip -r $SCRIPT_DIR/dist/$ARCHIVE lambda_function.py
popd

