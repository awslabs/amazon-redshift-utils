#! /bin/bash
# Allow to suplicated CTAS to run on the same transaction
: '
* Copyright 2018, Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0

10/26/2016 : Initial Release.
'
sed -i -e "s/^CREATE TEMPORARY TABLE \\(.*\) /DROP TABLE IF EXISTS \1\; CREATE TEMPORARY TABLE \1 /g"
