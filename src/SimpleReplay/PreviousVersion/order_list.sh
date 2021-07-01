#! /bin/bash
# Order replay by session start
: '
* Copyright 2018, Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: Apache-2.0

10/26/2016 : Initial Release.
'
for f in `cat $1`; do echo -n $f; cat $f | head -1 | cut -c13-; done | sort -k 2,3 | cut -d ' ' -f 1
