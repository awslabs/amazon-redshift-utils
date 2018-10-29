#! /bin/bash
# Order replay by session start
: '
* Copyright 2018, Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Amazon Software License (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* http://aws.amazon.com/asl/
*
* This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.

10/26/2016 : Initial Release.
'
for f in `cat $1`; do echo -n $f; cat $f | head -1 | cut -c13-; done | sort -k 2,3 | cut -d ' ' -f 1
