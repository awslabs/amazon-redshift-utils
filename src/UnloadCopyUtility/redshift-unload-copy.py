#!/usr/bin/env python
"""
Usage:

python redshift_unload_copy.py <config file> <region>


* Copyright 2017, Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Amazon Software License (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* http://aws.amazon.com/asl/
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
"""
import sys
import logging
import redshift_unload_copy
"""
 This file is only in place for backwards compatibility/callability
 It should be considered deprecated and it should not be used in new setups
 It should only be a proxy and code should reside in redshift_unload_copy.py
"""

if __name__ == "__main__":
    logging.warning("The entry point redhsift-unload-copy.py is deprecated, it is recommended to use "
                    "redshift_unload_copy.py which can be called with the same arguments.")
    redshift_unload_copy.main(sys.argv)
