#!/usr/bin/env bash

set -e

echo "Running user-last-login utility"

python UserLastLogin/user_last_login.py 
echo "done"
