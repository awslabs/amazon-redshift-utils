#!/bin/bash
# This script deploys components of demo Redshift ELT blog
# from Cloud9 environment


# The following steps need to be performed first:
# clone code repo
# git clone https://github.com/AndrewShvedRepo/redshiftcdcelt
# cd redshiftcdcelt/
# ./deploy.sh



# upgrade cdk
sudo npm install -g aws-cdk@1.72.0

# install dependencies
sudo pip3 install -r requirements.txt

# remove unnecessary docker images to free up space
docker rmi -f $(docker image ls -a -q)

# bootstrap ckd
cdk bootstrap

# deploy all stacks
cdk deploy --require-approval never "*" 


# install boto3
sudo pip3 install boto3

#start dms tasks
python dlpoc/start_dms_tasks.py
