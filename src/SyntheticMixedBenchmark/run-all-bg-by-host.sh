#!/bin/bash
#set -e
echo "starting"
echo `date`

hostname=$1
port=$2
database=$3
masteruser=$4
masterpass=$5
awskeyid=$6
awssecretkey=$7

echo "cluster should be ready, now submit the queries"

# 3 copy jobs. 3 is hard coded within the python script
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey COPY copyuser Abcd1234 tpch3t 3600 &

#16 dashboard queires
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DASH dashboarduser Abcd1234 tpch100g 2 &

#4 data science queries
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DATASCIENCE datascienceuser Abcd1234 tpch3t 1800 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DATASCIENCE datascienceuser Abcd1234 tpch3t 1800 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DATASCIENCE datascienceuser Abcd1234 tpch3t 1800 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey DATASCIENCE datascienceuser Abcd1234 tpch3t 1800 &

#6 report queries
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey REPORT reportuser Abcd1234 tpch3t 900 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey REPORT reportuser Abcd1234 tpch3t 900 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey REPORT reportuser Abcd1234 tpch3t 900 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey REPORT reportuser Abcd1234 tpch3t 900 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey REPORT reportuser Abcd1234 tpch3t 900 &
nohup python3 ParallelExecute.py $hostname $port $database $masteruser $masterpass $awskeyid $awssecretkey REPORT reportuser Abcd1234 tpch3t 900 &

exit 0
