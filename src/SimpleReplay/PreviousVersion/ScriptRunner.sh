#!/bin/bash
# Starts multiple sessions/threads to execute a list of SQL scripts

#  $1 Number of Concurrent Threads
#  $2 List of scripts to run


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

#PSQL Client Setup:
#export PGPASSWORD=
#export PGUSER=
#export PGHOST=
#export PGPORT=

# Functions
show_help()
{
   echo "Usage: $0 [-H host] [-P port] [-U user] [-W password] <Number of Threads> [<script list>] "
}

run()
{
# Add the list of scripts to run on a file names sqls
#
# $1 = Concurrency
# $2 = run (to diferentiate runs)
# $3 = sqls file
# $4 = fork indicator - call came from the fork runner

sqls=$3

logdate=`date '+%Y%m%d%H%M%S'`
tot_sqls=`wc $sqls | awk '{print $1}'`
pos=0
if [ "$4" != "fork" ]; then
  echo 0 > pos1; echo 0 > pos2;
fi

while [ $pos -le $tot_sqls ]
do

 #share the sql queue across forks
 safe="0"
 while [ $safe -lt "1" ]
 do
  mkdir ./lock$2 2> /dev/null; lock_state=$?
  # Make sure it is locked so there isn't race condition.
  if [ "$lock_state" = "0" ]; then
   pos=`cat pos$2`; pos=$((pos + 1)); echo $pos > pos$2
   safe=2; rm -rf lock$2
  else
   sleep 1
  fi
 done

 # Execute the SQL on Redshift via psql client
 if [ $pos -le $tot_sqls ]; then
  # Parse the next SQL Script
  script=`cat $sqls | sed -n ${pos}p`
  if [ -z "${script// }" ]; then
     continue
  fi
  #log_out=log_out_thread_${2}_$1_$logdate
  # if you want to send result-set to /dev/null
  log_out=/dev/null
  log_status=log_status_tread_${2}
  # Uses the same DB os the original file
  db=`basename $script | cut -d '-' -f 1`
  echo "`date '+%Y%m%d%H%M%S'` " start $script >> $log_out
  ds=$(date '+%s')
  psql -v ON_ERROR_STOP=1 -f $script -p $PGPORT $db >> $log_out 2>&1
  succ=$?
  de=$(date '+%s')
  # Dump some information such as exit status and runtime to log files
  echo "`date '+%Y%m%d%H%M%S'` " done $script >> log_out
  echo "$logdate $1 $script runtime " $succ $((de - ds)) >> $log_status
 fi
done
}
# End Functions

if [ "$#" -le 0 ]; then
  show_help
  exit 0
fi

# Process command line variables

OPTIND=1         # Reset in case getopts has been used previously in the shell.

while getopts "h?H:P:U:W:" opt; do
    case "$opt" in
    h|\?)
        show_help
        exit 0
        ;;
    H)  PGHOST=$OPTARG
        ;;
    P)  PGPORT=$OPTARG
        ;;
    U)  PGUSER=$OPTARG
        ;;
    W)  PGPASSWORD=$OPTARG
        ;;
    esac
done

shift $((OPTIND-1))

[ "$1" = "--" ] && shift

# End of command line variable

threads=${1:-1}
sqls=${2:-sqls}

echo -n STARTED $threads Thread\(s\) running $sqls on ' '
date

echo 0 > pos1; echo 0 > pos2;
for i in $(seq 1 $1)
 do
  echo "started instance no: $i"
  run 1 $i $sqls fork 2> error_${1}_1 &
done
wait
echo -n ENDED ' '
date

