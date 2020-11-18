#!/bin/bash
#
# Add the list of scripts to run on a file named by $1

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

10/26/2018 : Initial Release.
'

#PSQL Client Setup:
#export PGPASSWORD=
#export PGUSER=
#export PGHOST=
#export PGPORT= 


# Functions Definition
show_help()
{
   echo "Usage: $0 [-b <begin time>] [-e <end time>|-r <seconds to run>] [-H <host>] [-P port][-U user] [-W password][<script list>] [<target concurrency>]"
}

datediff() 
{
    d1=$(date -u -d "$1" +"%s")
    d2=$(date -u -d "$2" +"%s")
    echo $(( (d2 - d1) ))
}

replay_one()
{
	logdate=$1
	script=$2
	# Uses the same DB os the original file
	db=`basename $script | cut -d '-' -f 1`

	logfile=log_out_replay_`basename $script .sql`_$logdate.log
	#output=$logfile
	# if you want to send result-set to /dev/null
	output=/dev/null

	echo Starting $script

	# Execute the SQL on Redshift via psql client
	  echo "`date '+%Y%m%d%H%M%S'` " start $script >> $logfile
	  ds=$(date '+%s')
	  psql -v ON_ERROR_STOP=1 -f $script $db >> $output 2>&1
	  succ=$?
	  de=$(date '+%s')
	  # Dump some information such as exit status and runtime to log files
	  echo "`date '+%Y%m%d%H%M%S'` " done $script >> $logfile
	  echo "$logdate $script runtime " $succ $((de - ds)) >> log_status_replay

	echo Completed $script
}

# End of Functions

if [ "$#" -le 0 ]; then
  show_help
  exit 0
fi

# Process command line variables

OPTIND=1         # Reset in case getopts has been used previously in the shell.

while getopts "h?b:e:r:H:P:U:W:" opt; do
    case "$opt" in
    h|\?)
        show_help
        exit 0
        ;;
    b)  begin_time=$OPTARG
        ;;
    e)  end_time=$OPTARG
        ;;
    r)  run_time=$OPTARG
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

sqls=${1:-sqls}
target_concurrency=${2:-0}
run_time=${run_time:-0}

# End of command line variables

echo "`date '+%Y%m%d%H%M%S'` "  Start Replay

logdate=`date '+%Y%m%d%H%M%S'`
tot_sqls=`wc $sqls | awk '{print $1}'`
pos=1

# Setup the beginning
first_file=`cat $sqls | head -1`
first_date=`cat ${first_file} | head -1 | cut -c14-`
last_file=`cat $sqls | tail -1`
last_date=`cat ${last_file} | head -1 | cut -c14-`

begin_time=${begin_time:-$first_date}
end_time=${end_time:-$last_date}

SECONDS=0

while [ $pos -le $tot_sqls ]
do
  # Parse the next SQL Script
  script=`cat $sqls | sed -n ${pos}p`
  # Figure out wait time
  script_date=`cat ${script} | head -1 | cut -c14-`
  # Check for begin time
  run_offset=`datediff "$script_date" "$begin_time"`
  if [ $run_offset -gt 0 ]; then
     echo Skipping $script
     pos=$((pos + 1))
     first_date=$script_date
     continue
  fi
  # check for End time
  end_offset=`datediff "$script_date" "$end_time"`
  if [ $end_offset -lt 0 ]; then
     echo End Run on $end_time
     break
  fi
  # check for run time
  if [ $run_time -gt 0 ] && [ $SECONDS -gt $run_time ]; then
     echo End Run on runtime $run_time
     break
  fi
  # End of run time checks
  sleep=`datediff "$first_date" "$script_date"`
  sleep=$((sleep - SECONDS))
  concurrency=`ps -efww | grep 'psql -v ON_ERROR_STOP=1 -f' | grep -v grep | wc -l`
  if [ $sleep -le 0 ]; then
     sleep=0
   else
     if [ $target_concurrency -eq 0 ]; then
       echo Sleeping $sleep
       sleep $sleep
     elif [ $concurrency -ge $target_concurrency ]; then
       step_sleep=2
       sleep_sum=0
       echo -n Sleeping " "
       while [ $sleep_sum -le $sleep ]
       do
          echo -n $step_sleep " "
          sleep $step_sleep
          sleep_sum=$((sleep_sum + 2))
          concurrency=`ps -efww | grep 'psql -v ON_ERROR_STOP=1 -f' | grep -v grep | wc -l`
          if [ $concurrency -le $target_concurrency ]; then
            echo -n  Break
            break
          fi
        done
        echo Done Sleep
     else
       echo Skipping Sleep of $Sleep Conc:$concurrency Target:$target_concurrency
     fi
   fi
   replay_one "$logdate" "$script" &
   # Move along
   pos=$((pos + 1))
done
wait

echo "`date '+%Y%m%d%H%M%S'` " Completed Replay Runtime: $SECONDS
