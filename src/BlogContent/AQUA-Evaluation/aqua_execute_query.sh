#!/bin/bash
####################################################################################################################
#
# This script execute queries from capture.sql file generated earlier using
# aqua_capture_query.sh with AQUA turned On and Off
# Arguments are: 
#           -h PGHOST
#           -p PGPORT
#           -U PGUSR
#           -d PGDATABASE
#           -t PGCONNECT_TIMEOUT
# sample run with parameters below :
#./aqua_execute_query.sh -h 1.11.11.111 -p 5439 -d testDB -U test_user 
#######################################################################################################################
while getopts h:p:U:d:s:e:t:  option; do
        case "${option}" in
            h)
              export PGHOST=${OPTARG}
              ;;
            p)
              export PGPORT=${OPTARG}
              ;;
            d)
              export PGDATABASE=${OPTARG}
              ;;
            U)
              export PGUSER=${OPTARG}
              ;;
            t)
              export PGCONNECT_TIMEOUT=${OPTARG}
              ;;
        esac

done

read -s -p "Password to connect to Redshift cluster: " PGPASSWORD
export  PGPASSWORD=$PGPASSWORD

if [[ -z $PGCONNECT_TIMEOUT ]]; then
    export PGCONNECT_TIMEOUT=10
fi

if [[ -z $PGHOST ]] || [[ -z $PGPORT ]] || [[ -z $PGDATABASE ]] || [[ -z $PGUSER ]] ; then
    echo "connection parameters required and cannot be empty, please enter correct  hostname, dbname, port-number, username to connect"     
    exit -1
fi

if [ ! -s ./aqua_eligible_queries.sql  ]; then 
     echo "The aqua_eligible_queries.sql  seems empty, please capture aqua eligible queries using aqua_capture_query.sh then execute aqua_execute_query.sh"
else
 #log execution startime 
 date +"%Y-%m-%d %T" > workload_datetime.txt 
 #execute in redhshift three times
 echo "set activate_aqua to off;
       set enable_result_cache_for_session to off;" > capture_tmp.sql
 cat aqua_eligible_queries.sql >> capture_tmp.sql
 echo $capture_tmp.sql 
 for iter in {1..3}
     do
        echo Iteration with AQUA: $iter
        psql -q -f ./capture_tmp.sql -o cap.out > sql_error.log  2>&1 
        CONN_STATUS=$?
        if  [ $CONN_STATUS -eq 2 ]; then
            echo "Connection failed. Please try again with correct connection parameters"
            exit $CONN_STATUS
        fi
        SQL_ERROR=$(cat sql_error.log |  grep -i "error\|failed\|timeout" | wc -l )
        if [ $SQL_ERROR -ge  1 ]; then
            echo "Please check the sql_error.log for erros, once SQL error's fixed and then re-run aqua_execute_query.sh"
            echo $sql_error.log
            exit $SQL_ERROR
       fi
  done
  sleep 30
 #execute with AQUA turn on  three times
  echo "set activate_aqua to on;
        set enable_result_cache_for_session to off;" > tmp_capture.sql
  cat aqua_eligible_queries.sql >> tmp_capture.sql
  for iter in {1..3}
      do
        echo Iteration with AQUA off: $iter
        psql -q  -f ./tmp_capture.sql  -o cap.out  > sql_error.log  2>&1  
        CONN_STATUS=$?
        if [ $CONN_STATUS -eq 2 ]; then
            echo "Connection failed. Please try again with correct connection parameters"
            exit $CONN_STATUS
        fi
        SQL_ERROR=$(cat sql_error.log |  grep -i -q "error\|failed\|timeout" | wc -l )
        if [ $SQL_ERROR -ge  1  ]; then
            echo "Please check the sql_error.log for erros, once SQL error's fixed and then  re-run aqua_execute_query.sh"
            rm -f sql_error.log
            echo $sql_error.log
            exit $SQL_ERROR 
        fi
  done
 rm -f  capture_tmp.sql
 rm -f cap.out
 rm -f tmp_capture.sql
 echo "All Queries executed successfully with AQUA and without AQUA, please go ahead and run comparision scripts"
 date +"%Y-%m-%d %T" >> workload_datetime.txt
fi
