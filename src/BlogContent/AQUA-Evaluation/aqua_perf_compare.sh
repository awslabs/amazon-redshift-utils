#!/bin/bash

#############################################################

# This script compare  RS execution time with/without AQUA  and presents CSV output file
# Arguments are: 
#           -h PGHOST
#           -p PGPORT
#           -U PGUSR
#           -d PGDATABASE
## sample run with parameters below :
# ./aqua_perf_compare.sh -h 1.11.11.111 -p 5439 -d testDB -U test_user "
###########################################################

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
#TO: Validate ALL connection  inputs
if [[ -z $PGHOST ]] || [[ -z $PGPORT ]] || [[ -z $PGDATABASE ]] || [[ -z $PGUSER ]] ; then
    echo "connection parameters required and cannot be empty, please enter correct  hostname, dbname, port-number, username to connect"      
    exit -1
fi
# using the start time and end time logged during query execution
if [ -s workload_datetime.txt ]; then
    STARTTIME=$(head -n 1  workload_datetime.txt)
fi
if [ -s workload_datetime.txt ]; then
    ENDTIME=$(tail  -n 1  workload_datetime.txt)
fi
CAPTUREQUERY=$(cat <<QUERYMARKER 
    with aqua_run as 
         (
            select svl.query, q.starttime
            from stl_scan svl
                       join stl_query q using (query)
            where svl.perm_table_name like '%Aqua%'
                    and q.aborted = 0
                    and q.starttime >= to_timestamp('$STARTTIME','YYYY-MM-DD HH:MI:SS')
                    and q.endtime <=  to_timestamp('$ENDTIME','YYYY-MM-DD HH:MI:SS')
            group by svl.query, q.starttime
          ),
    aqua_run_md5 as 
          (
            select  aq.query,
                    md5(listagg(qt.text) within group (order by sequence)) md5,
                    (max(w.total_exec_time / 1000000.0)) as rs_aqua_exec_time,
                     Row_number() OVER (PARTITION BY md5(listagg(qt.text) within group (order by sequence)) ORDER BY aq.starttime desc) as r_num
            from aqua_run aq
                        join stl_querytext qt using (query)
                        join stl_wlm_query w using (query)
            group by aq.query, aq.starttime
            ),
    rs_run as
           (
             select qt.query rs_query,
                    max(w.total_exec_time) / 1000000.0  total_exec_time,
                    md5(listagg(qt.text) within group (order by sequence)) md5,
                    Row_number() OVER (PARTITION BY md5(listagg(qt.text) within group (order by sequence)) ORDER BY q.starttime desc) as r_num,
                    q.starttime
             from stl_querytext qt
                         join stl_wlm_query w using (query)
                         join stl_query q using (query)
             where qt.sequence < 327
                  and qt.query not in (select query from aqua_run)
                  and q.aborted = 0
                  and starttime >=  to_timestamp('$STARTTIME','YYYY-MM-DD HH:MI:SS')
                  and endtime <=    to_timestamp('$ENDTIME','YYYY-MM-DD HH:MI:SS')
            group by qt.query, q.starttime
            ),

                rs.rs_query,
                aq.rs_aqua_exec_time rs_aqua_exec_time_in_S,
                rs.rs_exec_time rs_exec_time_in_S,
                rs.rs_exec_time / rs_aqua_exec_time speedup
     from aqua_execution_time aq
         join Rs_execution_time rs on aq.md5 = rs.md5 and aq.r_num = rs.r_num
         join stl_wlm_query w on w.query = aq.query
     group by aq.query, rs_query, aq.md5, rs.rs_exec_time,aq.rs_aqua_exec_time, rs.r_num, aq.r_num
     order by speedup desc;
QUERYMARKER
)

RUNTIMEQUERY=$(cat <<QQ
    with aqua_run as
         (
            select svl.query, q.starttime
             from stl_scan svl
                    join stl_query q using (query)
            where svl.perm_table_name like '%Aqua%'
                    and q.aborted = 0
                    and q.starttime >= to_timestamp('$STARTTIME','YYYY-MM-DD HH:MI:SS')
                    and q.endtime <=  to_timestamp('$ENDTIME','YYYY-MM-DD HH:MI:SS')
             group by svl.query, q.starttime
         ),

        (
            select aq.query,
                   md5(listagg(qt.text) within group (order by sequence)) md5,
                   (max(w.total_exec_time / 1000000.0)) as rs_aqua_exec_time,
                   Row_number()  OVER (PARTITION BY md5(listagg(qt.text) within group (order by sequence)) ORDER BY aq.starttime desc) as r_num
            from aqua_run aq
                     join stl_querytext qt using (query)
                     join stl_wlm_query w using (query)
           group by aq.query, aq.starttime
        ),

        (
          select qt.query rs_query,
                 max(w.total_exec_time) / 1000000.0  total_exec_time,
                 md5(listagg(qt.text) within group (order by sequence)) md5,
                 Row_number() OVER (PARTITION BY md5(listagg(qt.text) within group (order by sequence)) ORDER BY q.starttime desc) as r_num,
                 q.starttime
          from stl_querytext qt
                   join stl_wlm_query w using (query)
                   join stl_query q using (query)
           where qt.sequence < 327
                 and qt.query not in (select query from aqua_run)
                 and q.aborted = 0
                 and starttime >=  to_timestamp('$STARTTIME','YYYY-MM-DD HH:MI:SS')
                 and endtime <=    to_timestamp('$ENDTIME','YYYY-MM-DD HH:MI:SS')
           group by qt.query, q.starttime
         ),

    exit $CONN_STATUS
fi
if [[  -z $RESULT ]]; then
    echo "Please run the script with date interval on the test cluster where  you have executed queries with AQUA on and off"
else 
    echo 'Query ID Redshift with AQUA turned on,Query ID Redshift with AQUA turned off,Runtime in seconds - AQUA turned on,Runtime in seconds - AQUA turned off,Speedup(colum C/Column B)' > aqua_benefit.csv
    echo $RESULT | tr ' ' '\n' | tr '|' ',' >> aqua_benefit.csv
BOOST=$(echo "$RESULT" | awk -F'|' -v RS='\n' '{print $NF}' | sort -n);
BOOST_MIN_RUNTIME=$(echo "$BOOST" | sed -n '1p')
BOOST_MAX_RUNTIME=$(echo "$BOOST" | sed -n '$p')

fi
