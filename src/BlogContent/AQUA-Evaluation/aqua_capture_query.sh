#!/bin/bash

####################################################################################################################
#
# This script captures queries with potential for AQUA acceleration. saves
# output to your current working directory
# Arguments are:
#           -h PGHOST
#           -p PGPORT
#           -U PGUSR
#           -d PGDATABASE
#           -s STARTTIME (FORMAT: YYYY-MM-DD hh:mm:ss)
#           -e ENDTIME (FORMAT: YYYY-MM-DD hh:mm:ss)
#           -t PGCONNECT_TIMEOUT
# sample run with parameters below :
#./aqua_capture_query.sh -h 11.11.11.111 -p 5439 -d dev -U test_user -s "2021-09-06 00:00:00" -e "2021-09-09 23:00:00"
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
        s) 
          STARTTIME="${OPTARG}"
          ;;
        e) 
          ENDTIME="${OPTARG}"
          ;;
        t) 
          export PGCONNECT_TIMEOUT=${OPTARG}
          ;;
    esac
done

read -s -p "Password to connect to Redshift cluster: " PGPASSWORD
export  PGPASSWORD=$PGPASSWORD
echo
if [[ -z $PGCONNECT_TIMEOUT ]]; then
    export PGCONNECT_TIMEOUT=10
fi


if [[ -z $PGHOST ]] || [[ -z $PGPORT ]] || [[ -z $PGDATABASE ]] || [[ -z $PGUSER ]]; then
    echo "connection parameters required and cannot be empty, please enter correct  hostname, dbname, port-number, username to connect"
    exit -1
fi

#If user doesn't provide start and endtime,defaults to current time as start time and use queries from first 3 hours
if [[ -z $STARTTIME ]]; then
    STARTTIME=$(date "+%Y-%m-%d")
fi

if [[ -z $ENDTIME ]]; then
    ENDTIME=$(date "+%Y-%m-%d 3:%m:%S")
fi

CAPTUREQUERY=$(cat <<QUERYMARKER
  with aq as
  (
    select
      q.query,
      w.total_exec_micros
    from
     (
       select
          query
       from
          stl_query
       where
        aborted = 0
        and starttime >= to_timestamp('$STARTTIME', 'YYYY-MM-DD HH:MI:SS')
        and endtime <= to_timestamp('$ENDTIME','YYYY-MM-DD HH:MI:SS')
        and userid <> 1
       group by
          query
      ) q
    inner join
      (
        select
          query,
          max(total_exec_time) total_exec_micros
        from
         stl_wlm_query
        where
         exec_start_time >= to_timestamp('$STARTTIME', 'YYYY-MM-DD HH:MI:SS')
         and exec_end_time <= to_timestamp('$ENDTIME','YYYY-MM-DD HH:MI:SS')
         and userid <> 1
       group by
        query
      ) w
    using (query)
   ),
  se as
   (
     select
        e.query,
        e.nodeid,
        p.segment
     from
         (
          select
              query,
              nodeid
          from
            stl_explain
          where
           query in (select query from aq)
           and trim(plannode) ~* '(XN|LD) Seq Scan'
           and trim(info) ~ 'Filter:'
           and trim(info) ~ '!?~+\\\*?'
        group by
         query,
         nodeid
         ) e
           inner join
         (
           select
              query,
              nodeid,
              segment
            from
              stl_plan_info
            where
              query in (select query from aq)
            group by
              query,
              nodeid,
              segment
          ) p
           using (query, nodeid)
   group by
      query,
      nodeid,
      segment
   ),
  sc_sg as
   (
     select
          userid,
          query,
          segment,
       sum(step_scan_micros) as seg_scan_micros
     from
          (
           select
               userid,
               query,
               segment,
               step,
               datediff('microsec', min(starttime), max(endtime)) step_scan_micros
           from
             stl_scan
           where
             query in (select query from se)
             and type=2
          group by
            userid,
            query,
            segment,
            step
         having
           step_scan_micros > 500000
            ) sc_sp
     group by
        userid,
        query,
        segment
     ),
  sct_q as
    (
        select
             userid,
             query,
             sum(stream_scan_micros) as scan_micros
        from
            (
              select
                   sc_sg.userid,
                   sc_sg.query,
                   se.nodeid,
                   sss.stream,
                   max(sc_sg.seg_scan_micros) as stream_scan_micros
              from
                 sc_sg
                inner join
             (
                 select
                     userid,
                     query,
                     segment,
                     max(stream) as stream
                 from
                     stl_stream_segs
                where
                     query in (select query from se)
                group by
                userid,
                 query,
                 segment
            ) sss
          on
           (
            sc_sg.userid=sss.userid and
            sc_sg.query=sss.query and
            sc_sg.segment=sss.segment
            )
       inner join
           se
         on
          (
            se.query=sc_sg.query and
            se.segment=sc_sg.segment
          )
       group by
          sc_sg.userid,
          sc_sg.query,
          se.nodeid,
          sss.stream
      ) q
      group by 
      userid, query
 )
select
      aggqrytxt || ';'  as querytxt
from
  (
     select
          query,
          listagg(text) within group (order by sequence) || case when max(sequence)>=299 then ';'  else '' end as aggqrytxt,
      case
          when aggqrytxt ~* '(\\\\s|^)(delete\|update\|create\|upload\|insert\|vaccum\|create table\|create view\|analyze\|copy)\\\\s' then 1
          else 0
      end as querytype
    from
         (
           select
              query,
              (CASE WHEN LEN(RTRIM(text)) = 0 THEN text else replace(RTRIM(text), '\\\\n', ' ') END) as text,
              sequence,
              row_number() over (partition by query, sequence order by xid desc) rn
            from
              stl_querytext
            where
              query in
                 (
                    select
                         query
                    from
                       stl_querytext
                    where
                        query in (select query from sct_q)
                        and text ~* '\\\\s(like\|ilike\|similar to)\\\\s'
                        and text !~*
                         '\\\\s(delete\|update\|create\|upload\|insert\|vaccum\|create as\|analyze\|copy)\\\\s'
                     group by
                        query
                  ) 
            ) iq
   where
      rn=1
      and sequence < 300
   group by query
     ) stlqtxt
      inner join
       sct_q  using (query)
      inner join
       aq  using (query)
 where
     querytype = 0
  order by
    sct_q.scan_micros::float/aq.total_exec_micros desc
   limit 100;
QUERYMARKER
)
#error handling to check psql
RESULT=$(psql -c "$CAPTUREQUERY" -A  --tuples-only --log-file=capture_sql.log  2>&1 )
CONN_STATUS=$?
if [ $CONN_STATUS -eq 2 ]; then
    echo "Connection failed. Please try again with correct connection parameters"
    echo $RESULT   
    exit $CONN_STATUS
elif [ $CONN_STATUS -eq 1 ]; then
    echo "Failed due to SQL  error, please check and run again"
    echo $RESULT
    exit $CONN_STATUS
fi

if  [[ ! -z "$RESULT" ]]; then
    OUTPUTSQLFILE='aqua_eligible_queries.sql'
    echo $RESULT > $OUTPUTSQLFILE
    echo -e  "\nThe AQUA eligible queries are captured for workload between $STARTTIME and $ENDTIME"
else
    echo "Your workload history for given workload interval does not have enough AQUA eligible queries. Please run the script with different date/time parameters. If you still don’t see any queries, please reach out to us to work together on this."
fi
rm -f capture_sql.log
