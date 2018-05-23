#!/usr/bin/env bash

STDOUTPUT="${HOME}/output.log"
STDERROR="${HOME}/output.error"
LABELS=""
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCENARIO_RESULT=0
STEP_RESULT=0

function number_of_labels() {
   echo -n "${LABELS}" | tr '|' '\n' | wc -l
}

function get_label_with_number() {
  NUMBER=$1
  echo "${LABELS}" | awk -F\| "{ print \$${NUMBER} }"
}

function get_last_label() {
  get_label_with_number "`number_of_labels`"
}

function remove_last_label() {
  LABELS="`echo -n \"${LABELS}\" | tr '|' '\n' | head -n $(( number_of_labels - 1 )) | tr '\n' '|'`"
}

function log_event_for_type() {
  EVENT="$1"
  TYPE="$2"
  if [ "${EVENT}" = "START" ]
  then
    LABEL="$3"
    LABELS="${LABELS}${LABEL}|"
  elif [ "${EVENT:0:4}" = "STOP" ]
  then
    LABEL="`get_last_label`"
    remove_last_label
  fi
  date=`date +"%d/%m/%Y %H:%M:%S.%N"`
  echo ">>>${TYPE}:${date}:${LABEL}:${EVENT}" >>${STDOUTPUT}
  echo ">>>${TYPE}:${date}:${LABEL}:${EVENT}" >>${STDERROR}

}

function start_for_type() {
  TYPE="$1"
  LABEL="$2"
  log_event_for_type "START" "${TYPE}" "${LABEL}"
}

function start_step() {
  LABEL="$1"
  STEP_RESULT=0
  start_for_type "STEP" "${LABEL}"
}

function increment_step_result() {
  AMOUNT_TO_ADD="$1"
  STEP_RESULT="$(( ${STEP_RESULT} + ${AMOUNT_TO_ADD} ))"
}

function start_scenario() {
  LABEL="$1"
  SCENARIO_RESULT=0
  start_for_type "SCENARIO" "${LABEL}"
}

function stop_for_type_with_return_code() {
  TYPE="$1"
  RETURN_CODE="$2"
  if [ "${RETURN_CODE}" == "0" ]
  then
    log_event_for_type "STOP SUCCEEDED" "${TYPE}"
  else
    log_event_for_type "STOP FAILED WITH RETURN_CODE ${RETURN_CODE}" "${TYPE}"
  fi
}

function stop_step() {
  RETURN_CODE="$1"
  SCENARIO_RESULT="$(( ${SCENARIO_RESULT} + ${RETURN_CODE} ))"
  stop_for_type_with_return_code "STEP" ${RETURN_CODE}
}

function stop_scenario() {
  RETURN_CODE="${SCENARIO_RESULT}"
  stop_for_type_with_return_code "SCENARIO" ${RETURN_CODE}
}

function update_status() {
  echo "`date` STATUS=$1" >> ${HOME}/STATUS
}