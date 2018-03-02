#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

. ${DIR}/log_functionality.sh

update_status "BOOTSTRAPPING"
start_scenario "Bootstrap the test EC2 instance and document environment"
scenario_result=0
start_step "Get commit information for this test report"
cd ${HOME}/amazon-redshift-utils
echo "git remote -v" >>${STDOUTPUT} 2>>${STDERROR}
git remote -v >>${STDOUTPUT} 2>>${STDERROR}
echo "Git branch:`git branch | grep '^*' | tr -d '*'`" >>${STDOUTPUT} 2>>${STDERROR}

nr_of_lines=$(( `git log | grep -n '^commit ' | head -n 2 | tail -n 1 | awk -F: '{print $1}'` - 1 )) 2>>${STDERROR}
git log | head -n ${nr_of_lines} >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Install Python pip (easy_install pip)"
sudo easy_install pip >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Install OS packages (yum install -y postgresql postgresql-devel python27-virtualenv python36-devel python36-virtualenv gcc python-devel git aws-cli libffi-devel.x86_64 )"
sudo yum install -y postgresql postgresql-devel gcc python-devel python27-virtualenv python36-devel python36-virtualenv git aws-cli libffi-devel.x86_64 >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Get IAM_INFO.json"
curl http://169.254.169.254/latest/meta-data/iam/info > ${HOME}/IAM_INFO.json 2>>${STDERROR}
echo "Result=`cat ${HOME}/IAM_INFO.json`" >>${STDOUTPUT} 2>>${STDERROR}
cat ${HOME}/IAM_INFO.json | grep Success &>>/dev/null
r=$? && stop_step $r

REGION_NAME=`curl http://169.254.169.254/latest/meta-data/hostname | awk -F. '{print $2}'`

start_step "Await full stack bootstrap"
STACK_NAME=`cat ${HOME}/IAM_INFO.json | grep InstanceProfileArn | awk -F/ '{ print $2}'`
max_minutes_to_wait=15
minutes_waited=0
return_code=0
while [ 1 = 1 ]
do
    if [ "${minutes_waited}" = "${max_minutes_to_wait}" ]
    then
        return_code=100
        break
    else
        aws cloudformation describe-stacks --region ${REGION_NAME} --stack-name ${STACK_NAME} | grep StackStatus | grep CREATE_COMPLETE &>/dev/null
        if [ "$?" = "0" ]
        then
            return_code=0
            break;
        else
            echo "`date` Stack not ready yet" >> ${STDOUTPUT}
            minutes_waited="$(( $minutes_waited + 1 ))"
            sleep 60
        fi
    fi
done
stop_step $return_code

start_step "Get Cloudformation Stack name (aws cloudformation describe-stacks --region ${REGION_NAME} --stack-name ${STACK_NAME})"
aws cloudformation describe-stacks --region ${REGION_NAME} --stack-name ${STACK_NAME} >${HOME}/STACK_DETAILS.json 2>>${STDERROR}
r=$? && stop_step $r

start_step "Setup Python2.7 environment"
echo 'VIRTUAL_ENV_PY27_DIR="${HOME}/venv_py27_env1/"' >> ${HOME}/variables.sh
source ${HOME}/variables.sh
virtualenv-2.7 ${VIRTUAL_ENV_PY27_DIR} >>${STDOUTPUT} 2>>${STDERROR}
source ${VIRTUAL_ENV_PY27_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
pip install -r ${DIR}/requirements.txt >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r
deactivate

start_step "Setup Python3.6 environment"
echo 'VIRTUAL_ENV_PY36_DIR="${HOME}/venv_py36_env1/"' >> ${HOME}/variables.sh
source ${HOME}/variables.sh
virtualenv-3.6 ${VIRTUAL_ENV_PY36_DIR} >>${STDOUTPUT} 2>>${STDERROR}
source ${VIRTUAL_ENV_PY36_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
pip install -r ${DIR}/requirements.txt >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r
deactivate


start_step "Get all stack parameters (python ${DIR}/get_stack_parameters.py)"
source ${VIRTUAL_ENV_PY36_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
python3 ${DIR}/get_stack_parameters.py >>${STDOUTPUT} 2>>${STDERROR}
grep "TargetClusterEndpointPort" $HOME/stack_parameters.json &>/dev/null
r=$? && stop_step $r

source ${HOME}/variables.sh

start_step "Create .pgpass files"
cat "${HOME}/PASSWORD_KMS.txt" | base64 --decode >>"${HOME}/PASSWORD_KMS.bin" 2>>${STDERROR}
CLUSTER_DECRYPTED_PASSWORD=`aws kms decrypt --ciphertext-blob fileb://${HOME}/PASSWORD_KMS.bin --region ${REGION_NAME} | grep Plaintext | awk -F\" '{print $4}' | base64 --decode` >>${STDOUTPUT} 2>>${STDERROR}
echo "${SourceClusterEndpointAddress}:${SourceClusterEndpointPort}:${SourceClusterDBName}:${SourceClusterMasterUsername}:${CLUSTER_DECRYPTED_PASSWORD}" >> ${HOME}/.pgpass 2>>${STDERROR}
echo "${TargetClusterEndpointAddress}:${TargetClusterEndpointPort}:${TargetClusterDBName}:${TargetClusterMasterUsername}:${CLUSTER_DECRYPTED_PASSWORD}" >> ${HOME}/.pgpass 2>>${STDERROR}
chmod 600  ${HOME}/.pgpass 2>>${STDERROR}
#Only verify that there are 2 records next we have tests for access
cat ${HOME}/.pgpass | grep -v "::"| wc -l | grep "^2$" >>/dev/null 2>>${STDERROR}
r=$? && stop_step $r

#Needed because source is restored from snapshot.
start_step "Reset password of source cluster to CloudFormation Configuration"
if [ "${CLUSTER_DECRYPTED_PASSWORD}" = "" ]
then
    CLUSTER_DECRYPTED_PASSWORD=`aws kms decrypt --ciphertext-blob fileb://${HOME}/PASSWORD_KMS.bin --region ${REGION_NAME} | grep Plaintext | awk -F\" '{print $4}' | base64 --decode` >>${STDOUTPUT} 2>>${STDERROR}
fi
aws redshift modify-cluster --cluster-identifier "${SourceClusterName}" --master-user-password "${CLUSTER_DECRYPTED_PASSWORD}" --region "${Region}"  >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Await no more pending modified variables"
max_minutes_to_wait=10
minutes_waited=0
return_code=0
while [ 1 = 1 ]
do
    if [ "${minutes_waited}" = "${max_minutes_to_wait}" ]
    then
        return_code=100
        break
    else
        aws redshift describe-clusters --cluster-identifier "${SourceClusterName}" --region "${Region}" | grep "\"PendingModifiedValues\": {}" >>/dev/null 2>>/dev/null
        if [ "$?" = "0" ]
        then
            return_code=0
            break;
        else
            echo "`date` There are variables to be modified on the cluster await cluster to be in sync" >>${STDOUTPUT}
            minutes_waited="$(( $minutes_waited + 1 ))"
            sleep 60
        fi
    fi
done
stop_step ${return_code}

start_step "Test passwordless (.pgpass) access to source cluster"
psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "select 'result='||1;" | grep "result=1" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Test passwordless (.pgpass) access to target cluster"
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "select 'result='||1;" | grep "result=1" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r


#Setup admin tools
start_step "Create Admin schema on source if it does not exist"
psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "CREATE SCHEMA IF NOT EXISTS admin;" | grep "CREATE SCHEMA" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Create Admin view admin.v_generate_tbl_ddl on source if it does not exist"
psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -f ${HOME}/amazon-redshift-utils/src/AdminViews/v_generate_tbl_ddl.sql | grep "CREATE VIEW"
r=$? && stop_step $r

update_status "Await Redshift restore of source cluster (${SOURCE_CLUSTER_NAME})"

SOURCE_CLUSTER_NAME=`grep -A 1 SourceClusterName ${HOME}/STACK_DETAILS.json | grep OutputValue | awk -F\" '{ print $4}'`
start_step "Await Redshift restore of source cluster (${SOURCE_CLUSTER_NAME})"
max_minutes_to_wait=20
minutes_waited=0
return_code=0
while [ 1 = 1 ]
do
    if [ "${minutes_waited}" = "${max_minutes_to_wait}" ]
    then
        return_code=100
        break
    else
        aws redshift describe-clusters --cluster-identifier ${SOURCE_CLUSTER_NAME} --region ${REGION_NAME} | grep -A 5  RestoreStatus | grep "\"Status\"" | grep completed >>/dev/null
        if [ "$?" = "0" ]
        then
            return_code=0
            break;
        else
            echo "`date` Cluster restore not finished yet" >> ${STDOUTPUT}
            minutes_waited="$(( $minutes_waited + 1 ))"
            sleep 60
        fi
    fi
done
stop_step ${return_code}

stop_scenario

total_tests=`find $DIR -type f -name 'run_test.sh' | wc -l | tr -d ' '`
test_nr=0
#Start running the scenario's
for file in `find $DIR -type f -name 'run_test.sh' | sort `
do
 test_nr="$(( $test_nr + 1 ))"
 update_status "Running test ${test_nr}/${total_tests}"
 . ${file}
done

update_status "Publishing results to S3"

#Publish results
echo "Publishing results to S3"
S3_PATH="s3://${ReportBucket}/`date +%Y/%m/%d/%H/%M`/"
aws s3 cp ${STDOUTPUT} ${S3_PATH}
aws s3 cp ${STDERROR} ${S3_PATH}
aws s3 cp /var/log/cloud-init-output.log ${S3_PATH}

update_status "Complete"
