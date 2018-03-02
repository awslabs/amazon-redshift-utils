#!/usr/bin/env bash

source ${HOME}/variables.sh

SCENARIO=scenario009

DESCRIPTION="Verify admin.v_generate_tbl_ddl for tables with quotes. "
DESCRIPTION="${DESCRIPTION}Make sure support for double quotes is available. "

start_scenario "${DESCRIPTION}"

start_step "Create schema admin:"
echo 'CREATE SCHEMA IF NOT EXISTS admin;' >>${STDOUTPUT} 2>>${STDERROR}
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "CREATE SCHEMA IF NOT EXISTS admin;" 2>>${STDERROR} | grep "CREATE SCHEMA" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Install the AdminView admin.v_generate_tbl_ddl"
cat ${HOME}/amazon-redshift-utils/src/AdminViews/v_generate_tbl_ddl.sql >>${STDOUTPUT} 2>>${STDERROR}
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -f "${HOME}/amazon-redshift-utils/src/AdminViews/v_generate_tbl_ddl.sql" 2>>${STDERROR} | grep "CREATE VIEW" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Create schema with double quote in name"
echo 'CREATE SCHEMA IF NOT EXISTS "my""schema"'  >>${STDOUTPUT} 2>>${STDERROR}
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c 'CREATE SCHEMA IF NOT EXISTS "my""schema"' 2>>${STDERROR} | grep "CREATE SCHEMA" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Create target table with double quote in name"
echo 'CREATE TABLE IF NOT EXISTS "my""schema"."ta""r""get"("i""d" int, primary key("i""d"));'  >>${STDOUTPUT} 2>>${STDERROR}
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c 'CREATE TABLE IF NOT EXISTS "my""schema"."ta""r""get"("i""d" int, primary key("i""d"));' 2>>${STDERROR} | grep "CREATE TABLE" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Create pointing table with double quote in name"
echo 'create table "po""i""nter"("su""p""er" int, foreign key("su""p""er") references "my""schema"."ta""r""get"("i""d"))'  >>${STDOUTPUT} 2>>${STDERROR}
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c 'create table "po""i""nter"("su""p""er" int, foreign key("su""p""er") references "my""schema"."ta""r""get"("i""d"))' 2>>${STDERROR} | grep "CREATE TABLE" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Check whether generated DDL is correct"
echo 'Result for select ddl from admin.v_generate_tbl_ddl where tablename='"'"'po"i"nter'"':" >>${STDOUTPUT} 2>>${STDOUTPUT}
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c 'select ddl from admin.v_generate_tbl_ddl where tablename='"'"'po"i"nter'"';" >>${STDOUTPUT} 2>>${STDOUTPUT}
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c 'select ddl from admin.v_generate_tbl_ddl where tablename='"'"'po"i"nter'"';" 2>>${STDERROR} | grep 'CREATE TABLE.*"po""i""nter"' >>${STDOUTPUT} 2>>${STDERROR}
RESULT="$?"
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c 'select ddl from admin.v_generate_tbl_ddl where tablename='"'"'po"i"nter'"';" 2>>${STDERROR} | grep 'ALTER TABLE.*"po""i""nter".*FOREIGN KEY.*"su""p""er".*"my""schema"."ta""r""get"."i""d".' >>${STDOUTPUT} 2>>${STDERROR}
r=$(( $? + ${RESULT} )) && stop_step $r
deactivate

stop_scenario