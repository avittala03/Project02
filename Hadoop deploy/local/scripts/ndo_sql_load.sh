#!/bin/sh
#====================================================================================================================================================
# Title            : Script to execute the SQOOP Delete and SQOOP Export jobs.
# ProjectName      : NDO
# Filename         : ndo_sql_load.sh
# Description      : Script to run Sqoop Job which exports  Tables from  HDFS to MySQL
# Developer        : Anthem
# Created on       : JUNE 2018
# Location         : 
# Logic            :
# Parameters       : Environment Variables file name
# Execution        :sh SQOOPCHECK.sh "Environment Variables file name"
# Return codes     :
# Date                         Ver#     Modified By(Name)                 Change and Reason for Change
# ----------    -----        -----------------------------               --------------------------------------
# 2017/08/31                      1     Initial Version

#  ***************************************************************************************************************************
#Execution constraints : 
#	1.Properties file must be passed as first variable 
#	2.Properties file must be present in $SCRIPTS_DIR<.profile> path
######### This script does the following
######### 1. Checks for  the  records count of the source table in HDFS  location.If it is 0 then it stops the execution.If not,it continues.
######### 2. Checks for  the  records count of the target table   in MySQL  location ,before  loading  - Only if needed
######### 3. Executes  the Sqoop Script  and loads  the Data  into Target  folder  in MySQL.
######### 4. Checks for  the  records count of the target table   in MySQL  location , after  loading  - Only if needed


echo "Execution of Script starts from here"
#Load the bash profile/ .profile/
source $HOME/.bash_profile
source $HOME/.profile

#Defaulting the profile variables for non-service ids
if [ "$(whoami)" != "srcpdppndobthpr" ]; then
        SCRIPTS_DIR="/pr/app/vs2/pdp/pndo/phi/no_gbd/r000/bin/scripts"
        HDFS_DIR="/pr/hdfsapp/vs2/pdp/pndo/phi/no_gbd/r000/bin"
        LOG_DIR="/pr/app/vs2/pdp/pndo/phi/no_gbd/r000/bin/logs"
fi

if [ -f $SCRIPTS_DIR/application_shell.properties ];
then
	source $SCRIPTS_DIR/application_shell.properties
else
	echo "ERROR - application_shell.properties not available in $SCRIPTS_DIR"
	exit -1
fi

#Source the parameter file
prpts_fl=$1
SUBJECT_AREA=`echo $prpts_fl | rev | awk -F "[/.]" '{ print $2 }' | rev`
LOG_FILE=$LOG_DIR/"script_"$SUBJECT_AREA"_"$(v_DATETIME)"_"$USER.log
v_CHECK_FILE $prpts_fl
source $prpts_fl
echo "$(v_TIMESTAMP):INFO:File sourced $SCRIPTS_DIR/$prpts_fl"

exec 1> $LOG_FILE 2>&1

#Default a Variable 
MAPPERS_CNT=10

#MAIN-STEP 1 : Check the source record count : ------------------------------------------
echo "$(v_TIMESTAMP):INFO: MAIN-STEP 1 : Check the source record count  beeline -u $v_BEELINE_SERVER -hiveconf db=$HIVEDB -hiveconf table=$SOURCETABLENAME --outputformat=tsv2 --showHeader=false -e select COUNT(*) FROM ${hiveconf:db}.${hiveconf:table}"

Tcount="$(beeline -u $v_BEELINE_SERVER --hiveconf mapred.job.queue.name=ndo_coca_yarn -hiveconf db=$HIVEDB -hiveconf table=$SOURCETABLENAME --outputformat=tsv2 --showHeader=false -e "select COUNT(*) FROM "'${hiveconf:db}'"."'${hiveconf:table}'"")"
v_RC_CHECK "$?" "Step1 - Hive-Source table count check"

if [ "$Tcount" -eq '0' ];then 
	echo "$(v_TIMESTAMP):WARNING: Execution stopped - $Tcount records in $SOURCETABLENAME"
	exit $v_ERROR
else 
	echo "$(v_TIMESTAMP):INFO: $Tcount records sourced from $SOURCETABLENAME"
fi
	
#MAIN-STEP 2 : DELETE TARGET TABLE IN SQL SERVER  : ------------------------------------------
echo "$(v_TIMESTAMP):INFO: MAIN-STEP 2 : DELETE TARGET TABLE IN SQL SERVER 
sqoop eval -Dhadoop.security.credential.provider.path=$CREDENTIAL_PROVIDER_PATH --connect $CONNECTION_STRING -username $DBUSERNAME --password-file $v_PASS_FILE --query $DEL_QUERY"

sqoop eval -Dhadoop.security.credential.provider.path=$CREDENTIAL_PROVIDER_PATH --connect "$CONNECTION_STRING" -username $DBUSERNAME --password-file $v_PASS_FILE --query "$DEL_QUERY"
v_RC_CHECK "$?" "Step2 - Sqoop-Delete SQL table"

 


#MAIN-STEP 3 : Export data to SQL SERVER  : ------------------------------------------
echo "$(v_TIMESTAMP):INFO: MAIN-STEP 3 :Export data to SQL SERVER 
	sqoop-export \
-Dhadoop.security.credential.provider.path=$CREDENTIAL_PROVIDER_PATH \
--connect $CONNECTION_STRING \
-username $DBUSERNAME \
--password-file $v_PASS_FILE \
--columns $column_names \
--hcatalog-table $SOURCETABLENAME \
--hcatalog-database $HIVEDB \
-m $MAPPERS_CNT \
--batch \
--table $TABLENAME \
-- --schema $SCHEMA_NAME
"

sqoop-export \
-Dmapred.job.queue.name=ndo_coca_yarn \
-Dhadoop.security.credential.provider.path=$CREDENTIAL_PROVIDER_PATH \
--connect "$CONNECTION_STRING" \
-username $DBUSERNAME \
--password-file $v_PASS_FILE \
--columns "$column_names" \
--hcatalog-table "$SOURCETABLENAME" \
--hcatalog-database "$HIVEDB" \
-m $MAPPERS_CNT \
--batch \
--table "$TABLENAME" \
-- --schema "$SCHEMA_NAME"
v_RC_CHECK "$?" "Step3 - Sqoop-Export"

echo "Execution completed for $prpts_fl"
exit $v_SUCCESS
