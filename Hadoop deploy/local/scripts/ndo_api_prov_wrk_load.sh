#!/bin/sh
#====================================================================================================
# Title            : API_PROV_WORK
# ProjectName      : NDO
# Filename         : ndo_api_prov_wrk_load.sh
# Developer        : Anthem
# Created on       : Mar 2019
# Location         : ATLANTA
# Date           Auth           Ver#     Modified By(Name)                 Change and Reason for Change
# ----------    -----        -----------------------------               --------------------------------------
# 2019/01/07     Deloitte         1     Initial Version
#====================================================================================================                                                                           #This script Refer data from CDL views and fetch the TIN and NPI data.

#=================================================================================================================

#Load the bash profile/ .profile/
source $HOME/.bash_profile
source $HOME/.profile

#Defaulting the profile variables for non-service ids
if [ "$(whoami)" != "srcpdppndobthpr" ]; then
        SCRIPTS_DIR="/pr/app/vs2/pdp/pndo/phi/no_gbd/r000/bin/scripts"
        HDFS_DIR="/pr/hdfsapp/vs2/pdp/pndo/phi/no_gbd/r000/bin"
        BIN_DIR="/pr/app/vs2/pdp/pndo/phi/no_gbd/r000/bin"
        LOG_DIR="/pr/app/vs2/pdp/pndo/phi/no_gbd/r000/bin/logs"
fi

#Load generic functions/Variables
if [ -f $SCRIPTS_DIR/application_shell.properties ];
then
        source $SCRIPTS_DIR/application_shell.properties
else
        echo "ERROR - application_shell.properties not available in $SCRIPTS_DIR"
        exit -1
fi

SUBJECT_AREA="API_PROV_WRK"
status="SUCCEEDED"

LOG_FILE=$LOG_DIR/"script_"$SUBJECT_AREA"_"$(v_DATETIME)"_"$USER.log
YARN_LOG_SMRY_FILE=$LOG_DIR/$SUBJECT_AREA"_"$(v_DATETIME)"_"$USER.log
#=================================================================================================================
# Run spark submit command
export JAVA_HOME=/usr/java/latest
exec
$v_SPARK_SUBMIT_2_2 --class  com.am.coca.baseline.driver.Main --master yarn --queue ndo_coca_yarn --deploy-mode cluster --driver-memory 8G --executor-memory 30G --executor-cores 8 --num-executors 40 --queue ndo_coca_yarn --conf spark.driver.maxResultSize=3g --conf spark.yarn.executor.memoryOverhead=8192 --conf spark.yarn.driver.memoryOverhead=8192 --conf spark.network.timeout=600 --conf hive.execution.engine=spark --conf spark.port.maxRetries=20 --conf spark.sql.broadcastTimeout=4800 --conf spark.executor.heartbeatInterval=30s --conf spark.dynamicAllocation.initialExecutors=10 --conf spark.dynamicAllocation.minExecutors=10   --conf spark.shuffle.partitions=300 --conf spark.broadcast.compress=true --conf spark.yarn.queue=pca_yarn  --conf spark.dynamicAllocation.maxExecutors=40 --files /etc/spark2/conf.cloudera.spark2_on_yarn/yarn-conf/hive-site.xml --name API_PROV_WORK $BIN_DIR/jar/$v_NDO_ETL_LATEST_jar_API $v_HIVE_INB_ALLOB $v_HIVE_OUB_ALLOB api_prov_wrk >$YARN_LOG_SMRY_FILE 2>&1

#=================================================================================================================

application_url=`grep tracking  $YARN_LOG_SMRY_FILE|head -1`
application_id=$(echo $application_url | sed 's:/*$::')
application_name=`echo $application_id| rev | cut -d'/' -f 1 | rev`

YARN_LOG_FILE=$LOG_DIR/$SUBJECT_AREA"_yarn_log_"${application_name}.log
TMP_YARN_LOG_FILE=$LOG_DIR/temp_app_details_$SUBJECT_AREA"_"$now"_"$USER.txt

#path to log file
yarn logs -applicationId ${application_name} >echo "$(v_TIMESTAMP):INFO:Yarn Log file : $YARN_LOG_FILE"  >>$LOG_FILE

#Get application status details and save in temp file
yarn application --status $application_name >$TMP_YARN_LOG_FILE

#Get the application final status
app_status=`grep Final-State $TMP_YARN_LOG_FILE`
yarn logs -applicationId ${application_name} >$YARN_LOG_FILE
final_app_status=`echo $app_status|rev | cut -d':' -f 1 | rev|tail -1`

#Compare application status
if [ $final_app_status  ==  $status ]
then
echo "$(v_TIMESTAMP):INFO: Spark Job for "$SUBJECT_AREA" COMPLETED." >>$LOG_FILE
touch $LOG_DIR/"API_PROV_WRK_"$status
rm $TMP_YARN_LOG_FILE
exit $v_SUCCESS
else
echo "$(v_TIMESTAMP):ERROR: Spark Job Failed for "$SUBJECT_AREA"" >>$LOG_FILE
exit $v_ERROR
fi

#=================================================================================================================
##  End of Script
#=================================================================================================================
