#====================================================================================================
#!/bin/sh
# Title            : NDO_AUDIT_LOAD
# ProjectName      : NDO
# Filename         : NDO_AUDIT_LOAD.sh
# Developer        : Anthem
# Created on       : FEB 2018
# Location         : ATLANTA
# Date           Auth           Ver#     Modified By(Name)                 Change and Reason for Change
# ----------    -----        -----------------------------               --------------------------------------
# 2018/10/01     Deloitte         1     Initial Version
#====================================================================================================                                                                                        
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


#VARIABLES:
DRIVER_CLASS="ControlTotalDriverEhoppa"
SUBJECT_AREA="$DRIVER_CLASS"
status="SUCCEEDED"

#Creating log file
LOG_FILE=$LOG_DIR/"script_"$SUBJECT_AREA"_"$(v_DATETIME)"_"$USER.log
YARN_LOG_SMRY_FILE=$LOG_DIR/$SUBJECT_AREA"_"$(v_DATETIME)"_"$USER.log

echo "$(v_TIMESTAMP):INFO:NDO ETL AUDIT -$SUBJECT_AREA Script logs in  $LOG_FILE"
echo "$(v_TIMESTAMP):INFO:NDO ETL AUDIT wrapper triggered">>$LOG_FILE
echo "$(v_TIMESTAMP):INFO:NDO  \
        SUBJECT_AREA-$SUBJECT_AREA \
        DRIVER_CLASS-$DRIVER_CLASS \
        ">>$LOG_FILE

#=================================================================================================================
# Run spark submit command
export JAVA_HOME=/usr/java/latest
exec
$v_SPARK_SUBMIT_2_2 --master yarn --queue ndo_coca_yarn --deploy-mode cluster --name $SUBJECT_AREA --executor-memory 80G --executor-cores 4 --driver-cores 10 --driver-memory 80G --conf spark.sql.shuffle.partitions=2100 --conf spark.sql.codegen.wholeStage=true --conf spark.yarn.maxAppAttempts=1  --conf spark.yarn.driver.memoryOverhead=4096  --conf spark.sql.parquet.cacheMetadata=false --conf spark.yarn.executor.memoryOverhead=8192 --conf spark.network.timeout=900 --conf spark.driver.maxResultSize=0 --conf spark.kryoserializer.buffer.max=1024m --conf spark.rpc.message.maxSize=1024 --conf spark.sql.broadcastTimeout=5800 --conf spark.executor.heartbeatInterval=30s --conf spark.dynamicAllocation.executorIdleTimeout=90 --conf spark.dynamicAllocation.initialExecutors=10 --conf spark.dynamicAllocation.maxExecutors=100 --conf spark.dynamicAllocation.minExecutors=10 --conf spark.sql.autoBroadcastJoinThreshold=604857600 --conf spark.sql.cbo.enabled=true  --files hdfs://${HDFS_DIR}//log4j.xml,/etc/spark2/conf.cloudera.spark2_on_yarn/yarn-conf/hive-site.xml --driver-java-options "-Dlog4j.configuration=log4j.xml" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.xml" --jars /usr/lib/tdch/1.7/lib/terajdbc4.jar,/usr/lib/tdch/1.7/lib/tdgssconfig.jar --class com.am.ndo.controlTotals.${DRIVER_CLASS} hdfs://${HDFS_DIR}/jar/$v_NDO_ETL_LATEST_jar hdfs://${HDFS_DIR} ${v_ENV} ndo >$YARN_LOG_SMRY_FILE 2>&1

#=================================================================================================================

#Get spark application URL

application_url=`grep tracking  $YARN_LOG_SMRY_FILE|head -1`
application_id=$(echo $application_url | sed 's:/*$::')
application_name=`echo $application_id| rev | cut -d'/' -f 1 | rev`

echo "$(v_TIMESTAMP):INFO: Application URL :$application_url" >>$LOG_FILE
echo "$(v_TIMESTAMP):INFO: Application Name : $application_name" >>$LOG_FILE

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
rm $TMP_YARN_LOG_FILE
exit $v_SUCCESS
else
echo "$(v_TIMESTAMP):INFO: Spark Job Failed for "$SUBJECT_AREA"" >>$LOG_FILE
exit $v_ERROR
fi

