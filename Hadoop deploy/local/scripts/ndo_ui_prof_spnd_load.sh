#====================================================================================================
#!/bin/sh
# Title            : NDO_ProfSpend_LOAD
# ProjectName      : NDO
# Filename         : NDO_profSpend_LOAD.sh
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
        SCRIPTS_DIR="/ts/app/vs2/pdp/pndo/phi/no_gbd/r000/bin/scripts"
        HDFS_DIR="/ts/hdfsapp/vs2/pdp/pndo/phi/no_gbd/r000/bin"
        BIN_DIR="/ts/app/vs2/pdp/pndo/phi/no_gbd/r000/bin"
        LOG_DIR="/ts/app/vs2/pdp/pndo/phi/no_gbd/r000/bin/logs"

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
DRIVER_CLASS="ProfSpendDriver"
SUBJECT_AREA="$DRIVER_CLASS"
status="SUCCEEDED"

#Creating log file
LOG_FILE=$LOG_DIR/"script_"$SUBJECT_AREA"_"$(v_DATETIME)"_"$USER.log
YARN_LOG_SMRY_FILE=$LOG_DIR/$SUBJECT_AREA"_"$(v_DATETIME)"_"$USER.log

exec 1> $LOG_FILE 2>&1

#Generic functions
write_flag()
{
if [ $1 -eq '0' ];then
        echo "$(v_TIMESTAMP) - TABLE CREATED"> $LOG_DIR/"$2"_$status
        echo "$(v_TIMESTAMP):INFO:$SUBJECT_AREA - TDCH Teradata extract for $2 Completed with RC $1 ... Flag file - $LOG_DIR/"$2"_$status CREATED  ">>$LOG_FILE
else
        echo "$(v_TIMESTAMP):ERROR:$SUBJECT_AREA - TDCH Teradata extract for $2 failed with RC $1 ... Flag file - $LOG_DIR/"$2"_$status NOT CREATED  ">>$LOG_FILE
        exit $v_ERROR
fi
}

remove_flag()
{
if [ -f $LOG_DIR/"$1"_$status ];then
        touch $LOG_DIR/TDCH_DATA_EXTRACT_FLAG_DELETE.log
        echo "$LOG_DIR/"$1"_$status">>$LOG_DIR/TDCH_DATA_EXTRACT_FLAG_DELETE.log
        rm $LOG_DIR/"$1"_$status
        echo "$(v_TIMESTAMP):INFO:$SUBJECT_AREA  Flag file - $LOG_DIR/"$1"_$status REMOVED  ">>$LOG_FILE
fi
}



trigger_extract()
{
if [ -f $LOG_DIR/"$2"_$status ];then
       echo "$(v_TIMESTAMP):INFO:$SUBJECT_AREA - TDCH Teradata extract for $2 skipped ... Flag file - $LOG_DIR/"$2"_$status AVAILABLE "
else

   /bin/sh $SCRIPTS_DIR/TDCH_EHOPPA_TDW.sh "$v_DL_TERADTA_PRFSPND_SRC_DB" "$1" "$v_HIVE_INB_NOGBD" "$2"
   write_flag "$?" "$2"
fi
}

echo "$(v_TIMESTAMP):INFO:NDO ETL AUDIT -$SUBJECT_AREA Script logs in  $LOG_FILE"
echo "$(v_TIMESTAMP):INFO:NDO ETL AUDIT wrapper triggered">>$LOG_FILE
echo "$(v_TIMESTAMP):INFO:NDO  \
        SUBJECT_AREA-$SUBJECT_AREA \
       DRIVER_CLASS-$DRIVER_CLASS \
        ">>$LOG_FILE

echo "$(v_TIMESTAMP):INFO:TDCH Teradata extract started">>$LOG_FILE

 /bin/sh $SCRIPTS_DIR/EXTRACT_AVRO_SCHEMA.sh "$v_DL_TERADTA_PRFSPND_SRC_DB" "PSRUSCURRENT" >>$LOG_FILE

if [ $? -ne 0 ];then
	echo "AVRO SCHEMA execution Script got Failed. Please Check"
	exit $v_ERROR
else 
	trigger_extract "PSRUSCURRENT" "PSRUSCURRENT"
    echo "$(v_TIMESTAMP):INFO:NDO ETL AUDIT -$SUBJECT_AREA Script logs in  $LOG_FILE"
fi



#=================================================================================================================
# Run spark submit command
export JAVA_HOME=/usr/java/latest
exec
$v_SPARK_SUBMIT_1_6 --master yarn --queue ndo_coca_yarn --deploy-mode cluster --name $SUBJECT_AREA --executor-memory 80G --executor-cores 5 --driver-cores 10 --driver-memory 60G --conf spark.sql.shuffle.partitions=2100 --conf spark.sql.codegen.wholeStage=true --conf spark.yarn.maxAppAttempts=1  --conf spark.yarn.driver.memoryOverhead=4096  --conf spark.sql.parquet.cacheMetadata=false --conf spark.yarn.executor.memoryOverhead=8192 --conf spark.network.timeout=900 --conf spark.driver.maxResultSize=0 --conf spark.kryoserializer.buffer.max=1024m --conf spark.rpc.message.maxSize=1024 --conf spark.sql.broadcastTimeout=5800 --conf spark.executor.heartbeatInterval=30s --conf spark.dynamicAllocation.executorIdleTimeout=90 --conf spark.dynamicAllocation.initialExecutors=30 --conf spark.dynamicAllocation.maxExecutors=150 --conf spark.dynamicAllocation.minExecutors=30 --conf spark.sql.autoBroadcastJoinThreshold=604857600 --conf spark.sql.cbo.enabled=true  --files hdfs://${HDFS_DIR}//log4j.xml,/etc/spark2/conf.cloudera.spark2_on_yarn/yarn-conf/hive-site.xml --driver-java-options "-Dlog4j.configuration=log4j.xml" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.xml" --jars /usr/lib/tdch/1.7/lib/terajdbc4.jar,/usr/lib/tdch/1.7/lib/tdgssconfig.jar --class com.am.ndo.profSpend.${DRIVER_CLASS} hdfs://${HDFS_DIR}/jar/${v_NDO_ETL_LATEST_jar} hdfs://${HDFS_DIR} ${v_ENV} ndo >$YARN_LOG_SMRY_FILE 2>&1

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
yarn logs -applicationId ${application_name} >$YARN_LOG_FILE
echo "$(v_TIMESTAMP):INFO:Yarn Log file name: $YARN_LOG_FILE"  >>$LOG_FILE

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
remove_flag "PSRUSCURRENT"
exit $v_SUCCESS
else
echo "$(v_TIMESTAMP):ERROR: Spark Job Failed for "$SUBJECT_AREA"" >>$LOG_FILE
exit $v_ERROR
fi

#=================================================================================================================
##  End of Script
#=================================================================================================================
