
#=================================================================================================================================================
#!/bin/sh
# Title            : NDO_PROF_REF_PATTERNS_ABC_Wrapper
# ProjectName      : NDO
# Filename         : NDO_PROF_REF_PATTERNS_ABC_Wrapper.sh
# Developer        : Anthem
# Created on       : FEB 2018
# Location         : ATLANTA
# Date           Auth           Ver#     Modified By(Name)                 Change and Reason for Change
# ----------    -----        -----------------------------               --------------------------------------
# 2018/02/01     Deloitte         1     Initial Version
#====================================================================================================================================================

##  PROGRAM STEPS

# 1. Run the Prof Ref Patterns program                                                                                  
# 2. Post Processing                                                                                                                         
#====================================================================================================================================================
## SAMPLE ARGUMENTS
#Config file path=hdfs:///ts/hdfsapp/ve2/pdp/hpip/phi/no_gbd/r000/control/
#Environment=sit
#Edge node app path = /ts/app/ve2/pdp/hpip/phi/no_gbd/r000/control
#Query Property file=HPIP_EMPLOYER_ATTRIBUTES_Wrapper.properties
#Variables for argument parameters passed

now=$(date +"%Y%m%d%H%M%S")
echo $now

echo "NDO PROFESSIONAL REFERRAL PATTERNS wrapper triggered at $now"

ENV=$1
EDGE_PATH=$2
CONFIG_NAME="application_script_"$1".properties"
SUBJECT_AREA="NDO_PROF_REF_PATTERNS_F"

#Fetch properties from Config file -- redirect that to property file in edge node
#source that property files and get the needed params as below

source $EDGE_PATH/$CONFIG_NAME

#Creating log file
NDO_LOG_FILE=$LOG_FILE_PATH/"script_"$SUBJECT_AREA"_"$now"_"$USER.log

if [ $# -eq 2 ]
    then
        echo "Argument check completed"  >>$NDO_LOG_FILE
    else
                echo "Error in number of arguments passed, Script needs 2 arguments for execution"  >>$NDO_LOG_FILE
                exit 1
fi


SummaryLogFileNm=$SUBJECT_AREA"_"$now"_"$USER.log
SLogFullFileName=$LOG_FILE_PATH/$SummaryLogFileNm

export JAVA_HOME=/usr/java/latest
#=====================================================================================================================================================
# Run spark submit command
export JAVA_HOME=/usr/java/latest
exec
spark2-submit --master yarn --queue ndo_coca_yarn --deploy-mode cluster --name $SUBJECT_AREA --executor-memory 80G --executor-cores 4 --driver-cores 10 --driver-memory 80G --conf spark.sql.shuffle.partitions=2100 --conf spark.sql.codegen.wholeStage=true --conf spark.yarn.maxAppAttempts=1  --conf spark.yarn.driver.memoryOverhead=4096  --conf spark.sql.parquet.cacheMetadata=false --conf spark.yarn.executor.memoryOverhead=8192 --conf spark.network.timeout=900 --conf spark.driver.maxResultSize=0 --conf spark.kryoserializer.buffer.max=1024m --conf spark.rpc.message.maxSize=1024 --conf spark.sql.broadcastTimeout=5800 --conf spark.executor.heartbeatInterval=30s --conf spark.dynamicAllocation.executorIdleTimeout=90 --conf spark.dynamicAllocation.initialExecutors=10 --conf spark.dynamicAllocation.maxExecutors=200 --conf spark.dynamicAllocation.minExecutors=10 --conf spark.sql.autoBroadcastJoinThreshold=6048576000 --conf spark.sql.cbo.enabled=true --files $HDFS_BIN_PATH/log4j.xml,/etc/spark2/conf.cloudera.spark2_on_yarn/yarn-conf/hive-site.xml --driver-java-options "-Dlog4j.configuration=log4j.xml" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.xml" --jars /usr/lib/tdch/1.5/lib/terajdbc4.jar,/usr/lib/tdch/1.5/lib/tdgssconfig.jar --class com.am.ndo.profReferralPatterns.ProfReferralPatternsDriverF $HDFS_JAR_FILE_PATH/prof-ref.jar $HDFS_BIN_PATH $ENV ndo >$SLogFullFileName 2>&1

#=====================================================================================================================================================
#Get spark application URL
application_url=`grep tracking  $SLogFullFileName|head -1` 
echo "Application URL is $application_url" >>$NDO_LOG_FILE

#Extract application_id from URL
application_id=$(echo $application_url | sed 's:/*$::') 

#Get application name
application_name=`echo $application_id| rev | cut -d'/' -f 1 | rev` 
echo "Application Name is : $application_name" >>$NDO_LOG_FILE

#path to log file
yarn logs -applicationId ${application_name} >$LOG_FILE_PATH/$SUBJECT_AREA"_yarn_log_"${application_name}.log
echo "Yarn Log file : "$LOG_FILE_PATH"/"$SUBJECT_AREA"_yarn_log_"${application_name}".log"  >>$NDO_LOG_FILE

#Get application status details and save in temp file
yarn application --status $application_name >$LOG_FILE_PATH/temp_app_details_$SUBJECT_AREA"_"$now"_"$USER.txt 

#Get the application final status
app_status=`grep Final-State $LOG_FILE_PATH/temp_app_details_$SUBJECT_AREA"_"$now"_"$USER.txt`
final_app_status=`echo $app_status|rev | cut -d':' -f 1 | rev|tail -1`

status="SUCCEEDED"

echo $final_app_status
echo $status
#Compare application status
if [ $final_app_status  ==  $status ]
then 
echo "Spark Job for "$SUBJECT_AREA" executed successfully. Proceeding for the next Job." >>$NDO_LOG_FILE
else
echo "Spark Job Failed for "$SUBJECT_AREA".Please check the log" >>$NDO_LOG_FILE
exit 1
fi
#Remove temp files
rm $LOG_FILE_PATH/temp_app_details_$SUBJECT_AREA"_"$now"_"$USER.txt

echo "Script ran successfully, completed at ${now}" >>$NDO_LOG_FILE

#====================================================================================================================================================
##  End of Script
#====================================================================================================================================================

