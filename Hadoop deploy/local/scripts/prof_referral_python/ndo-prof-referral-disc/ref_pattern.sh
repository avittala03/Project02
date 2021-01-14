#!/bin/ksh
source $HOME/.bash_profile
source $HOME/.profile
# Environment variable
env1=$1
# Home directory
home_dir=$2

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

SUBJECT_AREA="referral_pattern_py"
status="SUCCEEDED"

#Creating log file
LOG_FILE=$LOG_DIR/"script_"$SUBJECT_AREA"_"$(v_DATETIME)"_"$USER.log
YARN_LOG_SMRY_FILE=$LOG_DIR/$SUBJECT_AREA"_"$(v_DATETIME)"_"$USER.log

echo "$(v_TIMESTAMP):INFO:NDO ETL Pyspark -$SUBJECT_AREA Script logs in  $LOG_FILE"
echo "$(v_TIMESTAMP):INFO:NDO ETL Professional Referral Pattern Pyspark wrapper triggered">>$LOG_FILE
echo "$(v_TIMESTAMP):INFO:NDO  SUBJECT_AREA-$SUBJECT_AREA " >>$LOG_FILE


if [ ${env1} == 'test' ] || [ ${env1} == 'TEST' ]
then
    spark_script="${home_dir}/ndo-prof-referral-disc/referral_pattern.py"
    spark_conf="conf_${env1}.json"
    spark_log="referral.log"
    spark_input_distance_file="gaz2016zcta5distance100miles.csv"
    spark_util_py="${home_dir}/ndo-prof-referral-disc/util.py"
    echo " Read test environment configurations" >>$LOG_FILE
fi

if [ ${env1} == 'disc' ] || [ ${env1} == 'DISC' ]
then
    spark_script="${home_dir}/ndo-prof-referral-disc/referral_pattern.py"
    spark_conf="${home_dir}/ndo-prof-referral-disc/input/conf_${env1}.json"
    spark_log="${home_dir}/ndo-prof-referral-disc/output/referral.log"
    spark_input_distance_file="${home_dir}/ndo-prof-referral-disc/input/gaz2016zcta5distance100miles.csv"
    spark_util_py="${home_dir}/ndo-prof-referral-disc/util.py"
    echo " Read discovery environment configurations" >>$LOG_FILE
fi

if [ ${env1} == 'prd' ] || [ ${env1} == 'PRD' ]
then
    spark_script="${home_dir}/ndo-prof-referral-disc/referral_pattern.py"
    spark_conf="conf_${env1}.json"
    spark_log="referral.log"
    spark_input_distance_file="gaz2016zcta5distance100miles.csv"
    spark_util_py="${home_dir}/ndo-prof-referral-disc/util.py"
    echo " Read production environment configurations" >>$LOG_FILE
fi


    echo "#############Referal Pattern Spark Job################################" >>$LOG_FILE
    export PYSPARK_PYTHON=/opt/cloudera/parcels/Anaconda/bin/python
    echo "Running in Environment : ${env1} .." >>$LOG_FILE
    echo "File Checking - ${spark_script} ~ ${spark_conf} ~ ${spark_input_distance_file} ~ ${spark_log}" >>$LOG_FILE
    
    export JAVA_HOME=/usr/java/latest
    cd ${home_dir}/ndo-prof-referral-disc/input/

exec $v_SPARK_SUBMIT_1_6 --master yarn --queue ndo_coca_yarn --deploy-mode cluster --name $SUBJECT_AREA --executor-memory 80G --executor-cores 4 --driver-cores 5 --driver-memory 50G --conf spark.sql.shuffle.partitions=4200 --conf spark.sql.codegen.wholeStage=true --conf spark.yarn.maxAppAttempts=1  --conf spark.yarn.driver.memoryOverhead=4096  --conf spark.sql.parquet.cacheMetadata=false --conf spark.yarn.executor.memoryOverhead=8192 --conf spark.network.timeout=420000 --conf spark.driver.maxResultSize=0 --conf spark.kryoserializer.buffer.max=1024m --conf spark.rpc.message.maxSize=1024 --conf spark.sql.broadcastTimeout=5800 --conf spark.executor.heartbeatInterval=30s --conf spark.dynamicAllocation.executorIdleTimeout=180 --conf spark.dynamicAllocation.initialExecutors=30 --conf spark.dynamicAllocation.maxExecutors=200 --conf spark.dynamicAllocation.minExecutors=100 --conf spark.sql.autoBroadcastJoinThreshold=604857600 --conf spark.sql.cbo.enabled=true --py-files $spark_util_py --files ${spark_conf},${spark_input_distance_file},${spark_log} $spark_script ${spark_conf} ${spark_input_distance_file} ${spark_log} ${env1} >$YARN_LOG_SMRY_FILE 2>&1

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
exit $v_SUCCESS
else
echo "$(v_TIMESTAMP):INFO: Spark Job Failed for "$SUBJECT_AREA"" >>$LOG_FILE
exit $v_ERROR
fi


#=================================================================================================================
##  End of Script
#=================================================================================================================
