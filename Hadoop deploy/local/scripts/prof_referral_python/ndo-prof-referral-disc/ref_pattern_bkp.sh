#!/bin/ksh
#@AF49123

#num_args=$#

# Environment variable 
env1=$1
# Home directory 
home_dir=$2

# Define Execution log file
EXECUTION_LOGFILE="${home_dir}/referal_pattern_exe_log.log"

if [ ${env1} == 'test' ] || [ ${env1} == 'TEST' ]
then
    spark_script="${home_dir}/ndo-prof-referral-disc/referral_pattern.py"
    spark_conf="${home_dir}/ndo-prof-referral-disc/input/conf_${env1}.json"
    spark_log="${home_dir}/ndo-prof-referral-disc/output/referral.log"
    spark_input_distance_file="${home_dir}/ndo-prof-referral-disc/input/gaz2016zcta5distance100miles.csv"
    echo " Read test environment configurations"
fi

if [ ${env1} == 'disc' ] || [ ${env1} == 'DISC' ]
then
    spark_script="${home_dir}/ndo-prof-referral-disc/referral_pattern.py"
    spark_conf="${home_dir}/ndo-prof-referral-disc/input/conf_${env1}.json"
    spark_log="${home_dir}/ndo-prof-referral-disc/output/referral.log"
    spark_input_distance_file="${home_dir}/ndo-prof-referral-disc/input/gaz2016zcta5distance100miles.csv"
    echo " Read discovery environment configurations"
fi

if [ ${env1} == 'prd' ] || [ ${env1} == 'PRD' ]
then
    spark_script="${home_dir}/ndo-prof-referral-disc/referral_pattern.py"
    spark_conf="${home_dir}/ndo-prof-referral-disc/input/conf_${env1}.json"
    spark_log="${home_dir}/ndo-prof-referral-disc/output/referral.log"
    spark_input_distance_file="${home_dir}/ndo-prof-referral-disc/input/gaz2016zcta5distance100miles.csv"
    echo " Read production environment configurations"
fi

function ref_pattern {
    echo "#############Referal Pattern Spark Job################################" >> ${EXECUTION_LOGFILE}
    export PYSPARK_PYTHON=/opt/cloudera/parcels/Anaconda/bin/python 
    echo "Running in Environment : ${env1} .." >> ${EXECUTION_LOGFILE}
    echo "File Checking - ${spark_script} ~ ${spark_conf} ~ ${spark_input_distance_file} ~ ${spark_log}"

    #spark-submit --master yarn --deploy-mode client --driver-memory 32G --num-executors 20 --conf spark.kryoserializer.buffer.max=2047 --conf spark.driver.maxResultSize=32*1024 --conf spark.yarn.executor.memoryOverhead=5000 --conf spark.shuffle.registration.timeout=60000 --executor-memory 32G --executor-cores 5 --conf spark.network.timeout=600s --conf spark.cores.max=200 --conf spark.executor.memory=6G ${spark_script} ${spark_conf} ${spark_ref_file} ${spark_log}
    
    spark-submit ${spark_script} --executor-memory 32G --master yarn-client --num-executors 50 --executor-cores 5 --driver-memory 32G --conf spark.executor.memory=6G --conf spark.kryoserializer.buffer.max=2047 --conf spark.cores.max=200 --conf spark.yarn.executor.memoryOverhead=5000 --conf spark.driver.maxResultSize=32G ${spark_conf} ${spark_input_distance_file} ${spark_log} ${env1}
       
    ref_pattern=$?
    echo " Result - $? "
}

function email_notification {
    echo "############# Referal Pattern e-mail notification################################" >> ${EXECUTION_LOGFILE}

    if [[ $ref_pattern -eq 0 ]]; then
        "Referal Pattern Spark job completed" | mailx -s "Referal Pattern completed in env - ${env1}" mounika.narisetty@am.com
    fi

    if [[ $ref_pattern -eq 1 ]]; then
        "Referal Pattern Spark job failed" | mailx -s "Referal Pattern failed in env - ${env1}" mounika.narisetty@am.com
    fi  
}

ref_pattern >> ${EXECUTION_LOGFILE} 2>&1
#email_notification >> ${EXECUTION_LOGFILE} 2>&1
