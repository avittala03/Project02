#!/bin/bash
set -e
# Set up logging output
etl_base_path=`echo $( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )`
execution_timestamp=$(date '+%Y%m%d_%s')
project_name="csbdpihbaseetl"
script_name=`basename ${0} | cut -f 1 -d '.'`
log_file_path=${etl_base_path}/logs/${project_name}/${script_name}
log_file_name=${script_name}_${execution_timestamp}.log
mkdir -p ${log_file_path}
keytab=$(echo "${HOME}/$(whoami).keytab")
full_log_file_path="${log_file_path}/${log_file_name}"
echo "INFO: Writing to logs to ${full_log_file_path}..."
exec &> >(tee "${full_log_file_path}")

while getopts 'e:ht:p:' flag; do
  case "${flag}" in
    e) ENVIRONMENT="${OPTARG}" ;;
    h) echo "Available options are:
        -e  name: environment
            required: yes
            description: valid values are [development | test | discovery | production]
            example: ${BASH_SOURCE[0]} -e development

        -t  name: tags
            required: no
            description: A comma-delimited list of tags or table names. Whitespace is not allowed in this option.
            example: ${BASH_SOURCE[0]} -e development -t \"some_table,another_table,tag_name\"

        -p  name: partition
            required: no
            description: A value used as a parameter when looping through partitions for large/historical table loads. This
                         will execute a spark job for a single table and load a single partition for correctly configured
                         table. A single value in the tag/table parameter is required when using the partition option.
            example: ${BASH_SOURCE[0]} -e development -t xref_claims -p 201801"
        exit ;;
    t) TAGS="${OPTARG}" ;;
    p) PARTITION="${OPTARG}" ;;
    *) error "Unexpected option ${flag}" ;;
  esac
done

# make sure environment is provided
if [[ -z "$ENVIRONMENT" ]]; then
    echo "ERROR: Not all required variables have been specified. Please use the help flag (-h) to understand proper usage of script."
    exit 1
fi

# ensure tags option is specified when using the partition tag
if [[ -n "$PARTITION" && -z "$TAGS" ]]; then
    echo "ERROR: Tag/table name must be provided when supplying a partition. Please use the help flag (-h) to understand proper usage of script."
    exit 1
fi

# ensure only a single tag is provided when using the partition option
# this is a "lazy" check, as it only confirms this given the proper usage of the tag option
if [[ -n "$PARTITION" && "$TAGS" == *","* ]]; then
    echo "ERROR: Only one tag/table name can be provided when supplying a partition. Please use the help flag (-h) to understand proper usage of script."
    exit 1
fi

if [[ "$TAGS" == *\ * ]]; then
    echo "ERROR: Tag/table list cannot contain whitespace. Please use the help flag (-h) to understand proper usage of script."
    exit 1
fi

echo -e "INFO: Proceeding with the following:\n\tENVIRONMENT: ${ENVIRONMENT}\n\tTAGS: ${TAGS}\n\tPARTITION: ${PARTITION}"

case ${ENVIRONMENT^^} in
    DEVELOPMENT )
        DRIVER_MEMORY="16G"
        EXECUTOR_MEMORY="25G"
        EXECUTOR_CORES=4
        NUM_EXECUTORS=25
        PRINCIPAL=$(echo "$(whoami)@DEVAD.WELLPOINT.COM")
        YARN_QUEUE="root.ping_yarn"
            ;;
    TEST )
        DRIVER_MEMORY="16G"
        EXECUTOR_MEMORY="25G"
        EXECUTOR_CORES=4
        NUM_EXECUTORS=25
        PRINCIPAL=$(echo "$(whoami)@DEVAD.WELLPOINT.COM")
        YARN_QUEUE="root.ping_yarn"
            ;;
    DISCOVERY )
        DRIVER_MEMORY="24G"
        EXECUTOR_MEMORY="30G"
        EXECUTOR_CORES=4
        NUM_EXECUTORS=30
        PRINCIPAL=$(echo "$(whoami)@US.AD.WELLPOINT.COM")
        YARN_QUEUE="root.ping_yarn"
            ;;
    PRODUCTION )
        DRIVER_MEMORY="24G"
        EXECUTOR_MEMORY="60G"
        EXECUTOR_CORES=5
        NUM_EXECUTORS=35
        PRINCIPAL=$(echo "$(whoami)@US.AD.WELLPOINT.COM")
        YARN_QUEUE="root.ping_pool"
            ;;
    * )
        echo "Unrecognized environment specified. Exiting..."
        exit
            ;;
esac

FILE_BASE_PATH=`echo $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/${ENVIRONMENT,,}`

export JAVA_HOME=/usr/java/latest/

if [[ -n "${TAGS}" && -z "${PARTITION}" ]]; then
        spark2-submit --master yarn --deploy-mode cluster --queue "${YARN_QUEUE}" \
            --files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml \
            --name "PING CSBD HBase ETL: ${TAGS}" \
            --principal ${PRINCIPAL} \
            --keytab ${keytab} \
            --driver-memory ${DRIVER_MEMORY} \
            --executor-memory ${EXECUTOR_MEMORY} \
            --executor-cores ${EXECUTOR_CORES} \
            --num-executors ${NUM_EXECUTORS} \
            --conf "spark.driver.maxResultSize=5G" \
            --conf "spark.port.maxRetries=100" \
            --conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
            --conf "spark.yarn.security.tokens.hbase.enabled=true" \
            --conf "spark.dynamicAllocation.enabled=true" \
            --class "com.am.csbd.pi.etl.Driver" \
            ${FILE_BASE_PATH}/cogxHDFSHBaseETL-1.0.jar "${TAGS}"
elif [[ -n "${TAGS}" && -n "${PARTITION}" ]]; then
    DT_STR=$(echo "${PARTITION}" | cut -c -4)-$(echo "${PARTITION}" | cut -c 5-)-01
    DT=$(date -d "${DT_STR}" "+%Y-%m-%d")
    START_PARTITION=${PARTITION}
    STOP_PARTITION=$(date "+%Y%m")
    while [[ $PARTITION -le $STOP_PARTITION ]]; do
        echo "INFO: Loading ${TAGS}-${PARTITION}..."
	    spark2-submit --master yarn --deploy-mode cluster --queue "${YARN_QUEUE}" \
            --files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml \
            --name "PING CSBD HBase ETL: ${TAGS}-${PARTITION}" \
            --principal ${PRINCIPAL} \
	        --keytab ${keytab} \
	        --driver-memory 30G \
	        --executor-memory 35G \
	        --executor-cores 5 \
	        --num-executors 35 \
	        --conf "spark.driver.maxResultSize=5G" \
	        --conf "spark.port.maxRetries=100" \
	        --conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
            --conf "spark.yarn.security.tokens.hbase.enabled=true" \
            --conf "spark.dynamicAllocation.enabled=true" \
	        --class "com.am.cogx.pi.etl.Driver" \
	        ${FILE_BASE_PATH}/cogxHDFSHBaseETL-1.0.jar "${TAGS}" "${PARTITION}"
        RESULT=$?
        if [[ $RESULT -ne 0 ]]; then
            echo "Load for ${TAGS}-${PARTITION} failed with status ${RESULT}. Exiting now..."
            exit ${RESULT}
        else
            DT=$(date -d "${DT} +1 month" "+%Y-%m-%d")
            PARTITION=$(date -d "${DT}" "+%Y%m")
        fi
    done
else
    spark2-submit --master yarn --deploy-mode cluster --queue "${YARN_QUEUE}" \
        --files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml \
        --name "PING CSBD HBase ETL: All" \
        --principal ${PRINCIPAL} \
        --keytab ${keytab} \
        --driver-memory ${DRIVER_MEMORY} \
        --executor-memory ${EXECUTOR_MEMORY} \
        --executor-cores ${EXECUTOR_CORES} \
        --num-executors ${NUM_EXECUTORS} \
        --conf "spark.driver.maxResultSize=5G" \
        --conf "spark.port.maxRetries=100" \
        --conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
        --conf "spark.yarn.security.tokens.hbase.enabled=true" \
        --conf "spark.dynamicAllocation.enabled=true" \
        --class "com.am.csbd.pi.etl.Driver" \
        ${FILE_BASE_PATH}/cogxHDFSHBaseETL-1.0.jar
fi

exit_status=$?
echo "INFO: Exit status: ${exit_status}"

if [ ${exit_status} -eq 1 ]; then
    echo "ERROR: The CSBD HBase ETL process has failed."
else
    echo "INFO: The CSBD HBase ETL process has finished successfully."
fi

exit ${exit_status}
