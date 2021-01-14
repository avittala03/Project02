#!/bin/bash

set -e

if [ "$#" -ne 1 ]; then
    echo "Need to specify argument for ENVIRONMENT [DEVELOPMENT/TEST/DISCOVERY/PRODUCTION}. Exiting..."
    exit 1
fi

ENVIRONMENT="${1}"
if [ "${ENVIRONMENT^^}" != "DEVELOPMENT" ] && [[ "${ENVIRONMENT^^}" != "TEST" ]] && [[ "${ENVIRONMENT^^}" != "DISCOVERY" ]] && [[ "${ENVIRONMENT^^}" != "PRODUCTION" ]]; then
    echo "${ENVIRONMENT} is not a valid value for ENVIRONMENT argument. Please specify TEST, DISCOVERY, or PRODUCTION. Exiting..."
    exit 1
fi

CONF_DIR=`echo $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/deploy/${ENVIRONMENT,,}`
DDL_FILE="${CONF_DIR}/hbase_ddl.sql"

while read DDL; do
    printf "Executing the following HBase DDL:\n\t${DDL}\n"
    ERROR=$(echo ${DDL} | hbase --config ${CONF_DIR} shell | grep ERROR:)
    if [ "${ERROR}" == "" ]; then
        echo "HBase DDL executed successfully."
    else
        echo "WARN: HBase DDL execution returned the following error: ${ERROR}"
    fi
done << EOF
$(cat ${DDL_FILE})
EOF