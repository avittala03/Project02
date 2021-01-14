#!/bin/sh
#====================================================================================================
# Title            : TDCH- Dataextract
# ProjectName      : NDO
# Filename         : TDCH_data_extract.sh
# Developer        : Anthem
# Created on       : JAN 2019
# Location         : ATLANTA
# Date           Auth           Ver#     Modified By(Name)                 Change and Reason for Change
# ----------    -----        -----------------------------               --------------------------------------
# 2019/01/07     Deloitte         1     Initial Version
#====================================================================================================                                                                           #This script extract data from TERADATA through TDCH-fastexport to a HDFS location and ALTER the given table's location.
#Usage :
#To extract data with TDCH (provided, .avsc file is available)
#               sh TDCH_data_extract.sh GETDATA <SRC_DB> <SRC_TBL> <TGT_DB> <TGT_TBL>
#=================================================================================================================

#Load the bash profile/ .profile/
source $HOME/.bash_profile
source $HOME/.profile

#Defaulting the profile variables for non-service ids
if [ "$(whoami)" != "srcpdppndobthpr" ]; then
        SCRIPTS_DIR="/pr/app/ve2/pdp/pndo/phi/no_gbd/r000/bin/scripts"
        HDFS_DIR="/pr/hdfsapp/ve2/pdp/pndo/phi/no_gbd/r000/bin"
        BIN_DIR="/pr/app/ve2/pdp/pndo/phi/no_gbd/r000/bin"
        LOG_DIR="/pr/app/ve2/pdp/pndo/phi/no_gbd/r000/bin/logs"
fi

#Load generic functions/Variables
if [ -f $SCRIPTS_DIR/application_shell.properties ];
then
        source ${SCRIPTS_DIR}/application_shell.properties
else
        echo "ERROR - application_shell.properties not available in $SCRIPTS_DIR"
        exit -1
fi

#Creating log file
SUBJECT_AREA="TDCH_DATA_EXTRACT"
LOG_FILE=$LOG_DIR/"script_"$SUBJECT_AREA"_"$(v_DATETIME)"_"$USER.log
echo "$(v_TIMESTAMP):INFO:Teradata-$SUBJECT_AREA Script logs in  $LOG_FILE"
echo "$(v_TIMESTAMP):INFO:-----------"$SUBJECT_AREA" started -------------">>$LOG_FILE

l_SRC_DB=$1
l_SRC_TBL=$2
l_TGT_DB=$3
l_TGT_TBL=$4

echo ${l_SRC_DB}>>$LOG_FILE
echo ${l_SRC_TBL}>>$LOG_FILE
#Parameter check
if [ $# -eq '4' ];then
        echo "$(v_TIMESTAMP):INFO:Parameters passed  : \n
        ,${l_SRC_DB}
        ,${l_SRC_TBL}
        ,${l_TGT_DB}
        ,${l_TGT_TBL}" >>$LOG_FILE

else
        echo "$(v_TIMESTAMP):ERROR:Incorrect Parameters passed  : \n
        ,${l_SRC_DB}
        ,${l_SRC_TBL}
        ,${l_TGT_DB}
        ,${l_TGT_TBL}" >>$LOG_FILE
exit $v_ERROR
fi

l_PASSWORD="tdwallet(`echo $v_TERADATA_USERID`)"
prefix='$'
l_TERADATA_PASSWORD=$prefix$l_PASSWORD

export l_CONDITIONS="1=1"
export l_TAB_DATA_PATH="$v_AVRO_HDFS_INBD_DATA${l_TGT_TBL}_"$(v_DATETIME)""
export l_TERADATA_TDCH_URL="jdbc:${v_TERADATA_TDCH_URL}/database=${l_SRC_DB},logmech=td2,tmode=ANSI,charset=UTF8"

echo "$(v_TIMESTAMP):INFO:Generated variables
l_TERADATA_PASSWORD -  $l_TERADATA_PASSWORD
l_TAB_DATA_PATH - $l_TAB_DATA_PATH
l_TERADATA_TDCH_URL=$l_TERADATA_TDCH_URL
">>$LOG_FILE


export l_CONDITIONS="1=1"


if [[ $(fgrep -ix $l_SRC_TBL <<< "outpatient_detail") ]]; then
export QUERY_SAMPLE=$QUERY_AVRO_OUTPAT_DET
export QUERY_SCHEMA=$QUERY_SCHEMA_OUTPAT_DET

elif [[ $(fgrep -ix $l_SRC_TBL <<< "facility_attribute_profile") ]]; then
export QUERY_SAMPLE=$QUERY_AVRO_FAC_ATT
export QUERY_SCHEMA=$QUERY_SCHEMA_FAC_ATT

elif [[ $(fgrep -ix $l_SRC_TBL <<< "inpatient_summary") ]]; then
export QUERY_SAMPLE=$QUERY_AVRO_INPAT_SUM
export QUERY_SCHEMA=$QUERY_SCHEMA_INPAT_SUM

elif [[ $(fgrep -ix $l_SRC_TBL <<< "outpatient_summary") ]]; then
export QUERY_SAMPLE=$QUERY_AVRO_OTPAT_SUM
export QUERY_SCHEMA=$QUERY_SCHEMA_OTPAT_SUM

elif [[ $(fgrep -ix $l_SRC_TBL <<< "admsn_src_cd") ]]; then
export QUERY_SAMPLE=$QUERY_AVRO_ADMN_SRC
export QUERY_SCHEMA=$QUERY_SCHEMA_ADMN_SRC

elif [[ $(fgrep -ix $l_SRC_TBL <<< "worktbl_NDO_QHIP") ]]; then
export QUERY_SAMPLE=$QUERY_WORKTBL_NDO_QHIP
export QUERY_SCHEMA=$QUERY_SCHEMA_WORKTBL_NDO_QHIP

else
export QUERY_SAMPLE=$DEFAULT_QUERY
export QUERY_SCHEMA=$DEFAULT_QUERY_SCHEMA
fi

echo "---------------------------------------------------------------------------------------------">>$LOG_FILE
echo " $l_TAB_DATA_PATH ----->>>> $l_TGT_TBL">>$LOG_FILE
echo "---------------------------------------------------------------------------------------------">>$LOG_FILE

if [ -f $v_AVRO_LOCAL_SCHEMA/$l_SRC_DB/$l_SRC_TBL/${l_SRC_TBL}.avsc ]
then
              if [[ $(fgrep -ix $l_TGT_TBL <<< "outpatient_detail") ]] || [[ $(fgrep -ix $l_TGT_TBL <<< "facility_attribute_profile") ]] || [[ $(fgrep -ix $l_TGT_TBL <<< "inpatient_summary") ]] || [[ $(fgrep -ix $l_TGT_TBL <<< "outpatient_summary") ]] || [[ $(fgrep -ix $l_TGT_TBL <<< "worktbl_NDO_QHIP") ]]; then

                        hadoop jar $v_TERADATA_TDCH_CONNECTOR_JAR com.teradata.connector.common.tool.ConnectorImportTool \
                        -Dmapreduce.job.queuename=$v_MAPRED_QUEUENAME \
                        -libjars $v_TERADATA_TDCH_LIB_JARS \
                        -classname com.teradata.jdbc.TeraDriver \
                        -url $l_TERADATA_TDCH_URL \
                        -username $v_TERADATA_USERID \
                        -password $l_TERADATA_PASSWORD \
                        -queryband "ApplicationName=${l_SRC_DB};MapNum=${v_TDCH_NUM_MAPPER};JobType=TDCH;JobOptions=InternalFastExport;Frequency=AdHoc;" \
                        -jobtype hdfs \
                        -fileformat avrofile \
                        -sourcetable $l_SRC_TBL \
                        -sourcefieldnames "${QUERY_SAMPLE}" \
                        -targetdatabase $l_TGT_DB \
                        -targettable $l_TGT_TBL \
                        -targettableschema "$QUERY_SCHEMA"\
                        -targetfieldnames "$QUERY_SAMPLE" \
                        -nummappers ${v_TDCH_NUM_MAPPER} \
                        -method internal.fastexport \
                        -fastexportsockettimeout 50000 \
                        -batchsize 100000 \
                        -separator ''\''\u0001'\''' \
                        -targetpaths  $l_TAB_DATA_PATH\
                        -avroschemafile file:////$v_AVRO_LOCAL_SCHEMA/${l_SRC_DB}/$l_SRC_TBL/${l_SRC_TBL}.avsc \
                        -sourceconditions "$l_CONDITIONS"  >>$LOG_FILE

                else

                        hadoop jar $v_TERADATA_TDCH_CONNECTOR_JAR com.teradata.connector.common.tool.ConnectorImportTool \
                        -Dmapreduce.job.queuename=$v_MAPRED_QUEUENAME \
                        -libjars $v_TERADATA_TDCH_LIB_JARS \
                        -classname com.teradata.jdbc.TeraDriver \
                        -url $l_TERADATA_TDCH_URL \
                        -username $v_TERADATA_USERID \
                        -password $l_TERADATA_PASSWORD \
                        -queryband "ApplicationName=${l_SRC_DB};MapNum=${v_TDCH_NUM_MAPPER};JobType=TDCH;JobOptions=InternalFastExport;Frequency=AdHoc;" \
                        -jobtype hdfs \
                        -fileformat avrofile \
                        -sourcetable $l_SRC_TBL \
                        -nummappers ${v_TDCH_NUM_MAPPER} \
                        -method internal.fastexport \
                        -fastexportsockettimeout 50000 \
                        -batchsize 100000 \
                        -separator ''\''\u0001'\''' \
                        -targetpaths  $l_TAB_DATA_PATH\
                        -avroschemafile file:////$v_AVRO_LOCAL_SCHEMA/${l_SRC_DB}/$l_SRC_TBL/${l_SRC_TBL}.avsc \
                        -sourceconditions $l_CONDITIONS >>$LOG_FILE


                      fi
                        if [ $? -eq '0' ];then
                                beeline -u ${v_BEELINE_SERVER} -e "ALTER TABLE ${l_TGT_DB}.${l_TGT_TBL} SET LOCATION '${l_TAB_DATA_PATH}';"
                                v_RC_CHECK "$?" "Beeline execution -ALTER TABLE ${l_TGT_DB}.${l_TGT_TBL} SET LOCATION ${l_TAB_DATA_PATH}"
                                   
                                   if [[ $(fgrep -ix $l_TGT_TBL <<< "worktbl_NDO_QHIP") ]]; then

                                beeline -u ${v_BEELINE_SERVER} -e "DROP table IF EXISTS ${l_TGT_DB}.qhip_test"
                                beeline -u ${v_BEELINE_SERVER} -e "CREATE table IF NOT EXISTS ${l_TGT_DB}.qhip_test as select trim(medcr_id) as medcr_id,trim(prov_nm) as prov_nm,trim(qhip_nbr) as qhip_nbr,date_format(from_unixtime(qhip_strt_dt DIV 1000),'MM/dd/yyyy') as qhip_strt_dt,date_format(from_unixtime(qhip_end_dt DIV 1000),'MM/dd/yyyy') as qhip_end_dt from ${l_TGT_DB}.${l_TGT_TBL};"

                                  fi
                        else
                        exit $v_ERROR
                        fi
                v_RC_CHECK "$?" 'tdch'
else
        echo "$(v_TIMESTAMP):ERROR:Schema file not available - $v_AVRO_LOCAL_SCHEMA/$l_SRC_DB/$l_SRC_TBL/${l_SRC_TBL}.avsc">>$LOG_FILE
        exit $v_ERROR
fi

#========================================
echo "TDCH extract completed succesfully"
#========================================