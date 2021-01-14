
#!/bin/sh
#====================================================================================================
# Title            : TDCH-Dataextract
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
        SCRIPTS_DIR="/pr/app/vs2/pdp/pndo/phi/no_gbd/r000/bin/scripts"
        HDFS_DIR="/pr/hdfsapp/vs2/pdp/pndo/phi/no_gbd/r000/bin"
        BIN_DIR="/pr/app/vs2/pdp/pndo/phi/no_gbd/r000/bin"
        LOG_DIR="/pr/app/vs2/pdp/pndo/phi/no_gbd/r000/bin/logs"
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


#v_TERADATA_USERID="AF05436"

l_PASSWORD="tdwallet(`echo $v_TERADATA_USERID`)"
prefix='$'
l_TERADATA_PASSWORD=$prefix$l_PASSWORD
export l_CONDITIONS="1=1"
export l_TAB_DATA_PATH="$v_AVRO_HDFS_INBD_DATA${l_TGT_TBL}_"$(v_DATETIME)""
export l_TERADATA_TDCH_URL="jdbc:${v_TERADATA_TDCH_URL}/database=${l_SRC_DB},logmech=TD2,tmode=ANSI,charset=UTF8"

echo "$(v_TIMESTAMP):INFO:Generated variables
l_TERADATA_PASSWORD -  $l_TERADATA_PASSWORD
l_TAB_DATA_PATH - $l_TAB_DATA_PATH
l_TERADATA_TDCH_URL=$l_TERADATA_TDCH_URL

 ">>$LOG_FILE
echo "---------------------------------------------------------------------------------------------">>$LOG_FILE
echo " $l_TAB_DATA_PATH ----->>>> $l_TGT_TBL">>$LOG_FILE
echo "---------------------------------------------------------------------------------------------">>$LOG_FILE

if [ -f $v_AVRO_LOCAL_SCHEMA/$l_SRC_DB/$l_SRC_TBL/${l_SRC_TBL}.avsc ]
then
					if [[ $(fgrep -ix $l_TGT_TBL <<< "outpatient_detail") ]]; then
					echo "$(v_TIMESTAMP):INFO: TDCH extract jar - /n
                        hadoop jar $v_TERADATA_TDCH_CONNECTOR_JAR com.teradata.connector.common.tool.ConnectorImportTool \
                        -Dmapreduce.job.queuename=$v_MAPRED_QUEUENAME \
                        -libjars $v_TERADATA_TDCH_LIB_JARS \
                        -classname com.teradata.jdbc.TeraDriver \
                        -url $l_TERADATA_TDCH_URL \
                        -username $v_TERADATA_USERID \
                        -password $l_TERADATA_PASSWORD \
                        -queryband 'ApplicationName=${l_SRC_DB};MapNum=${v_TDCH_NUM_MAPPER};JobType=TDCH;JobOptions=InternalFastExport;Frequency=AdHoc;' \
                        -jobtype hdfs \
                        -fileformat avrofile \
                        -sourcetable $l_SRC_TBL \
                        -sourcefieldnames 'MBR_State,MBR_County,PROV_ST_NM,PROV_County,MEDCR_ID,MBR_ZIP3,MBR_ZIP_CD,prodlvl3,fundlvl2,MBUlvl2,MBUlvl4,MBUlvl3,INN_CD,MCS,liccd,MBU_CF_CD,Inc_Month' \
                        -targetdatabase $l_TGT_DB \
                        -targettable $l_TGT_TBL \
                        -targettableschema 'MBR_State string,MBR_County string,PROV_ST_NM string,PROV_County string,MEDCR_ID string,MBR_ZIP3 string,MBR_ZIP_CD string,prodlvl3 string,fundlvl2 string,MBUlvl2 string,MBUlvl4 string,MBUlvl3 string,INN_CD string,MCS string,liccd string,MBU_CF_CD string,Inc_Month string' \
                        -targetfieldnames 'MBR_State,MBR_County,PROV_ST_NM,PROV_County,MEDCR_ID,MBR_ZIP3,MBR_ZIP_CD,prodlvl3,fundlvl2,MBUlvl2,MBUlvl4,MBUlvl3,INN_CD,MCS,liccd,MBU_CF_CD,Inc_Month' \
                        -nummappers ${v_TDCH_NUM_MAPPER} \
                        -method internal.fastexport \
                        -fastexportsocketport 8678 \
                        -fastexportsockettimeout 50000 \
                        -batchsize 100000 \
                        -separator ''\''\u0001'\''' \
                        -targetpaths  $l_TAB_DATA_PATH\
                        -avroschemafile file:////$v_AVRO_LOCAL_SCHEMA/${l_SRC_DB}/$l_SRC_TBL/${l_SRC_TBL}.avsc \
                        -sourceconditions 1=1">>$LOG_FILE

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
                        -sourcefieldnames "MBR_State,MBR_County,PROV_ST_NM,PROV_County,MEDCR_ID,MBR_ZIP3,MBR_ZIP_CD,prodlvl3,fundlvl2,MBUlvl2,MBUlvl4,MBUlvl3,INN_CD,MCS,liccd,MBU_CF_CD,Inc_Month" \
                        -targetdatabase $l_TGT_DB \
                        -targettable $l_TGT_TBL \
                        -targettableschema "MBR_State string,MBR_County string,PROV_ST_NM string,PROV_County string,MEDCR_ID string,MBR_ZIP3 string,MBR_ZIP_CD string,prodlvl3 string,fundlvl2 string,MBUlvl2 string,MBUlvl4 string,MBUlvl3 string,INN_CD string,MCS string,liccd string,MBU_CF_CD string,Inc_Month string" \
                        -targetfieldnames "MBR_State,MBR_County,PROV_ST_NM,PROV_County,MEDCR_ID,MBR_ZIP3,MBR_ZIP_CD,prodlvl3,fundlvl2,MBUlvl2,MBUlvl4,MBUlvl3,INN_CD,MCS,liccd,MBU_CF_CD,Inc_Month" \
                        -nummappers ${v_TDCH_NUM_MAPPER} \
                        -method internal.fastexport \
                        -fastexportsocketport 8678 \
                        -fastexportsockettimeout 50000 \
                        -batchsize 100000 \
                        -separator ''\''\u0001'\''' \
                        -targetpaths  $l_TAB_DATA_PATH\
                        -avroschemafile file:////$v_AVRO_LOCAL_SCHEMA/${l_SRC_DB}/$l_SRC_TBL/${l_SRC_TBL}.avsc \
                        -sourceconditions $l_CONDITIONS >>$LOG_FILE
						
					else
                        echo "$(v_TIMESTAMP):INFO: TDCH extract jar - /n
                                hadoop jar $v_TERADATA_TDCH_CONNECTOR_JAR com.teradata.connector.common.tool.ConnectorImportTool \
                        -Dmapreduce.job.queuename=$v_MAPRED_QUEUENAME \
                        -libjars $v_TERADATA_TDCH_LIB_JARS \
                        -classname com.teradata.jdbc.TeraDriver \
                        -url $l_TERADATA_TDCH_URL \
                        -username $v_TERADATA_USERID \
                        -password $l_TERADATA_PASSWORD \
                        -queryband ApplicationName=${l_SRC_DB};MapNum=${v_TDCH_NUM_MAPPER};JobType=TDCH;JobOptions=InternalFastExport;Frequency=AdHoc; \
                        -jobtype hdfs \
                        -fileformat avrofile \
                        -sourcetable $l_SRC_TBL \
                        -nummappers ${v_TDCH_NUM_MAPPER} \
                        -method internal.fastexport \
                        -fastexportsocketport 8678 \
                        -fastexportsockettimeout 50000 \
                        -batchsize 100000 \
                        -separator ''\''\u0001'\''' \
                        -targetpaths  $l_TAB_DATA_PATH\
                        -avroschemafile file:////$v_AVRO_LOCAL_SCHEMA/${l_SRC_DB}/$l_TGT_TBL/${l_TGT_TBL}.avsc \
                        -sourceconditions 1=1">>$LOG_FILE
					
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
                        -fastexportsocketport 8678 \
                        -fastexportsockettimeout 50000 \
                        -batchsize 100000 \
                        -separator ''\''\u0001'\''' \
                        -targetpaths  $l_TAB_DATA_PATH\
                        -avroschemafile file:////$v_AVRO_LOCAL_SCHEMA/${l_SRC_DB}/$l_SRC_TBL/${l_SRC_TBL}.avsc \
                        -sourceconditions $l_CONDITIONS >>$LOG_FILE
				
					fi
                        if [ $? -eq '0' ];then
#                                kinit -kt /home/af05436/AF05436.keytab -p AF05436@DEVAD.WELLPOINT.COM
				beeline -u ${v_BEELINE_SERVER} -e "ALTER TABLE ${l_TGT_DB}.${l_TGT_TBL} SET LOCATION '${l_TAB_DATA_PATH}';"
                                v_RC_CHECK "$?" "Beeline execution -ALTER TABLE ${l_TGT_DB}.${l_TGT_TBL} SET LOCATION ${l_TAB_DATA_PATH}"
                        else
                        exit $v_ERROR
                        fi
                v_RC_CHECK "$?" 'tdch'
else
        echo "$(v_TIMESTAMP):ERROR:Schema file not available - $v_AVRO_LOCAL_SCHEMA/$l_SRC_DB/$l_SRC_TBL/${l_SRC_TBL}.avsc">>$LOG_FILE
        exit $v_ERROR
fi



######################################## UPDATE LOCATION OF THE TARGET TABLE

