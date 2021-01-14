#!/bin/sh
#====================================================================================================
# Title            : AVRO_extract_Wrapper
# ProjectName      : NDO
# Filename         : AVRO_EXTRACT_WRAPPER.sh
# Developer        : Legat
# Created on       : MAY 2019
# Location         : INDIA
# Date           Auth           Ver#     Modified By(Name)                 Change and Reason for Change
# ----------    -----        -----------------------------               --------------------------------------
# 2019/05/06     LEGATO         1         Initial Version
#====================================================================================================                                                                           #This script extract data from TERADATA through TDCH-fastexport to a HDFS location and ALTER the given table's location.        
#Usage : 
#To extract data with TDCH (provided, .avsc file is available)
#		sh TDCH_data_extract.sh GETDATA <SRC_DB> <SRC_TBL> <TGT_DB> <TGT_TBL>
#=================================================================================================================
source $HOME/.bash_profile
source $HOME/.profile

#Defaulting the profile variables for non-service ids
if [ "$(whoami)" != "srcpdppndobthpr" ]; then
        SCRIPTS_DIR="/pr/app/ve2/pdp/pndo/phi/no_gbd/r000/bin/scripts"
        HDFS_DIR="/pr/hdfsapp/ve2/pdp/pndo/phi/no_gbd/r000/bin"
        BIN_DIR="/pr/app/ve2/pdp/pndo/phi/no_gbd/r000/bin"
        LOG_DIR="/pr/app/ve2/pdp/pndo/phi/no_gbd/r000/bin/logs"
fi

LOG_FILE=$LOG_DIR/AVRO_SCHEMA_WRAPPER.log
source $SCRIPTS_DIR/mail.config
#exec 1> $LOG_FILE 2>&1

#Load generic functions/Variables
if [ -f $SCRIPTS_DIR/application_shell.properties ];
then
        source $SCRIPTS_DIR/application_shell.properties
else
        echo "ERROR - application_shell.properties not available in $SCRIPTS_DIR"
        exit -1
fi

for table in `cat $SCRIPTS_DIR/Datalabtable_Ehoppa.txt`;
do
echo $v_DL_TERADTA_EHOPPA_SRC_DB
echo $table

##############Extracting avsc file for all tables###########

sh -x $SCRIPTS_DIR/EXTRACT_AVRO_SCHEMA.sh $v_DL_TERADTA_EHOPPA_SRC_DB $table
#v_RC_CHECK $?

if [ $? -ne 0 ];then

echo -e "The Avro Extract Failed for $table" | mail -s "avsc file not extracted for $table" -r ${MAIL_FROM} ${MAIL_TO}
exit $v_ERROR
else
echo "The Avro Extract completed for $table successfully"  | mail -s "avsc file extracted succesfully for $table" -r ${MAIL_FROM} ${MAIL_TO}
fi
done

echo "Avro Extract for all tables completed"

