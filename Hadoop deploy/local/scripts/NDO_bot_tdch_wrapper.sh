


#!/bin/sh
#====================================================================================================
# Title            : BOT_TDCH_Wrapper
# ProjectName      : NDO
# Filename         : BOT_TDCH_Wrapper.sh
# Developer        : Legat
# Created on       : MAY 2019
# Location         : INDIA
# Date           Auth           Ver#     Modified By(Name)                 Change and Reason for Change
# ----------    -----        -----------------------------               --------------------------------------
# 2019/05/06     LEGATO         1         Initial Version
#====================================================================================================                                                                           #This script extract data from TERADATA through TDCH-fastexport to a HDFS location and ALTER the given table's location.
#Usage :
#To extract data with TDCH (provided, .avsc file is available)
#               sh TDCH_data_extract.sh GETDATA <SRC_DB> <SRC_TBL> <TGT_DB> <TGT_TBL>
#=================================================================================================================
source $HOME/.bash_profile
source $HOME/.profile

#Defaulting the profile variables for non-service ids
if [ "$(whoami)" != "srcpdppndobthpr" ]; then
        SCRIPTS_DIR="/ts/app/ve2/pdp/pndo/phi/no_gbd/r000/bin/scripts"
        HDFS_DIR="/ts/hdfsapp/ve2/pdp/pndo/phi/no_gbd/r000/bin"
        BIN_DIR="/ts/app/ve2/pdp/pndo/phi/no_gbd/r000/bin"
        LOG_DIR="/ts/app/ve2/pdp/pndo/phi/no_gbd/r000/bin/logs"
fi


#Load generic functions/Variables
if [ -f $SCRIPTS_DIR/application_shell.properties ];
then
        source $SCRIPTS_DIR/application_shell.properties
else
        echo "ERROR - application_shell.properties not available in $SCRIPTS_DIR"
        exit -1
fi

LOG_FILE=$LOG_DIR/"NDO_bot_tdch_wrapper_"$(v_DATETIME)"_"$USER.log
source $SCRIPTS_DIR/mail.config
exec 1> $LOG_FILE 2>&1

if [ $# -eq 1 ]
    then
        echo "Argument check completed"  >>$LOG_FILE
    else
                echo "Error in number of arguments passed, Script needs 1 arguments for execution"  >>$LOG_FILE
                exit 1
fi

export table=$1
echo "The table name to be loaded from source is $1" >>$LOG_FILE

if [[ $(fgrep -ix $table <<< "NDT_RATING_AREA") ]]; then
echo "Running the script for worktbl_NDT_RATING_AREA table" >>$LOG_FILE
hive_table="ndo_zip_submarket_xwalk"
td_table="WORKTBL_NDT_RATING_AREA_ACA"
v_DL_TERADTA_BOT_SRC_DB=$v_DL_TERADTA_XWLK
elif [[ $(fgrep -ix $table <<< "WORKTBL_NDT_RATING_AREA_ACA_ZIP3") ]]; then
echo "Running the script for WORKTBL_NDT_RATING_AREA_ACA_ZIP3 table" >>$LOG_FILE
hive_table="WORKTBL_NDT_RATING_AREA_ACA_ZIP3"
td_table="WORKTBL_NDT_RATING_AREA_ACA_ZIP3"
v_DL_TERADTA_BOT_SRC_DB=$v_DL_TERADTA_XWLK
elif [[ $(fgrep -ix $table <<< "BOT_PCR_FNCL_PR_CF") ]]; then
echo "Running the script for BOT_PCR_FNCL_PROD_CF table" >>$LOG_FILE
hive_table="BOT_PCR_FNCL_PROD_CF"
td_table="BOT_PCR_FNCL_PROD_CF"

else
echo "Running the script for BOT table" >>$LOG_FILE
hive_table=$table
td_table=$table
fi
##############Extracting avsc file for all tables###########

sh -x $SCRIPTS_DIR/EXTRACT_AVRO_SCHEMA.sh $v_DL_TERADTA_BOT_SRC_DB $td_table >>$LOG_FILE
#v_RC_CHECK $?

if [ $? -ne 0 ];then

echo -e "The Avro Extract Failed for $td_table" | mail -s "AVSC file not extracted for $td_table" -r ${MAIL_FROM} ${MAIL_TO} >>$LOG_FILE
exit $v_ERROR
else
echo "The Avro Extract completed for $td_table successfully"  | mail -s "AVSC extracted succesfully for $td_table" -r ${MAIL_FROM} ${MAIL_TO} >>$LOG_FILE
fi

beeline -u $v_BEELINE_SERVER -f $SCRIPTS_DIR/create_table_avro.hql --hiveconf mapred.job.queue.name=ndo_coca_yarn -hivevar hivedb=$v_HIVE_INB_NOGBD -hivevar hivetable=$hive_table -hivevar tdtable=$td_table -hivevar tdDB=$v_DL_TERADTA_BOT_SRC_DB

if [ $? -ne 0 ];then

echo -e "The avro schema for $td_table table got Failed " | mail -s "AVSC file not extracted for $td_table" -r ${MAIL_FROM} ${MAIL_TO} >>$LOG_FILE
exit $v_ERROR
else
echo "The Avro schema for $td_table table got completed  successfully"  | mail -s "AVSC extracted succesfully for $td_table" -r ${MAIL_FROM} ${MAIL_TO} >>$LOG_FILE
fi

sh -x $SCRIPTS_DIR/TDCH_EHOPPA_TDW.sh $v_DL_TERADTA_BOT_SRC_DB $td_table $v_HIVE_INB_NOGBD $hive_table >>$LOG_FILE
#v_RC_CHECK $?

if [ $? -ne 0 ];then

echo -e "The TDCH extract Failed for $td_table" | mail -s "TDCH extraction not completed for $td_table" -r ${MAIL_FROM} ${MAIL_TO} >>$LOG_FILE
exit $v_ERROR
else
echo "The TDCH Extract completed for $td_table successfully"  | mail -s "TDCH extraction completed succesfully for $td_table" -r ${MAIL_FROM} ${MAIL_TO} >>$LOG_FILE
fi

echo "Avro Extract and TDCH load for $td_table is completed" >>$LOG_FILE
exit $v_SUCCESS



