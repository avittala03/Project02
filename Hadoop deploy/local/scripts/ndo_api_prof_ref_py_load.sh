
#=============================================================================================================
#!/bin/sh
# Title            : NDO_PROF_REF_PATTERNS_python_load
# ProjectName      : NDO
# Filename         : NDO_PROF_REF_PATTERNS_python_load.sh
# Developer        : Anthem
# Created on       : FEB 2018
# Location         : ATLANTA
# Date           Auth           Ver#     Modified By(Name)                 Change and Reason for Change
# ----------    -----        -----------------------------               --------------------------------------
# 2018/02/01     Deloitte         1     Initial Version
#=============================================================================================================

#Load the bash profile/ .profile/
source $HOME/.bash_profile
source $HOME/.profile

#Load generic functions/Variables
if [ -f $SCRIPTS_DIR/application_shell.properties ];
then
	source $SCRIPTS_DIR/application_shell.properties
else
	echo "ERROR - application_shell.properties not available in $SCRIPTS_DIR"
	exit -1
fi

#Defaulting the profile variables for non-service ids
if [ "$(whoami)" != "srcpdppndobthpr" ]; then
	SCRIPTS_DIR="/ts/app/vs2/pdp/pndo/phi/no_gbd/r000/bin/scripts"
	HDFS_DIR="/ts/hdfsapp/vs2/pdp/pndo/phi/no_gbd/r000/bin"
	BIN_DIR="/ts/app/vs2/pdp/pndo/phi/no_gbd/r000/bin"
	LOG_DIR="/ts/app/vs2/pdp/pndo/phi/no_gbd/r000/bin/logs"
fi


HIVE_SCHEMA="$v_HIVE_OUB_ALLOB"
HIVE_PROF_REF_TGT_TBL_NAME="API_PROF_REFERRAL_CASTED"
HIVE_PROF_REF_SRC_TBL_NAME="API_PROF_REFERRAL"


LOG_FILE=$LOG_DIR/"script_NDO_PROF_REF_PATTERNS_Main_"$(v_DATETIME)"_"$USER.log
echo "$(v_TIMESTAMP):INFO:Professional Referral Patterns Script logs in  $LOG_FILE"
echo "$(v_TIMESTAMP):INFO:Professional Referral Patterns Script triggered">>$LOG_FILE
echo "$(v_TIMESTAMP):INFO:Professional Referral Patterns -$SUBJECT_AREA Script logs in  $LOG_FILE"


/bin/sh $SCRIPTS_DIR/prof_referral_python/ndo-prof-referral-disc/ref_pattern.sh prd $SCRIPTS_DIR/prof_referral_python
if [ $? -ne 0 ];then

echo  "Step1 - PYTHON JOB FOR Professional Referral Pattern got failed" >>$LOG_FILE
exit $v_ERROR
else
echo "Step1 - PYTHON JOB FOR Professional Referral Pattern got completed" >>$LOG_FILE
fi
 

beeline -u $v_BEELINE_SERVER --headerInterval=400 --maxColumnWidth=20 --hiveconf Dmapreduce.job.queuename=ndo_coca_yarn --hiveconf hivedb=$HIVE_SCHEMA --hiveconf hivetargettbl=$HIVE_PROF_REF_TGT_TBL_NAME --hiveconf hivesourcetbl=$HIVE_PROF_REF_SRC_TBL_NAME -e "DROP TABLE IF EXISTS "'${hiveconf:hivedb}.${hiveconf:hivetargettbl}'""

beeline -u $v_BEELINE_SERVER --headerInterval=400 --maxColumnWidth=20 --hiveconf Dmapreduce.job.queuename=ndo_coca_yarn --hiveconf hivedb=$HIVE_SCHEMA --hiveconf hivetargettbl=$HIVE_PROF_REF_TGT_TBL_NAME --hiveconf hivesourcetbl=$HIVE_PROF_REF_SRC_TBL_NAME -e "CREATE TABLE IF NOT EXISTS "'${hiveconf:hivedb}.${hiveconf:hivetargettbl}'" AS SELECT st_cd,lob_id,prod_id ,rfrl_npi,rfrl_prov_spclty_desc,rndrg_npi ,rndrg_prov_spclty_desc,cast (explct_clm_amt  as decimal(18, 2)) as explct_clm_amt  ,cast (explct_clm_cnt  as int) as explct_clm_cnt ,cast (implct_clm_amt  as decimal(18, 2)) as implct_clm_amt,cast (implct_clm_cnt   as decimal(18, 2)) as implct_clm_cnt,snap_year_mnth_nbr  FROM "'${hiveconf:hivedb}.${hiveconf:hivesourcetbl}'""

if [ $? -ne 0 ];then

echo  "Table creation got failed"  >>$LOG_FILE
exit $v_ERROR
else
echo "Table creation is completed successfully"   >>$LOG_FILE
fi

echo "$(v_TIMESTAMP):INFO:Professional Referral Patterns Script completed">>$LOG_FILE



#=============================================================================================================
##  End of Script
#=============================================================================================================

