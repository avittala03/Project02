#=================================================================================================================================================
#!/bin/sh
# Title            : NDO_PROF_REF_PATTERNS_ABC_Wrapper
# ProjectName      : NDO
# Filename         : NDO_PROF_REF_PATTERNS_ABC_Wrapper.sh
# Description      : Shell wrapper Script for Professional Referral Patterns
# Developer        : Anthem
# Created on       : DEC 2017
# Location         : ATLANTA
# Date           Auth           Ver#     Modified By(Name)                 Change and Reason for Change
# ----------    -----        -----------------------------               --------------------------------------
# 2017/12/18     Deloitte         1     Initial Version
#================================================================================================================
 

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

LOG_FILE=$LOG_DIR/"script_NDO_PROF_REF_PATTERNS_Main_"$(v_DATETIME)"_"$USER.log
echo "$(v_TIMESTAMP):INFO:Professional Referral Patterns Script logs in  $LOG_FILE"
echo "$(v_TIMESTAMP):INFO:Professional Referral Patterns Script triggered">>$LOG_FILE

#Executing the shell script for Rendering Spend 

/bin/sh $SCRIPTS_DIR/NDO_PROF_REF_PATTERNS_Wrapper.sh "ProfReferralPatternsDriverABC"
v_RC_CHECK "$?" "Step1 - SPARK JOB FOR part-ABC" >>$LOG_FILE


/bin/sh $SCRIPTS_DIR/NDO_PROF_REF_PATTERNS_Wrapper.sh "ProfReferralPatternsDriverD"
v_RC_CHECK "$?" "Step1 - SPARK JOB FOR part-D" >>$LOG_FILE


/bin/sh $SCRIPTS_DIR/NDO_PROF_REF_PATTERNS_Wrapper.sh "ProfReferralPatternsDriverE"
v_RC_CHECK "$?" "Step1 - SPARK JOB FOR part-E" >>$LOG_FILE


/bin/sh $SCRIPTS_DIR/NDO_PROF_REF_PATTERNS_Wrapper.sh "ProfReferralPatternsDriverF"
v_RC_CHECK "$?" "Step1 - SPARK JOB FOR part-F" >>$LOG_FILE


/bin/sh $SCRIPTS_DIR/NDO_PROF_REF_PATTERNS_Wrapper.sh "ProfReferralPatternsDriverG"
v_RC_CHECK "$?" "Step1 - SPARK JOB FOR part-F" >>$LOG_FILE

exit $v_SUCCESS

#====================================================================================================================
##  End of Script      
#====================================================================================================================
