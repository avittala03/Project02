
#!/bin/sh
#====================================================================================================================================================
# Title            : BitBucket_prod_Migration
# ProjectName      : NDO_ETL
# Filename         : NDO_BitBucket_Prod_Code_Migration_Release.sh
# Description      : Script moves all the bibucket files to prod respective environment 
# Developer        : Anthem
# Created on       : Jan 2018
# Location         : ATLANTA
# Logic            : 
# Parameters       : 
# Return codes     : 
# Date                         Ver#     Modified By(Name)                 Change and Reason for Change
# ----------    -----        -----------------------------               --------------------------------------
# 2018/01/04                      1     Initial Version
#====================================================================================================================================================

echo "Execution of Script starts from here"
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
        source $SCRIPTS_DIR/application_shell.properties
else
        echo "ERROR - application_shell.properties not available in $SCRIPTS_DIR"
        exit -1
fi


#Check the parameters passed
if [ $# -eq 1 ];then
        TBL_NM=`echo $1 | tr [a-z] [A-Z]`
        echo "$(v_TIMESTAMP):INFO:Backup process started for $TBL_NM"
else
        echo "$(v_TIMESTAMP):ERROR:Incorrect parameters passed- $TBL_NM"
    exit $v_ERROR
fi

#LOG FILE
LOG_FILE=${LOG_DIR}/${TBL_NM}"_backup_script_"$(v_DATETIME)"_"$USER.log
exec 1> $LOG_FILE 2>&1


bkptablename=$1"_bkp"
echo $bkptablename

#-----------------------------------------------------------------------------------------------------------------------
#Checking for the count in source table ------------------------------------------

SourceCount="$(beeline -u $v_BEELINE_SERVER -hiveconf hivedb=$v_HIVE_OUB_ALLOB -hiveconf sourcetablename=$1 --outputformat=tsv2 --showHeader=false -e "select COUNT(*) FROM "'${hiveconf:hivedb}'"."'${hiveconf:sourcetablename}'"")"

if [ $? -ne 0 ];then
	echo "HQL Query execution failed. Please Check"
	 exit $v_ERROR
else 
	echo "HQL Query execution is successful."
fi 
#-----------------------------------------------------------------------------------------------------------------------

runid="$(beeline -u $v_BEELINE_SERVER -hiveconf hivedb=$v_HIVE_OUB_ALLOB -hiveconf sourcetablename=$1 --outputformat=tsv2 --showHeader=false -e "select run_id FROM "'${hiveconf:hivedb}'"."'${hiveconf:sourcetablename}'" limit 1")"

if [ $? -ne 0 ];then
	echo "HQL Query execution failed. Please Check"
	 exit $v_ERROR
else 
	echo "HQL Query execution is successful."
fi 
#-----------------------------------------------------------------------------------------------------------------------

#Backup table load 
beeline -u $v_BEELINE_SERVER -f $SCRIPTS_DIR/backupexec.hql -hivevar hivedb=$v_HIVE_OUB_ALLOB -hivevar sourcetablename=$1 -hivevar bkptablename=$bkptablename

if [ $? -ne 0 ];then
	echo "HQL Query execution failed. Please Check"
	 exit $v_ERROR
else 
	echo "HQL Query execution is successful."
fi 
#-----------------------------------------------------------------------------------------------------------------------

#Validating the source and target table count
TargetCount="$(beeline -u $v_BEELINE_SERVER -hiveconf hivedb=$v_HIVE_OUB_ALLOB -hiveconf bkptablename=$bkptablename -hiveconf runid=$runid --outputformat=tsv2 --showHeader=false -e "select COUNT(*) FROM "'${hiveconf:hivedb}'"."'${hiveconf:bkptablename}'" where run_id="'${hiveconf:runid}'"")"

if [ $? -ne 0 ];then
	echo "HQL Query execution failed. Please Check"
	 exit $v_ERROR
else 
	echo "HQL Query execution is successful."
fi 
#-----------------------------------------------------------------------------------------------------------------------
	
if [ "$SourceCount" -eq "$TargetCount" ];then
        echo "$(v_TIMESTAMP):INFO: Validation completed succesfully."
       exit $v_SUCCESS
else
        echo "$(v_TIMESTAMP):ERROR: Validation failed."
		exit $v_ERROR
fi

v_RC_CHECK $? "Spark-Backup job"

