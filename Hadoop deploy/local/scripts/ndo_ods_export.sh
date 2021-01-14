#!/bin/sh
#====================================================================================================================================================
# Title            : Script to run Sqoop Job
# ProjectName      : NDO
# Filename         : SQOOP.sh
# Description      : Script to run Sqoop Job which exports  Tables from  HDFS to MySQL
# Developer        : Anthem
# Created on       : AUGUST 2017
# Location         : ATLANTA
# Logic            : 
# Parameters       : Environment Variables file name
# Execution        :sh SQOOP.sh "Environment Variables file name"
# Return codes     : 
# Date                         Ver#     Modified By(Name)                 Change and Reason for Change
# ----------    -----        -----------------------------               --------------------------------------
# 2017/08/31                      1     Initial Version

#  ***************************************************************************************************************************

######### This script does the following 
######### 1. Checks for  the  records count of the source table   in HDFS  location  - Only if needed
######### 2. Checks for  the  records count of the target table   in MySQL  location ,before  loading  - Only if needed
######### 3. Executes  the Sqoop Script  and loads  the Data  into Target  folder  in MySQL.
######### 4. Checks for  the  records count of the target table   in MySQL  location , after  loading  - Only if needed
######### 5.


echo "Execution of  Script start from here"




#Source the parameter file 
echo $1 
source $1


#Conf Parameters
echo $TABLENAME
echo $SOURCETABLENAME
echo $USER
echo $CREDENTIAL_PROVIDER_PATH
echo $CONNECTION_STRING
echo $DBUSERNAME
echo $HIVEDB
echo $SCHEMA_NAME
echo $column_names
echo $DEL_QUERY


#deleting the existing data#
echo "deleting   the existing records have been  started"
sqoop eval -Dhadoop.security.credential.provider.path=$CREDENTIAL_PROVIDER_PATH --connect "$CONNECTION_STRING" -username $DBUSERNAME --password-file hdfs:///user/srcpdppndobthpr/sqlserver_pass_prod.txt --query "$DEL_QUERY"





