#!/bin/sh
#====================================================================================================================================================
# Title            : Script to copy from  linux to HDFS  and hdfs to hdfs.
# ProjectName      : NDO
# Filename         : sqoop_export_property_files_copy.sh
# Description      : Script to copy from  linux to HDFS  and hdfs to hdfs.
# Developer        : Anthem
# Created on       : OCT 2017
# Location         : ATLANTA
# Logic            : 
# Parameters       : Environment Variables file name
# Execution        :sh sqoop_export_property_files_copy.sh "Environment Variables file name"
# Return codes     : 
# Date                         Ver#     Modified By(Name)                 Change and Reason for Change
# ----------    -----        -----------------------------               --------------------------------------
# 2017/08/31                      1     Initial Version

#  ***************************************************************************************************************************

######### This script does the following 
######### 1. Creating  a directory  as "scripts,conf,sql and jar"
######### 2. Creating a directory as "Septrelease"
######### 3. Creating a directory as "scripts,conf,sql and jar" under "Septrelease"
######### 4. Copying data from Linux(scripts,conf,sql and jar) to   HDFS
######### 5.Copying application conf file  hdfs/(scripts,conf,sql and jar) to hdfs/archive



echo "Execution of  Script start from here"




#Source the parameter file 
echo $1 
source $1


#Conf Parameters
echo $HDFSPATH
echo $EDGENODE


##### Creating  a directory  as  "conf" under   edge node
 
mkdir -p $EDGENODE/conf
 
 ##### Creating  a directory  as  "jar" under   edge node
 
mkdir -p $EDGENODE/jar

##### Creating  a directory  as  "sql" under   edge node
 
mkdir -p $EDGENODE/sql

##### Creating  a directory  as  "scripts" under   edge node
 
mkdir  -p $EDGENODE/scripts
###################################################################################################################################################### 
 
##### Creating  a directory  as "conf" under   HDFS

hadoop fs -mkdir -p $HDFSPATH/conf

hadoop  fs -chmod 755 $HDFSPATH/conf


##### Creating  a directory  as "jar" under   HDFS

hadoop fs -mkdir  -p $HDFSPATH/jar

hadoop  fs -chmod 755 $HDFSPATH/jar

##### Creating  a directory  as "sql" under   HDFS

hadoop fs -mkdir -p $HDFSPATH/sql

hadoop  fs -chmod 755 $HDFSPATH/sql

##### Creating  a directory  as "scripts" under   HDFS

hadoop fs -mkdir  -p $HDFSPATH/scripts

hadoop  fs -chmod 755 $HDFSPATH/scripts

###################################################################################################################################################

#####Copying data from Linux  to   HDFS
hdfs dfs –put –f $EDGENODE/conf/application.conf $HDFSPATH/conf

#####Copying data from Linux  to   HDFS
hdfs dfs –put –f $EDGENODE/jar/* $HDFSPATH/jar/

#####Copying data from Linux  to   HDFS
hdfs dfs –put –f $EDGENODE/sql/* $HDFSPATH/sql/

#####Copying data from Linux  to   HDFS
hdfs dfs –put –f $EDGENODE/scripts/* $HDFSPATH/scripts/















