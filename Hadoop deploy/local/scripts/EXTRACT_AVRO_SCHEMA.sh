#====================================================================================================
#!/bin/sh
# Title            :  EXTRACT_AVRO_SCHEMA
# ProjectNome      : NDO
# Filename         : EXTRACT_AVRO_SCHEMA.sh
# Developer        : Anthem
# Created on       : FEB 2019
# Location         : ATLANTA
# Date           Auth           Ver#     Modified By(Name)                 Change and Reason for Change
# ----------    -----        -----------------------------               --------------------------------------
# 2019/01/31     Deloitte         1     Initial Version
#====================================================================================================               ##********************* ALERT !!! - TABLE NAME/FILE NAMES ARE CASE SENSITIVE
#
#=================================================================================================================
#Load the bash profile/ .profile/
source $HOME/.bash_profile
source $HOME/.profile

#Defaulting the profile variables for non-service ids
if [ "$(whoami)" != "srcpdppndobthpr" ]; then
        SCRIPTS_DIR="/pr/app/ve2/pdp/pndo/phi/no_gbd/r000/bin/scripts"
        HDFS_DIR="/pr/hdfsapp/ve2/pdp/pndo/phi/no_gbd/r000/bin"
        BIN_DIR="/pr/app/vs2/ve2/pndo/phi/no_gbd/r000/bin"
        LOG_DIR="/pr/app/vs2/ve2/pndo/phi/no_gbd/r000/bin/logs"
fi

#Load generic functions/Variables
if [ -f $SCRIPTS_DIR/application_shell.properties ];
then
        source $SCRIPTS_DIR/application_shell.properties
else
        echo "ERROR - application_shell.properties not available in $SCRIPTS_DIR"
        exit -1
fi

#Variables
SRC_DB=$1
SRC_TBL=$2
if [ $# -ne '2' ]; then

        exit $v_ERROR
fi
CONDITIONS="1=1"
export QUERY_SAMPLE=$DEFAULT_QUERY

echo "$(v_TIMESTAMP):INFO: Schema Generation started for  ${SRC_DB}.${SRC_TBL}"

########################################################################################
if [[ $(fgrep -ix $SRC_TBL <<< "outpatient_detail") ]]; then
export QUERY_SAMPLE=$QUERY_AVRO_OUTPAT_DET

elif [[ $(fgrep -ix $SRC_TBL <<< "facility_attribute_profile") ]]; then
export QUERY_SAMPLE=$QUERY_AVRO_FAC_ATT

elif [[ $(fgrep -ix $SRC_TBL <<< "inpatient_summary") ]]; then
export QUERY_SAMPLE=$QUERY_AVRO_INPAT_SUM

elif [[ $(fgrep -ix $SRC_TBL <<< "outpatient_summary") ]]; then
export QUERY_SAMPLE=$QUERY_AVRO_OTPAT_SUM

elif [[ $(fgrep -ix $SRC_TBL <<< "admsn_src_cd") ]]; then
export QUERY_SAMPLE=$QUERY_AVRO_ADMN_SRC

elif [[ $(fgrep -ix $SRC_TBL <<< "worktbl_NDO_QHIP") ]]; then
export QUERY_SAMPLE=$QUERY_WORKTBL_NDO_QHIP

else
export QUERY_SAMPLE=$DEFAULT_QUERY

fi

echo "$(v_TIMESTAMP):INFO: Schema Generation started for  ${SRC_DB}.${SRC_TBL}"

if [ -s "$v_AVRO_LOCAL_SCHEMA/$SRC_DB/$SRC_TBL/${SRC_TBL}.avsc" ];then
        echo "$(v_TIMESTAMP):INFO: Schema available $v_AVRO_LOCAL_SCHEMA/$SRC_DB/$SRC_TBL/${SRC_TBL}.avsc"
        echo "Removing the old .avsc file"
        rm $v_AVRO_LOCAL_SCHEMA/$SRC_DB/$SRC_TBL/${SRC_TBL}.avsc
fi

######Removing avsc files from HDFS avro path###################

if hadoop fs -test -d $v_AVRO_HDFS_SCHEMA/$SRC_DB/$SRC_TBL;
then
echo "$(v_TIMESTAMP):INFO: DATA available table $SRC_TBL -$v_AVRO_HDFS_SCHEMA/$SRC_DB/$SRC_TBL "
echo "Removing the old files in HDFS AVRO PATH"
hadoop fs -rm $v_AVRO_HDFS_SCHEMA/$SRC_DB/$SRC_TBL/*
hadoop fs -rmdir $v_AVRO_HDFS_SCHEMA/$SRC_DB/$SRC_TBL
hadoop fs -rm -r $v_AVRO_HDFS_SCHEMA/$SRC_DB
echo "$(v_TIMESTAMP):INFO: STANDARDISED table path available -$v_AVRO_HDFS_SCHEMA/$SRC_DB/$SRC_TBL "
fi

###################SQOOP IMPORT##################

        ####ALERT !!!
        ## For proxy ids, dont use logmech=LDAP - ,logmech=LDAP
        sqoop import \
        -Dhadoop.security.credential.provider.path=$CREDENTIAL_PROVIDER_PATH_TDCH \
        -libjars ${v_SQOOP_LIB_JARS}  \
        --driver com.teradata.jdbc.TeraDriver \
        --connect jdbc:$v_TERADATA_TDCH_URL/database=$SRC_DB,logmech=td2,tmode=ANSI,charset=UTF8 \
        --username $v_TERADATA_USERID \
        -password-alias $v_SQOOP_PWD_ALIAS \
        --query "select $QUERY_SAMPLE from $SRC_DB.$SRC_TBL where \$CONDITIONS sample 1"  \
        --as-avrodatafile \
        --m 1 \
        --target-dir $v_AVRO_HDFS_SCHEMA/$SRC_DB/$SRC_TBL/

                        if [ $? -eq 0 ];then
                                                mkdir -p $v_AVRO_LOCAL_SCHEMA/$SRC_DB/$SRC_TBL/
                                                touch $v_AVRO_LOCAL_SCHEMA/$SRC_DB/$SRC_TBL/${SRC_TBL}.avsc
                                                hadoop jar ${v_AVRO_TOOLS_JAR} getschema $v_AVRO_HDFS_SCHEMA/$SRC_DB/$SRC_TBL/part-m-00000.avro> $v_AVRO_LOCAL_SCHEMA/$SRC_DB/$SRC_TBL/${SRC_TBL}.avsc
                                                v_RC_CHECK $? "GENERATE SCHEMA - 'hadoop jar ${v_AVRO_TOOLS_JAR} getschema $v_AVRO_HDFS_SCHEMA/$SRC_DB/$SRC_TBL/part-m-00000.avro> $v_AVRO_LOCAL_SCHEMA/$SRC_DB/$SRC_TBL/${SRC_TBL}.avsc'"

                                                                hadoop fs -put $v_AVRO_LOCAL_SCHEMA/$SRC_DB/$SRC_TBL/${SRC_TBL}.avsc $v_AVRO_HDFS_SCHEMA/$SRC_DB/$SRC_TBL/
                                hadoop fs -chmod 777 $v_AVRO_HDFS_SCHEMA/$SRC_DB/$SRC_TBL/*
                                hadoop fs -chmod 777 $v_AVRO_HDFS_SCHEMA/$SRC_DB
                                hadoop fs -chmod 777 $v_AVRO_HDFS_SCHEMA/$SRC_DB/*
                                                                exit $v_SUCCESS

                        else
                                        echo "$(v_TIMESTAMP):INFO: Sqoop export failed with RC $? "
                                        exit $v_ERROR
                        fi
