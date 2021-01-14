#!/bin/sh
#Load the bash profile/ .profile/
source $HOME/.bash_profile
source $HOME/.profile

#Defaulting the profile variables for non-service ids
if [ "$(whoami)" != "srcpdppndobthpr" ]; then
	SCRIPTS_DIR="/ts/app/vs2/pdp/pndo/phi/no_gbd/r000/bin/scripts"
	HDFS_DIR="/ts/hdfsapp/vs2/pdp/pndo/phi/no_gbd/r000/bin"
fi

#Load generic functions/Variables
if [ -f $SCRIPTS_DIR/application_shell.properties ];
then
	source $SCRIPTS_DIR/application_shell.properties
else
	echo "ERROR - application_shell.properties not available in $SCRIPTS_DIR"
	exit $v_ERROR
fi



/bin/sh $SCRIPTS_DIR/ndo_sql_load.sh $SCRIPTS_DIR/ehoppa_base.properties
if [ $? -ne 0 ]; then
  echo "$(v_TIMESTAMP):ERROR:$SCRIPTS_DIR/ehoppa_base.properties sqoop load failed"
  echo "$(v_TIMESTAMP):ERROR:Clearing Residual loads - $SCRIPTS_DIR/ehoppa_base.properties"
  source $SCRIPTS_DIR/ehoppa_base.properties
  sqoop eval -Dhadoop.security.credential.provider.path=$CREDENTIAL_PROVIDER_PATH --connect "$CONNECTION_STRING" -username $DBUSERNAME --password-file $v_PASS_FILE --query "$DEL_QUERY"
  exit $v_ERROR
else 
	  /bin/sh $SCRIPTS_DIR/ndo_sql_load.sh $SCRIPTS_DIR/ehoppa_base_mbr.properties
	  if [ $? -ne 0 ]; then
	  		echo "$(v_TIMESTAMP):ERROR: $SCRIPTS_DIR/ehoppa_base_mbr.properties sqoop load failed"
			echo "$(v_TIMESTAMP):ERROR: Clearing Residual loads -1 - $SCRIPTS_DIR/ehoppa_base.properties"
			source $SCRIPTS_DIR/ehoppa_base.properties
			sqoop eval -Dhadoop.security.credential.provider.path=$CREDENTIAL_PROVIDER_PATH --connect "$CONNECTION_STRING" -username $DBUSERNAME --password-file $v_PASS_FILE --query "$DEL_QUERY"
			echo "$(v_TIMESTAMP):ERROR: Clearing Residual loads -2 - $SCRIPTS_DIR/ehoppa_base_mbr.properties"
			source $SCRIPTS_DIR/ehoppa_base_mbr.properties
			sqoop eval -Dhadoop.security.credential.provider.path=$CREDENTIAL_PROVIDER_PATH --connect "$CONNECTION_STRING" -username $DBUSERNAME --password-file $v_PASS_FILE --query "$DEL_QUERY"
			exit $v_ERROR
	  else 
			/bin/sh $SCRIPTS_DIR/ndo_sql_load.sh $SCRIPTS_DIR/ehoppa_base_mbr_cnty.properties
			if [ $? -ne 0 ]; then
				echo "$(v_TIMESTAMP):ERROR: $SCRIPTS_DIR/ehoppa_base_mbr_cnty.properties sqoop load failed"
				echo "$(v_TIMESTAMP):ERROR: Clearing Residual loads -1 - $SCRIPTS_DIR/ehoppa_base.properties"
				source $SCRIPTS_DIR/ehoppa_base.properties
				sqoop eval -Dhadoop.security.credential.provider.path=$CREDENTIAL_PROVIDER_PATH --connect "$CONNECTION_STRING" -username $DBUSERNAME --password-file $v_PASS_FILE --query "$DEL_QUERY"
				echo "$(v_TIMESTAMP):ERROR: Clearing Residual loads -2 - $SCRIPTS_DIR/ehoppa_base_mbr.properties"
				source $SCRIPTS_DIR/ehoppa_base_mbr.properties
				sqoop eval -Dhadoop.security.credential.provider.path=$CREDENTIAL_PROVIDER_PATH --connect "$CONNECTION_STRING" -username $DBUSERNAME --password-file $v_PASS_FILE --query "$DEL_QUERY"
				echo "$(v_TIMESTAMP):ERROR: Clearing Residual loads -3 - $SCRIPTS_DIR/ehoppa_base_mbr_cnty.properties"
				source $SCRIPTS_DIR/ehoppa_base_mbr_cnty.properties
				sqoop eval -Dhadoop.security.credential.provider.path=$CREDENTIAL_PROVIDER_PATH --connect "$CONNECTION_STRING" -username $DBUSERNAME --password-file $v_PASS_FILE --query "$DEL_QUERY"
				exit $v_ERROR
			else
				exit $v_SUCCESS
			fi 
	  fi
fi

