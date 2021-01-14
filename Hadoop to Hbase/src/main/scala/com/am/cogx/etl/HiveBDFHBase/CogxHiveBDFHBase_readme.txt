=============================================================
*************** Use Service Keytab: ********************
----------------------------------------
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/application_sit.properties
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFHbase.properties
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ds-cogx-etl-2.0.7.jar



hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/application_sit.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFHbase.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/ds-cogx-etl-2.0.7.jar   /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000//bin/



-------------------SIT-------------------------

spark2-submit \
--master yarn \
--deploy-mode cluster \
--queue cdl_yarn  \
--driver-cores 5 \
--driver-memory 8G \
--num-executors 50 \
--executor-memory 20G \
--executor-cores 4 \
--conf spark.ui.port=5052 \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.yarn.driver.memoryOverhead=2048 \
--conf spark.yarn.executor.memoryOverhead=9096 \
--conf spark.network.timeout=800 \
--conf spark.driver.maxResultSize=0 \
--conf spark.kryoserializer.buffer.max=1024m \
--conf spark.rpc.message.maxSize=1024 \
--conf spark.sql.broadcastTimeout=4800 \
--conf spark.executor.heartbeatInterval=30s \
--conf spark.dynamicAllocation.executorIdleTimeout=180 \
--conf spark.dynamicAllocation.initialExecutors=30 \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.dynamicAllocation.minExecutors=30 \
--conf spark.sql.shuffle.partitions=2200 \
--conf hive.exec.dynamic.partition=true  \
--conf hive.exec.max.dynamic.partitions=10000  \
--principal srcccpcogxbthts \
--keytab /home/srcccpcogxbthts/srcccpcogxbthts.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_BDF_HBase_load - by AF35352" \
--class com.am.cogx.etl.HiveBDFHBase.CogxHiveBDFHBaseDriver \
hdfs:///ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000//bin/ds-cogx-etl-2.0.7.jar   \
hdfs:///ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control sit cogxHiveBDFHbase

----------------------------------------------------------