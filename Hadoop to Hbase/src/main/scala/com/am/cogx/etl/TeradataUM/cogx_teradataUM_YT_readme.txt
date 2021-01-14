
Step 1 - To run the application:
=======================================================================
Right click CogxTeradataUMDriver.scala => 
C:\java\git\repos\cognitiveclaim_dev\ds-cogx-etl-padp\sprint03\ds-cogx-etl-padp\conf local local

Action 1 - Update the Application_local.properties
application_local.properties
-------------------------------------------------------
### on local: need to remove quote to run on local
dbserverurl=jdbc:teradata://DWTEST3COP1.WELLPOINT.COM/database=T89_ETL_VIEWS_ENT,logmech=LDAP,tmode=ANSI,charset=UTF8
-------------------------------------------------------
Action 2 - Update query_local.properties
-------------------------------------------------------
### remove quote to run on Local
hbaseconfigfile=hbase-site.xml
-------------------------------------------------------

Action 3 - Update CogxSpark2Config.scala
-------------------------------------------------------
 /////////// When Run on Local ///////////////
    val spark = SparkSession.builder().appName("FSI").master("local[*]").getOrCreate()
-------------------------------------------------------

Action 4 - put the real password on CogxTemplateOperation.scala
-------------------------------------------------------
        //// Test on Local - Action 2 //////////////   
           cred = "*******"
-------------------------------------------------------

    
Step 2 - To run the Application on Cluster - client mode
=======================================================================

Action 1 - Update the Application_dev.properties
application_dev.properties
-------------------------------------------------------
### on Linux environment: need Double Quote for url
dbserverurl="jdbc:teradata://DWTEST3COP1.WELLPOINT.COM/database=T89_ETL_VIEWS_ENT_XM,logmech=LDAP,tmode=ANSI,charset=UTF8"
-------------------------------------------------------
Action 2 - Update query_cogxTeradataUMHistory.properties
-------------------------------------------------------
### On Linux need Double Quote
hbaseconfigfile="hbase-site.xml"
-------------------------------------------------------

Action 3 - put the real password on CogxTemplateOperation.scala
-------------------------------------------------------
        //// Test on Local - Action 2 //////////////   
           cred = "*******"
-------------------------------------------------------

Action 4: Turn on CogxSpark2COnfig
/////////////////// When Run on Cluster /////////////////////
 val spark = SparkSession
    .builder()
   // .config("spark.sql.warehouse.dir", warehouseLocation)
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("hive.warehouse.data.skipTrash", "true")
    .config("spark.sql.parquet.writeLegacyFormat", "true")

    .enableHiveSupport()
    .getOrCreate()
    
  spark.conf.set("spark.sql.shuffle.partitions", "200")
  spark.conf.set("spark.sql.avro.compression.codec", "snappy")
  spark.conf.set("spark.kryo.referenceTracking",	"false")
  spark.conf.set("hive.exec.dynamic.partition", "true")
  spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  spark.conf.set("spark.sql.parquet.filterPushdown", "true")
  spark.conf.set("spark.driver.maxResultSize", "5G")
  spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
  spark.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")

/////////////////// End of When Run on Cluster /////////////////////
Action 5 - Maven build install

Action 6: FTP the following:
	C:\java\git\repos\cognitiveclaim_dev\ds-cogx-etl-padp\TeradataUMSprint03\ds-cogx-etl-padp\target\ds-cogx-padp-etl-2.0.2.jar
		To:
	/data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.2.jar
	
	C:\java\git\repos\cognitiveclaim_dev\ds-cogx-etl-padp\TeradataUMSprint03\ds-cogx-etl-padp\conf\application_dev.properties
	C:\java\git\repos\cognitiveclaim_dev\ds-cogx-etl-padp\TeradataUMSprint03\ds-cogx-etl-padp\conf\query_cogxTeradataUMHistory.properties
		To:
	/data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control
	
	
Action 7: Put to hfds 
hadoop fs -mkdir /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test

hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/application_dev.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/query_cogxTeradataUMHistory.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/query_cogxTeradataUMIncremental.properties

hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.2.jar

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/application_dev.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/query_cogxTeradataUMHistory.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/query_cogxTeradataUMIncremental.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/hbase-site.xml  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/


hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.2.jar   /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/


Step 3 - Create the correct JCEKS
----------------------------------------------------
cd /home/ag29035
mkdir jcekstore
cd jcekstore
/home/ag29035/jcekstore
hadoop fs -ls /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/jcekstore/
	- The jceks already exist and working = This is for teradata
	
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/jcekstore/ag29035Cogx.jceks
hadoop credential create ag29035CogxPass -provider jceks://hdfs/dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/jcekstore/ag29035Cogx.jceks
hadoop fs -ls /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/jcekstore/



hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/jcekstore/af35352Cogx.jceks

hadoop credential create af35352CogxPass -provider jceks://hdfs/dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/jcekstore/af35352Cogx.jceks

Build on SIT:
====================================================================
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/af35352Cogx.jceks
hadoop credential create af35352CogxPass -provider jceks://hdfs/ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/af35352Cogx.jceks


hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/application_sit.properties
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_SasiTeradata.properties




hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/application_sit.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_SasiTeradata.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/


----------------------------------------------------
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
--name "COGX HBase ETL: cogx_UM_Teradata - by AF35352" \
--class com.am.cogx.etl.TeradataUM.CogxTeradataUMDriver \
hdfs:///ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000//bin/ds-cogx-etl-2.0.9.jar   \
hdfs:///ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control sit SasiTeradata

-----------------------------------------

https://dwbdtest1r1m.wp.com:8090/proxy/application_1563939707779_8182/
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1563939707779_8182/

----------------------------------------------------

Step 4 - Create Keytab
---------------------------------------------
cd 
mkdir keytabstore
chmod 755 keytabstore
cd keytabstore
------------------------------------------
ktutil
addent -password -p ag29035@DEVAD.WELLPOINT.COM -k 1 -e rc4-hmac
 - Password for ag29035@DEVAD.WELLPOINT.COM: Enter Password for your Domain ID
addent -password -p ag29035@DEVAD.WELLPOINT.COM -k 1 -e aes256-cts
 - Password for ag29035@DEVAD.WELLPOINT.COM: Enter Password for your Domain ID
ktutil:  wkt ag29035.keytab
ktutil:  quit
kinit ag29035@DEVAD.WELLPOINT.COM -k -t ag29035.keytab
==========================================================


Step 5 - Create the Audit table
=========================
CREATE  TABLE IF NOT EXISTS dv_ccpcogxph_nogbd_r000_wh.cogx_um_teradata_hb_audit (
program String, 
user_id String, 
app_id String, 
start_time String, 
app_duration String, 
status String, 
loaded_row_count BIGINT,
LastUpdate timestamp)

stored as parquet
location  'hdfs:///dv/hdfsdata/ve2/ccp/cogx/phi/no_gbd/r000/warehouse/cogx_um_teradata_hb_audit';
==========================

beeline -u "jdbc:hive2://dwbdtest1hs2lb.wp.com:10000/default;principal=hive/_HOST@DEVAD.WELLPOINT.COM;ssl=true" 
select * from dv_ccpcogxph_nogbd_r000_wh.cogx_um_teradata_hb_audit

================================
************* Incremental solution **************
====================================

beeline -u "jdbc:hive2://dwbdtest1hs2lb.wp.com:10000/default;principal=hive/_HOST@DEVAD.WELLPOINT.COM;ssl=true" 

select * from dv_ccpcogxph_nogbd_r000_wh.cogx_um_teradata_hb_audit where program='COGX HBase ETL: cogx_um_load - by AF35352';

select max(CAST(date_format(to_date(lastupdate),'yyyyMMdd') as INT)) from dv_ccpcogxph_nogbd_r000_wh.cogx_um_teradata_hb_audit where program='COGX HBase ETL: cogx_um_load - by AF35352' and status ='completed' ;

insert into dv_ccpcogxph_nogbd_r000_wh.cogx_um_teradata_hb_audit values ('COGX HBase ETL: cogx_um_load - by AF35352', 'AF35352', 'local-1559749206160', '2019-04-01 09:40:09', '151 Seconds', 'completed', 0, '2019-04-01 09:40:09' )   ;

truncate dv_ccpcogxph_nogbd_r000_wh.cogx_um_teradata_hb_audit;

=======================================================

Step 5 - Run simple on local mode

-------------------------------------------------
spark2-submit \
--master local[*] \
--name cogx_Test_client \
--principal AF35352 \
--keytab /home/af35352/keytabstore/AF35352.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by AF35352" \
--class com.am.cogx.etl.TeradataUM.CogxTeradataUMDriver \
/data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.2.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxTeradataUMHistory
-----------------------------------------------------

------------------------------------------------
spark2-submit \
--master local[*] \
--name cogx_Test_client \
--principal AF35352 \
--keytab /home/af35352/keytabstore/AF35352.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by AF35352" \
--class com.am.cogx.etl.TeradataUM.CogxTeradataUMDriver \
/data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.2.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxTeradataUMIncremental


Step 6 - Run simple on client mode

-------------------------------------------------
spark2-submit \
--master yarn \
--deploy-mode client \
--name cogx_Test_client \
--principal AF35352 \
--keytab /home/af35352/keytabstore/AF35352.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by AF35352" \
--class com.am.cogx.etl.TeradataUM.CogxTeradataUMDriver \
/data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.2.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxTeradataUMHistory
-----------------------------------------------------



Step 7 - Run simple on cluster mode

-------------------------------------------------
spark2-submit \
--master yarn \
--deploy-mode cluster \
--name cogx_Test_client \
--principal AF35352 \
--keytab /home/af35352/keytabstore/AF35352.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by AF35352" \
--class com.am.cogx.etl.TeradataUM.CogxTeradataUMDriver \
/data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.2.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxTeradataUMHistory
-----------------------------------------------------
==================================================================================


Step 8 - Run simple on cluster mode - jar on HDFS
------------------------------------------------------------------------------
hadoop fs -mkdir /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test

hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/application_dev.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/query_cogxTeradataUMHistory.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/query_cogxTeradataUMIncremental.properties

hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.2.jar
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.3.jar

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/application_dev.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/query_cogxTeradataUMHistory.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/query_cogxTeradataUMIncremental.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/hbase-site.xml  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/


hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.2.jar   /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.3.jar   /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/

-------------------------------------------------
spark2-submit \
--master yarn \
--deploy-mode cluster \
--name cogx_Test_client \
--principal AF35352 \
--keytab /home/af35352/keytabstore/AF35352.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by AF35352" \
--class com.am.cogx.etl.TeradataUM.CogxTeradataUMDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.2.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxTeradataUMHistory
-----------------------------------------------------

-------------------------------------------------
spark2-submit \
--master yarn \
--deploy-mode cluster \
--name cogx_Test_client \
--driver-cores 5 \
--driver-memory 8G \
--num-executors 50 \
--executor-memory 20G \
--executor-cores 4 \
--queue ndo_coca_yarn
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
--conf spark.maxRemoteBlockSizeFetchToMem=1G \
--principal AF35352 \
--keytab /home/af35352/keytabstore/AF35352.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by AF35352" \
--class com.am.cogx.etl.TeradataUM.CogxTeradataUMDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.2.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxTeradataUMHistory
-----------------------------------------------------

spark2-submit \
--master yarn \
--deploy-mode cluster \
--name cogx_Test_client \
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
--principal AF35352 \
--keytab /home/af35352/keytabstore/AF35352.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by AF35352" \
--class com.am.cogx.etl.TeradataUM.CogxTeradataUMDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.5.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxTeradataUMHistory

--------------------------------------------------------

-----------------------------------------------------

spark2-submit \
--master yarn \
--deploy-mode cluster \
--name cogx_Test_client \
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
--principal AF35352 \
--keytab /home/af35352/keytabstore/AF35352.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by AF35352" \
--class com.am.cogx.etl.TeradataUM.CogxTeradataUMDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.5.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxTeradataUMIncremental

---------------------------------------------------------

yarn application -kill application_1558649704445_14007 
yarn application -kill application_1558649704445_23641
yarn application -kill application_1558649704445_24622
yarn application -kill application_1558649704445_24525
Spark UI:
https://dwbdtest1r2m.wp.com:8090/proxy/application_1558649704445_24525/executors/
https://dwbdtest1r2m.wp.com:8090/proxy/application_1558649704445_24622/executors/
https://dwbdtest1r2m.wp.com:8090/proxy/application_1558649704445_24642/
https://dwbdtest1r2m.wp.com:8090/proxy/application_1558649704445_25186
https://dwbdtest1r2m.wp.com:8090/proxy/application_1558649704445_25448/
https://dwbdtest1r2m.wp.com:8090/proxy/application_1560313293965_4419


Hadoop history
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1558649704445_24525
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1558649704445_24622
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1558649704445_24642
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1558649704445_25186

https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1558649704445_25432 

https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1558649704445_25562

yarn application -kill  application_1558649704445_24432

Total counts: 144987940

==================================================================================

***************** Trouble shooting Notes Below **********************************
===================================================================================

Trouble shooting link:
Hadoop History
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1558649704445_14007 
Hue Job browser
https://dwbdtest1r1e.wp.com:8890/hue/jobbrowser


Verify HBase:

scan "ts_hb_ccpcogx_gbd_r000_in:um_auth"
scan 'dv_hb_ccpcogx_gbd_r000_in:um_auth' , {COLUMNS => 'um:jsonData' , FILTER => "ValueFilter (=, 'substring:\"rfrnc_nbr\"\:\"82876996\"')"}
scan 'ts_hb_ccpcogx_gbd_r000_in:um_auth' 
scan 'dv_hb_ccpcogx_gbd_r000_in:um_auth' , FILTER=>"SingleColumnValueFilter ('','', =, 'regexstring:.*123186519*'  )"


dv_hb_ccpcogx_gbd_r000_in:cogx_claims   

count 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims_test'
count dv_hb_ccpcogx_nogbd_r000_in:cogx_claims_test   

cogx_benefits    


scan 'dv_hb_ccpcogx_gbd_r000_in:um_auth' , { FILTER => " (TimestampsFilter ( 1556848066000, 1557848066000))"}

import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
scan 'dv_hb_ccpcogx_nogbd_r000_in:um_auth'
count  'dv_hb_ccpcogx_nogbd_r000_in:um_auth' 
scan 'dv_hb_ccpcogx_nogbd_r000_in:um_auth', {FILTER => org.apache.hadoop.hbase.filter.RowFilter.new(CompareFilter::CompareOp.valueOf('EQUAL'),SubstringComparator.new("word_by_which_you_want_to_search"))} 

truncate table  'dv_hb_ccpcogx_nogbd_r000_in:um_auth' 


==============================================================================
***** Manual Testing on HBase Connection From Spark ******************
=================================================================================
Step 1 - Start the Spark Shell
spark2-shell \
--principal AF35352 \
--keytab /home/af35352/keytabstore/AF35352.keytab  \
--jars hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.5.jar 

Step 2 - Create a simple DataFrame:
import java.io.File
import java.util.UUID

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{current_timestamp, lit}

sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
val sqlContext = new SQLContext(sc)

:paste

var uuid: String = null;
var newData1 = List(("",""));
for (i<-1L to 5L)
{
  uuid = UUID.randomUUID().toString()
  newData1 = newData1:::List((uuid,"value" + uuid))
}
newData1=newData1.drop(1)
val df3 = sqlContext.createDataFrame(sc.parallelize(newData1, 5).map(x => (x._1,x._2))).toDF("rowKey", "jsonData")
df3.show()
*** Control D *****


val columnFamily = "um"
val columnName = "jsonData"


import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
:paste

    val putRDD = df3.rdd.map(x => {
          val rowKey = x.getAs[String]("rowKey")
          val holder = x.getAs[String]("jsonData")
          print(rowKey, holder)
          val p = new Put(Bytes.toBytes(rowKey))
          p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(holder))
          (new org.apache.hadoop.hbase.io.ImmutableBytesWritable, p)
        }
    )

*********** control D ***************

import com.google.gson.GsonBuilder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectData
import org.apache.avro.reflect.ReflectDatumWriter
import java.io.ByteArrayOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapreduce.{Job, JobStatus}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableOutputFormat}
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.hadoop.fs.{FSDataInputStream, Path}
val hdfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)


:paste

  def getMapReduceJobConfiguration():Configuration = {
      val job = Job.getInstance(HBaseConfiguration.create(), "HDFS-to-HBase ETL")
      job.setOutputFormatClass(new org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable].getClass)
     var confIT = job.getConfiguration().iterator()
     println ("*********************************************************")
     while (confIT.hasNext())
     {
       var it = confIT.next()
       if (it.getKey.toString().contains("hbase"))
       {
       
       println("Key: "+ it.getKey())
       println("Value: " + it.getValue())
       }
     }
     println ("*********************************************************")
     
      job.getConfiguration().addResource( (hdfs.open(new Path("hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/hbase-site.xml")) ) )    
      //job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,tableName)
      job.getConfiguration
  }
  
val config = getMapReduceJobConfiguration()
config.set(TableOutputFormat.OUTPUT_TABLE,"dv_hb_ccpcogx_gbd_r000_in:um_auth")
HBaseAdmin.checkHBaseAvailable(config)
  
new PairRDDFunctions(putRDD).saveAsNewAPIHadoopDataset(config)

=============================================================



