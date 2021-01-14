
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


hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.2.jar

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/application_dev.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/query_cogxTeradataUMHistory.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
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
--class com.am.cogx.etl.Driver.CogxDriver \
/data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.2.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxTeradataUMHistory
-----------------------------------------------------

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
--class com.am.cogx.etl.Driver.CogxDriver \
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
--class com.am.cogx.etl.Driver.CogxDriver \
/data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.2.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxTeradataUMHistory
-----------------------------------------------------
==================================================================================


Step 8 - Run simple on cluster mode - jar on HDFS
------------------------------------------------------------------------------
hadoop fs -mkdir /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test

hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/application_dev.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/query_cogxTeradataUMHistory.properties


hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.2.jar

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/application_dev.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/query_cogxTeradataUMHistory.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/hbase-site.xml  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/


hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.2.jar   /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/
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
--class com.am.cogx.etl.Driver.CogxDriver \
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
--class com.am.cogx.etl.Driver.CogxDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.2.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxTeradataUMHistory
-----------------------------------------------------
yarn application -kill application_1558649704445_14007 

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
c
scan "ts_hb_ccpcogx_gbd_r000_in:um_auth"
scan 'dv_hb_ccpcogx_gbd_r000_in:um_auth' , {COLUMNS => 'um:jsonData' , FILTER => "ValueFilter (=, 'substring:\"rfrnc_nbr\"\:\"82876996\"')"}
scan 'ts_hb_ccpcogx_gbd_r000_in:um_auth' 
scan 'dv_hb_ccpcogx_gbd_r000_in:um_auth' , FILTER=>"SingleColumnValueFilter ('','', =, 'regexstring:.*123186519*'  )"



scan 'dv_hb_ccpcogx_gbd_r000_in:um_auth' , { FILTER => " (TimestampsFilter ( 1556848066000, 1557848066000))"}

import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
scan 'dv_hb_ccpcogx_nogbd_r000_in:um_auth'
count  'dv_hb_ccpcogx_nogbd_r000_in:um_auth' 
scan 'dv_hb_ccpcogx_nogbd_r000_in:um_auth', {FILTER => org.apache.hadoop.hbase.filter.RowFilter.new(CompareFilter::CompareOp.valueOf('EQUAL'),SubstringComparator.new("word_by_which_you_want_to_search"))} 


Final working spark submit:

spark2-submit --master yarn --queue ndo_coca_yarn --deploy-mode cluster --executor-memory 40G --executor-cores 2 --driver-cores 2 --driver-memory 30G --name "COGX HBase ETL: cogx_um_load - by ag21866" --files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml --principal AG21866 --keytab /home/ag21866/ag21866.keytab --conf spark.sql.codegen.wholeStage=true --conf spark.yarn.maxAppAttempts=1 --conf spark.driver.memoryOverhead=5120 --conf spark.sql.parquet.cacheMetadata=false --conf spark.yarn.executor.memoryOverhead=16384 --conf spark.network.timeout=420000 --conf "spark.driver.maxResultSize=15g" --conf "spark.default.parallelism=1000" --conf spark.kryoserializer.buffer.max=1024m --conf spark.rpc.message.maxSize=1024 --conf spark.sql.broadcastTimeout=5800 --conf spark.executor.heartbeatInterval=30s --conf spark.dynamicAllocation.executorIdleTimeout=90 --conf spark.dynamicAllocation.initialExecutors=0 --conf spark.dynamicAllocation.maxExecutors=100 --conf spark.dynamicAllocation.minExecutors=30 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.sql.cbo.enabled=true --conf "spark.yarn.security.tokens.hbase.enabled=true" --conf "spark.sql.shuffle.partitions=4200" --files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml --jars /opt/cloudera/parcels/CDH/jars/hive-contrib-1.1.0-cdh5.12.2.jar --jars /usr/lib/tdch/1.5/lib/terajdbc4.jar,/usr/lib/tdch/1.5/lib/tdgssconfig.jar --class com.am.cogx.etl.HiveUM.CogxHiveUMDriver hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag21866Test/ds-cogx-padp-etl-2.0.0.jar hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag21866Test dev cogxHiveUMHistory

