
Step 1 - To run the application:
=======================================================================
C:\java\git\repos\cognitiveclaim_dev\ds-cogx-etl-padp\ds-cogx-etl-padp\conf local local

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
dbserverurl="jdbc:teradata://DWTEST3COP1.WELLPOINT.COM/database=T89_ETL_VIEWS_ENT,logmech=LDAP,tmode=ANSI,charset=UTF8"
-------------------------------------------------------
Action 2 - Update query_local.properties
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

Action 5: Turn on the Keytab and jces the CogxOperationSession.scala

     val cred = String.valueOf(conf.getPassword( config.getString("password")))

Action 6: FTP the following:
	C:\java\git\repos\cognitiveclaim_dev\ds-cogx-etl-padp\ds-cogx-etl-padp\target\ds-cogx-padp-etl-2.0.0.jar
		To:
	/data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.0.jar
	
	C:\java\git\repos\cognitiveclaim_dev\ds-cogx-etl-padp\ds-cogx-etl-padp\conf\application_dev.properties
	C:\java\git\repos\cognitiveclaim_dev\ds-cogx-etl-padp\ds-cogx-etl-padp\conf\query_cogx.properties
		To:
	/data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control
	
	
Action 7: Put to hfds 
hadoop fs -mkdir /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test

hadoop fs -rm -r -skipTrash /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/application_dev.properties
hadoop fs -rm -r -skipTrash /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/query_cogx.properties


hadoop fs -rm -r -skipTrash /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ag29035Test/ds-cogx-padp-etl-2.0.0.jar

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/application_dev.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogx.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/hbase-site.xml  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/


hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.0.jar   /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ag29035Test/


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




Step 5 - Run simple on local mode

-------------------------------------------------
spark2-submit \
--master local[*] \
--name cogx_Test_client \
--principal ag29035 \
--keytab /home/ag29035/keytabstore/ag29035.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by ag29035" \
--class com.am.cogx.etl.Driver.CogxDriver \
/data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.0.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/ dev cogx
-----------------------------------------------------

Step 6 - Run simple on client mode

-------------------------------------------------
spark2-submit \
--master yarn \
--deploy-mode client \
--name cogx_Test_client \
--principal ag29035 \
--keytab /home/ag29035/keytabstore/ag29035.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by ag29035" \
--class com.am.cogx.etl.Driver.CogxDriver \
/data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.0.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/ dev cogx
-----------------------------------------------------



Step 7 - Run simple on cluster mode

-------------------------------------------------
spark2-submit \
--master yarn \
--deploy-mode cluster \
--name cogx_Test_client \
--principal ag29035 \
--keytab /home/ag29035/keytabstore/ag29035.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by ag29035" \
--class com.am.cogx.etl.Driver.CogxDriver \
/data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.0.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/ dev cogx
-----------------------------------------------------
==================================================================================


Step 8 - Run simple on cluster mode - jar on HDFS
------------------------------------------------------------------------------
hadoop fs -mkdir /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test

hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/application_dev.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/query_cogx.properties


hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ag29035Test/ds-cogx-padp-etl-2.0.0.jar

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/application_dev.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogx.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/hbase-site.xml  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/


hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.0.jar   /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ag29035Test/
-------------------------------------------------
spark2-submit \
--master yarn \
--deploy-mode cluster \
--driver-cores 4 \
--driver-memory 50G \
--executor-memory 50G \
--executor-cores 4 \
--name cogx_Test_client \
--principal ag29035 \
--keytab /home/ag29035/keytabstore/ag29035.keytab  \
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
--conf spark.sql.shuffle.partitions=2100 \
--jars /opt/cloudera/parcels/CDH/jars/hive-contrib-1.1.0-cdh5.12.2.jar  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by ag29035" \
--class com.am.cogx.etl.Driver.CogxDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ag29035Test/ds-cogx-padp-etl-2.0.0.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/ dev cogx
-----------------------------------------------------
==================================================================================

***************** Trouble shooting Notes Below **********************************
===================================================================================

Trouble shooting link:
Hadoop History
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1557843199409_0132
Hue Job browser
https://dwbdtest1r1e.wp.com:8890/hue/jobbrowser


Verify HBase:
scan "dv_hb_ccpcogx_nogbd_r000_in:um_auth"
scan "ts_hb_ccpcogx_nogbd_r000_in:um_auth"
scan 'dv_hb_ccpcogx_nogbd_r000_in:um_auth' , {COLUMNS => 'um:jsonData' , FILTER => "ValueFilter (=, 'substring:\"rfrnc_nbr\"\:\"82876996\"')"}
scan 'ts_hb_ccpcogx_nogbd_r000_in:um_auth' 
scan 'dv_hb_ccpcogx_nogbd_r000_in:um_auth' , FILTER=>"SingleColumnValueFilter ('','', =, 'regexstring:.*123186519*'  )"




scan 'dv_hb_ccpcogx_no_gbd_r000_in:um_auth' , { FILTER => " (TimestampsFilter ( 1556848066000, 1557848066000))"}


===============================================================================

spark2-submit \
--master yarn \
--deploy-mode client \
--name cogx_Test_client \
--driver-cores 5 \
--driver-memory 8G \
--num-executors 50 \
--executor-memory 20G \
--executor-cores 4 \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml \
--name "COGX HBase ETL: cogx_um_load - by ag29035" \
--principal ag29035 \
--keytab /home/ag29035/keytabstore/ag29035.keytab  \
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
--jars /opt/cloudera/parcels/CDH/jars/hive-contrib-1.1.0-cdh5.12.2.jar  \
--files hdfs:///dv/hdfsapp/ve2/pdp/spfi/phi/no_no_gbd/r000/control/log4j.xml,/etc/spark2/conf.cloudera.spark2_on_yarn/yarn-conf/hive-site.xml \
--driver-java-options "-Dlog4j.configuration=log4j.xml" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.xml" \
--class com.am.cogx.etl.Driver.CogxDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ag29035Test/ds-cogx-padp-etl-2.0.0.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/ dev cogx



spark2-submit \
--master yarn \
--deploy-mode client \
--name cogx_Test_client \
--driver-cores 5 \
--driver-memory 8G \
--num-executors 50 \
--executor-memory 20G \
--executor-cores 4 \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml \
--name "COGX HBase ETL: cogx_um_load - by ag29035" \
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
--conf spark.dynamicAllocation.minExecutors=3 \
--jars /opt/cloudera/parcels/CDH/jars/hive-contrib-1.1.0-cdh5.12.2.jar  \
--files hdfs:///dv/hdfsapp/ve2/pdp/spfi/phi/no_no_gbd/r000/control/log4j.xml,/etc/spark2/conf.cloudera.spark2_on_yarn/yarn-conf/hive-site.xml \
--driver-java-options "-Dlog4j.configuration=log4j.xml" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.xml" \
--class com.am.cogx.etl.Driver.CogxDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ag29035Test/ds-cogx-padp-etl-2.0.0.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/ dev cogx

run everything on local host on Cluster:
---------------------------------------------
spark2-submit \
--master local[*] \
--name cogx_Test_client \
--principal ag29035 \
--keytab /home/ag29035/keytabstore/ag29035.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by ag29035" \
--class com.am.cogx.etl.Driver.CogxDriver \
/data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.0.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/ dev cogx
=================================================================


spark2-submit \
--master yarn \
--deploy-mode cluster \
--name cogx_Test_cluster \
--driver-cores 5 \
--driver-memory 8G \
--num-executors 50 \
--executor-memory 20G \
--executor-cores 4 \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml \
--name "COGX HBase ETL: cogx_um_load - by ag29035" \
--principal ag29035 \
--keytab /home/ag29035/keytabstore/ag29035.keytab  \
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
--jars /opt/cloudera/parcels/CDH/jars/hive-contrib-1.1.0-cdh5.12.2.jar  \
--files hdfs:///dv/hdfsapp/ve2/pdp/spfi/phi/no_no_gbd/r000/control/log4j.xml,/etc/spark2/conf.cloudera.spark2_on_yarn/yarn-conf/hive-site.xml \
--driver-java-options "-Dlog4j.configuration=log4j.xml" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.xml" \
--class com.am.cogx.etl.Driver.CogxDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ag29035Test/ds-cogx-padp-etl-2.0.0.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/ dev cogx




=================================================
***** Trouble shooting HBase ******************
===============================================
Step 1 - Start the Spark Shell
spark2-shell \
--principal ag29035 \
--keytab /home/ag29035/keytabstore/ag29035.keytab  \
--jars hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ag29035Test/ds-cogx-padp-etl-2.0.0.jar   

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
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableOutputFormat}
import org.apache.spark.rdd.PairRDDFunctions

     val conf = HBaseConfiguration.create()
     conf.set("hbase.zookeeper.quorum","dwbdtest1r5e1.wp.com")
	 conf.set("hbase.security.authentication","kerberos")
	 
	 conf.set("hbase.master.kerberos.principal","ag29035@DEVAD.WELLPOINT.COM")
     conf.set("hbase.regionserver.kerberos.principal","ag29035@DEVAD.WELLPOINT.COM")
	 
     conf.set("hbase.master.kerberos.principal","hbase/_HOST@DEVAD.WELLPOINT.COM")
     conf.set("hbase.regionserver.kerberos.principal","hbase/_HOST@DEVAD.WELLPOINT.COM")
     
======================================================

/opt/cloudera/parcels/CDH/lib/hbase/conf

dwbdtest1r2m.wp.com,dwbdtest1r2m3.wp.com,dwbdtest1r1m.wp.com

     spark.sparkContext.hadoopConfiguration.addResource(com.am.cogx.etl.config.CogxSpark2Config.getClass.getClassLoader.getResourceAsStream("hbase-site.xml"))
     
     HBaseAdmin.checkHBaseAvailable(conf)
 	 conf.set(TableOutputFormat.OUTPUT_TABLE,"dv_hb_ccpcogx_no_gbd_r000_in:um_auth")
 
 
 new PairRDDFunctions(putRDD).saveAsNewAPIHadoopDataset(conf)
 
 
 
     
         <name>hbase.security.authentication</name>
    <value>kerberos</value>
        <name>hbase.master.kerberos.principal</name>
    <value>hbase/_HOST@DEVAD.WELLPOINT.COM</value>
  </property>
  <property>
    <name>hbase.regionserver.kerberos.principal</name>
    <value>hbase/_HOST@DEVAD.WELLPOINT.COM</value>
    
     
     conf.set("hbase.zookeeper.quorum","dwbdtest1r2m.wp.com,dwbdtest1r5e1.wp.com,dwbdtest1r1m.wp.com")
     
      job.setOutputFormatClass(new org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable].getClass)
      
      job.getConfiguration().set("hbase.zookeeper.quorum","dwbdtest1r2m.wp.com,dwbdtest1r5e1.wp.com,dwbdtest1r1m.wp.com")
      
      job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,tableName)
      job.getConfiguration



spark2-submit \
--master local[*] \
--name cogx_Test_client \
--principal ag29035 \
--keytab /home/ag29035/keytabstore/ag29035.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by ag29035" \
--class com.am.cogx.etl.Driver.CogxDriver \
/data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.1.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/ dev cogx







