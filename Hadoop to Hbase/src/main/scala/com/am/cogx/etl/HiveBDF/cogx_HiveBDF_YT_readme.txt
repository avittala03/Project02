Note: we do not have Local run on BDF
- BDF table was copied to 




Step 4 - Create Keytab
---------------------------------------------
cd 
mkdir keytabstore
chmod 755 keytabstore
cd keytabstore
------------------------------------------


ktutil
addent -password -p srcccpcogxbthdv@DEVAD.WELLPOINT.COM -k 1 -e rc4-hmac
 - Password for srcccpcogxbthdv@DEVAD.WELLPOINT.COM: Enter Password for your Domain ID
addent -password -p srcccpcogxbthdv@DEVAD.WELLPOINT.COM -k 1 -e aes256-cts
 - Password for srcccpcogxbthdv@DEVAD.WELLPOINT.COM: Enter Password for your Domain ID
ktutil:  wkt srcccpcogxbthdv.keytab
ktutil:  quit
kinit srcccpcogxbthdv@DEVAD.WELLPOINT.COM -k -t srcccpcogxbthdv.keytab



ktutil
addent -password -p AF35352@DEVAD.WELLPOINT.COM -k 1 -e rc4-hmac
 - Password for AF35352@DEVAD.WELLPOINT.COM: Enter Password for your Domain ID
addent -password -p AF35352@DEVAD.WELLPOINT.COM -k 1 -e aes256-cts
 - Password for AF35352@DEVAD.WELLPOINT.COM: Enter Password for your Domain ID
ktutil:  wkt AF35352.keytab
ktutil:  quit
kinit AF35352@DEVAD.WELLPOINT.COM -k -t AF35352.keytab



========================================================================
***** Test run on cluster Mode ********************
----------------------------------------------------------------------
hadoop fs -mkdir /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test

hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/application_dev.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/query_cogxHiveBDFHistory.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/query_cogxHiveBDFIncremental.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.5.jar


hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalNarrow.properties
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalAll.properties

hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalNarrow.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalAll.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/


cogxHiveBDFIncrementalAll
cogxHiveBDFIncrementalNarrow



hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/application_dev.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/query_cogxHiveBDFHistory.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/query_cogxHiveBDFIncremental.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/ds-cogx-padp-etl-2.0.5.jar   /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/hbase-site.xml  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/

----------------------------------------------------------------------
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
--principal AF35352 \
--keytab /home/af35352/keytabstore/AF35352.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by AF35352" \
--class com.am.cogx.etl.HiveBDF.CogxHiveBDFDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.5.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxHiveBDFHistory

=============================================================================

========================== AF35352 From SIT source to DEV HBase ==================

hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/query_cogxHiveBDFHistoryNarrow.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-etl-2.0.6.jar
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/application_dev.properties

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/query_cogxHiveBDFHistoryNarrow.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/ds-cogx-etl-2.0.6.jar   /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/application_dev.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/

=========================================================

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
--principal AF35352 \
--keytab /home/af35352/keytabstore/AF35352.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by AF35352" \
--class com.am.cogx.etl.HiveBDF.CogxHiveBDFDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-etl-2.0.7.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxHiveBDFHistoryNarrow


https://dwbdtest1r1m.wp.com:8090/proxy/application_1562157072409_10160/jobs/
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1562157072409_10160
=============================================================
*************** Use Service Keytab: ********************
----------------------------------------
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/application_sit.properties
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFHistory.properties
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ds-cogx-etl-2.0.6.jar

hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalNarrow.properties
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalOriginal.properties

hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalNarrow.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalOriginal.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/


cogxHiveBDFIncrementalAll
cogxHiveBDFIncrementalNarrow

hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/application_sit.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFHistory.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/ds-cogx-etl-2.0.6.jar   /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000//bin/
hadoop fs -chmod 771 /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/*


hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/hbase-site.xml /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/

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
--name "COGX HBase ETL: cogx_BDF_PartialLoad - by AF35352" \
--class com.am.cogx.etl.HiveBDF.CogxHiveBDFDriver \
hdfs:///ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000//bin/ds-cogx-etl-2.0.6.jar   \
hdfs:///ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control sit cogxHiveBDFHistory


----------------------------- old & new ------------------------
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
--name "COGX HBase ETL: cogx_BDF_Old_load - by AF35352" \
--class com.am.cogx.etl.HiveBDF.CogxHiveBDFDriver \
hdfs:///ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000//bin/ds-cogx-etl-2.0.7.jar   \
hdfs:///ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control sit cogxHiveBDFIncrementalNarrow

----------------------------------------------------------
-------------------DEV-------------------------
=============================================================
*************** Use Service Keytab: ********************
----------------------------------------
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/application_dev.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFHistory.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ds-cogx-etl-2.0.6.jar

hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalNarrow.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalOriginal.properties

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalNarrow.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalOriginal.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/


cogxHiveBDFIncrementalAll
cogxHiveBDFIncrementalNarrow

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/application_dev.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFHistory.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/ds-cogx-etl-2.0.6.jar   /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000//bin/
hadoop fs -chmod 771 /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/*


hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/hbase-site.xml /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/

-------------------DEV-------------------------

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
--principal srcccpcogxbthdv \
--keytab /home/srcccpcogxbthdv/keytabstore/srcccpcogxbthdv.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_BDF_PartialLoad - by AF35352" \
--class com.am.cogx.etl.HiveBDF.CogxHiveBDFDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000//bin/ds-cogx-etl-2.0.6.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control sit cogxHiveBDFHistory


-----------------------------------------------------------------------
Loading to HBase count

cogxHiveBDFIncrementalAll
cogxHiveBDFIncrementalNarrow


https://dwbdtest1r1m.wp.com:8090/proxy/application_1562824131263_2447
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1562824131263_1713

https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1562157072409_10146/

https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1562157072409_9108

application_1562157072409_9108
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1562157072409_9108

https://dwbdtest1r2m.wp.com:8090/proxy/application_1562794595640_0068
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1562794595640_0068

https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1562157072409_7433

https://dwbdtest1r2m.wp.com:8090/proxy/application_1562000135734_0317/
https://dwbdtest1r2m.wp.com:8090/proxy/application_1562000135734_0319/
https://dwbdtest1r2m.wp.com:8090/proxy/application_1562000135734_0324/
--------------------------

count 'ts_hb_ccpcogx_nogbd_r000_in:cogx_claims'
truncate 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims'

create 'ts_hb_ccpcogx_nogbd_r000_in:cogx_claims_new', 'c1'


get 'ts_hb_ccpcogx_nogbd_r000_in:cogx_claims','0ee77aad342482446302018071020180710'

========================================================================
Run Incremental:

Case 1: Force to run the with default and ignore the Audit table

query_cogxHiveBDFIncremental.properties
------------------------
backout_months=36
# Incremental loading Config
default_incremental_startdt=20180611
isIncremental=yes
force_default_incremental=yes
------------------------
Case 2: use the last load from audit table:
Action 1 - Update the configuration file
query_cogxHiveBDFIncremental.properties
------------------------
backout_months=36
# Incremental loading Config
default_incremental_startdt=20180611
isIncremental=yes
force_default_incremental=no
------------------------
Action 2 - clean up the Audit table and insert new records

beeline -u "jdbc:hive2://dwbdtest1hs2lb.wp.com:10000/default;principal=hive/_HOST@DEVAD.WELLPOINT.COM;ssl=true" 

select * from dv_ccpcogxph_nogbd_r000_wh.cogx_um_teradata_hb_audit where program='COGX HBase ETL: cogx_BDF_load - by AF35352';

select max(CAST(date_format(to_date(lastupdate),'yyyyMMdd') as INT)) from dv_ccpcogxph_nogbd_r000_wh.cogx_um_teradata_hb_audit where program='COGX HBase ETL: cogx_BDF_load - by AF35352' and status ='completed' ;

insert into dv_ccpcogxph_nogbd_r000_wh.cogx_um_teradata_hb_audit values ('COGX HBase ETL: cogx_BDF_load - by AF35352', 'AF35352', 'local-1559749206160', '2018-04-01 09:40:09', '151 Seconds', 'completed', 0, '2018-04-01 09:40:09' )   ;

truncate dv_ccpcogxph_nogbd_r000_wh.cogx_um_teradata_hb_audit;


----------------------------------------------------------------------
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
--principal AF35352 \
--keytab /home/af35352/keytabstore/AF35352.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_BDF_load - by AF35352" \
--class com.am.cogx.etl.HiveBDF.CogxHiveBDFDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-padp-etl-2.0.5.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxHiveBDFIncremental

========================================================================

query_cogxHiveBDFIncremental.properties




create 'ts_hb_ccpcogx_nogbd_r000_in:cogx_claims_old', 'c1'
create 'ts_hb_ccpcogx_nogbd_r000_in:cogx_claims_new', 'c1'


count 'ts_hb_ccpcogx_nogbd_r000_in:cogx_claims_old'

scan 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims' , { FILTER => " (TimestampsFilter ( 1556848066000, 1557848066000))"}
count 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims'
truncate 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims' 
scan 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims' , {STARTROW => 'ffd02124125135820', ENDROW => 'ffd02124125135826'} 

count 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims'
get   'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims',' d99e8286243450060302018-07-062018-07-06' 



scan 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims' , {STARTROW => '00028cf7116038207202018081420180814', ENDROW => '00028cf7116038207202018081420180815'} 

 
scan 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims_test'' , {STARTROW => 'fff7b0ea741874128502018081620180816', ENDROW => 'fff7b0ea741874128502018081620180817'} 


import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
scan 'dv_hb_ccpcogx_nogbd_r000_in:um_auth'
count  'dv_hb_ccpcogx_nogbd_r000_in:um_auth' 
scan 'dv_hb_ccpcogx_nogbd_r000_in:um_auth', {FILTER => org.apache.hadoop.hbase.filter.RowFilter.new(CompareFilter::CompareOp.valueOf('EQUAL'),SubstringComparator.new("word_by_which_you_want_to_search"))} 

truncate  'dv_hb_ccpcogx_nogbd_r000_in:um_auth' 


dv_hb_ccpcogx_gbd_r000_in:cogx_claims 

===================================================

yarn application -kill application_1558649704445_28356

yarn application -kill application_1562157072409_9469


application_1558649704445_28356

Hadoop History
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1558649704445_28372 
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1558649704445_28750
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1558649704445_28957
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1562157072409_9105
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1562157072409_11341

https://dwbdtest1r1m.wp.com:8090/cluster/app//application_1562785983704_0057/



https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1562157072409_9469
=======
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1560313293965_9403 

application_1560313293965_9403

Spark UI:
https://dwbdtest1r2m.wp.com:8090/proxy/application_1558649704445_28372 
https://dwbdtest1r2m.wp.com:8090/proxy/application_1558649704445_28750/
https://dwbdtest1r2m.wp.com:8090/proxy/application_1558649704445_28957/
https://dwbdtest1r2m.wp.com:8090/proxy/application_1558649704445_29147/
https://dwbdtest1r2m.wp.com:8090/proxy/application_1558649704445_31454 


https://dwbdtest1r2m.wp.com:8090/proxy/application_1560313293965_11179
=======
https://dwbdtest1r2m.wp.com:8090/proxy/application_1560313293965_6368



 hadoop fs -ls /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/BDFTestData.csv/part-00000-bbc1b192-552c-4b8e-af00-fe71dae20c27-c000.csv

/data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/teradata/part-00000-bbc1b192-552c-4b8e-af00-fe71dae20c27-c000.csv

Full load on Test
==============================
https://dwbdtest1r2m.wp.com:8090/cluster/app/application_1561983075485_0277 

