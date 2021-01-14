

========================================================================
***** Test run on cluster Mode ********************
----------------------------------------------------------------------
hadoop fs -mkdir /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test

hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/application_dev.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/query_cogxHiveBDFSync.properties
hadoop fs -rm /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-etl-2.0.6.jar

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/application_dev.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/query_cogxHiveBDFSync.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/bin/etlHbase/ds-cogx-etl-2.0.6.jar   /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/gbd/r000/control/hbase-site.xml  /dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/

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
--conf hive.exec.dynamic.partition=true  \
--conf hive.exec.max.dynamic.partitions=10000  \
--principal AF35352 \
--keytab /home/af35352/keytabstore/AF35352.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by AF35352" \
--class com.am.cogx.etl.HiveBDFSync.CogxHiveBDFSyncDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/bin/af35352Test/ds-cogx-etl-2.0.6.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/ dev cogxHiveBDFSync




          .config("hive.exec.dynamic.partition", "true")
          .config("hive.exec.max.dynamic.partitions", "5000)
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .config("parquet.compression", "SNAPPY")
          .config("hive.exec.max.dynamic.partitions", "5000)
                   hive.exec.max.dynamic.partitions
          
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1561577230255_3006

application_1561577230255_5212

https://dwbdtest1r2m.wp.com:8090/proxy/application_1561577230255_5822 
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1562157072409_9914
=================================================================


hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/application_sit.properties
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFSyncSITAll.properties
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFSyncSITIncremental.properties
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ds-cogx-etl-2.0.6.jar

hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/application_sit.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFSyncSITAll.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFSyncSITIncremental.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/ds-cogx-etl-2.0.6.jar   /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000//bin/
hadoop fs -chmod 771 /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/*


hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/hbase-site.xml /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/

hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ds-cogx-etl-2.0.7.jar
hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/bin/ds-cogx-etl-2.0.7.jar   /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000//bin/





=============================================================
Use Service Keytab:
----------------------------------------

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
--name "COGX HBase ETL: cogx_BDF_Sync_LoadAll - by AF35352" \
--class com.am.cogx.etl.HiveBDFSync.CogxHiveBDFSyncDriver \
hdfs:///ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000//bin/ds-cogx-etl-2.0.6.jar   \
hdfs:///ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control sit cogxHiveBDFSyncSITIncremental



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
--name "COGX HBase ETL: cogx_BDF_Sync_Load_All - by AF35352" \
--class com.am.cogx.etl.HiveBDFSync.CogxHiveBDFSyncDriver \
hdfs:///ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000//bin/ds-cogx-etl-2.0.7.jar   \
hdfs:///ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control sit cogxHiveBDFSyncSITAll

cogxHiveBDFSyncSITAll
cogxHiveBDFSyncSITIncremental
----------------------------


hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalNarrow.properties
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalAll.properties

hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalNarrow.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/
hadoop fs -put  /data/01/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/control/query_cogxHiveBDFIncrementalAll.properties  /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/


cogxHiveBDFIncrementalAll
cogxHiveBDFIncrementalNarrow




=================================================================
/ts/app/ve2/ccp/cogx/phi/no_gbd/r000/bin

https://dwbdtest1r2m.wp.com:8090/proxy/application_1562824131263_2284
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1561577230255_5779
https://dwbdtest1r2m.wp.com:8090/proxy/application_1562157072409_7279 \
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1562824131263_2193/
https://dwbdtest1r2m.wp.com:8090/proxy/application_1562157072409_9469

https://dwbdtest1r2m.wp.com:8090/proxy/application_1562824131263_2193/



https://dwbdtest1r2m.wp.com:8090/proxy/application_1562157072409_7423
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1562157072409_7423

https://dwbdtest1r2m.wp.com:8090/proxy/application_1562157072409_7433 
application_1562157072409_7433 

https://dwbdtest1r2m.wp.com:8090/proxy/application_1561577230255_6018
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1561577230255_6018


https://dwbdtest1r2m.wp.com:8090/proxy/application_1561577230255_6471 
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1561577230255_6471 

https://dwbdtest1r2m.wp.com:8090/proxy/application_1561577230255_6475 
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1561577230255_6475 


https://dwbdtest1r2m.wp.com:8090/proxy/application_1561577230255_6606
https://dwbdtest1r1m.wp.com:8090/cluster/app/application_1561577230255_6606


https://dwbdtest1r1m.wp.com:8090/proxy/application    



https://dwbdtest1r2m.wp.com:8090/proxy/application_1562157072409_9469
application_1562157072409_9469                                                                                                                                                          _1562157072409_7415/

=================================================================================

auditing table check 
select * from ts_ccpcogxph_nogbd_r000_wh.cogx_etl_audit;


================================================
beeline -u "jdbc:hive2://dwbdtest1hs2lb.wp.com:10000/default;principal=hive/_HOST@DEVAD.WELLPOINT.COM;ssl=true" 
select count (*) from ts_ccpcogxph_nogbd_r000_sg.clm_wgs_gncclmp_cogx;
select count (*) from ts_ccpcogxph_nogbd_r000_sg.clm_wgs_gncdtlp_cogx;
select count (*) from ts_ccpcogxph_nogbd_r000_sg.clm_wgs_gncnatp_ea2_cogx;



NarrowHeaderDB="ts_ccpcogxph_nogbd_r000_sg.clm_wgs_gncclmp_cogx"
NarrowDetailDB="ts_ccpcogxph_nogbd_r000_sg.clm_wgs_gncdtlp_cogx"
NarrowEA2DB="ts_ccpcogxph_nogbd_r000_sg.clm_wgs_gncnatp_ea2_cogx"

yarn application -kill application_1561577230255_6471
hadoop fs -rm /ts/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/application_dev.properties




cogx_claims_part1




scan 'ts_hb_ccpcogx_nogbd_r000_in:cogx_claims_part1' , { FILTER => " (TimestampsFilter ( 1556848066000, 1557848066000))"}
count 'ts_hb_ccpcogx_nogbd_r000_in:cogx_claims_part1'

create 'ts_hb_ccpcogx_nogbd_r000_in:cogx_claims_old', 'c1'
create 'ts_hb_ccpcogx_nogbd_r000_in:cogx_claims_new', 'c1'


create 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims_dev', 'c1'

truncate 'ts_hb_ccpcogx_nogbd_r000_in:cogx_claims_part1' 



scan 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims' , {STARTROW => 'ffd02124125135820', ENDROW => 'ffd02124125135826'} 


get 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims','00028cf7116038207202018081420180814' 
scan 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims' , {STARTROW => '00028cf7116038207202018081420180814', ENDROW => '00028cf7116038207202018081420180815'} 

 
scan 'dv_hb_ccpcogx_nogbd_r000_in:cogx_claims_test'' , {STARTROW => 'fff7b0ea741874128502018081620180816', ENDROW => 'fff7b0ea741874128502018081620180817'} 


import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
scan 'dv_hb_ccpcogx_nogbd_r000_in:um_auth'
count  'dv_hb_ccpcogx_nogbd_r000_in:um_auth' 
scan 'dv_hb_ccpcogx_nogbd_r000_in:um_auth', {FILTER => org.apache.hadoop.hbase.filter.RowFilter.new(CompareFilter::CompareOp.valueOf('EQUAL'),SubstringComparator.new("word_by_which_you_want_to_search"))} 

truncate  'dv_hb_ccpcogx_nogbd_r000_in:um_auth' 

select * from ts_ccpcogxph_nogbd_r000_wh.cogx_etl_audit where cogx_etl_audit.app_id='application_1562824131263_2865';
