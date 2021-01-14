Note: we do not have Local run on BDF
- BDF table was copied to 

beeline 
 beeline -u "jdbc:hive2://dwbdtest1hs2lb.wp.com:10000/default;principal=hive/_HOST@DEVAD.WELLPOINT.COM;ssl=true"

========================================================================
***** Test run on Client Mode ********************
----------------------------------------------------------------------
hadoop fs -mkdir /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test

hadoop fs -rm -r -skipTrash /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/application_dev.properties
hadoop fs -rm -r -skipTrash /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/query_cogxHiveBDFHistory.properties
hadoop fs -rm -r -skipTrash /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/query_cogxHiveBDFIncremental.properties
hadoop fs -rm -r -skipTrash /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ag29035Test/ds-cogx-etl-2.0.6.jar

hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/application_dev.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/query_cogxHiveBDFHistory.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/query_cogxHiveBDFIncremental.properties  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/
hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/hbase-site.xml  /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/


hadoop fs -put  /data/01/dv/app/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/ds-cogx-etl-2.0.6.jar   /dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ag29035Test/
----------------------------------------------------------------------
spark2-submit \
--master yarn \
--deploy-mode cluster \
--name cogx_Test_client \
--queue ndo_coca_yarn  \
--driver-cores 5 \
--driver-memory 4G \
--num-executors 50 \
--executor-memory 40G \
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
--principal AG29035 \
--keytab /home/ag29035/keytabstore/ag29035.keytab  \
--files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml  \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/" \
--conf "spark.yarn.security.tokens.hbase.enabled=true" \
--name "COGX HBase ETL: cogx_um_load - by AG29035" \
--class com.am.cogx.etl.HiveBDF.CogxHiveBDFDriver \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/bin/ag29035Test/ds-cogx-padp-etl-2.0.5.jar   \
hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag29035Test/ dev cogxHiveBDFHistory

========================================================================




scan 'dv_hb_ccpcogx_nono_gbd_r000_in:cogx_claims' , { FILTER => " (TimestampsFilter ( 1556848066000, 1557848066000))"}
count 'dv_hb_ccpcogx_nono_gbd_r000_in:cogx_claims'
truncate 'dv_hb_ccpcogx_nono_gbd_r000_in:cogx_claims' 


import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
scan 'dv_hb_ccpcogx_nono_gbd_r000_in:um_auth'
count  'dv_hb_ccpcogx_nono_gbd_r000_in:um_auth' 
scan 'dv_hb_ccpcogx_nono_gbd_r000_in:um_auth', {FILTER => org.apache.hadoop.hbase.filter.RowFilter.new(CompareFilter::CompareOp.valueOf('EQUAL'),SubstringComparator.new("word_by_which_you_want_to_search"))} 

truncate table  'dv_hb_ccpcogx_nono_gbd_r000_in:um_auth' 


dv_hb_ccpcogx_no_gbd_r000_in:cogx_claims 

===================================================


counts 

================== Cogx Performance Test clmHeaderDF: 7919474
 ================== Cogx Performance Test clmDetailDF: 27952363
 
 
 YT count 
 ================== Cogx Performance Test clmHeaderDF: 7919474
 ================== Cogx Performance Test clmDetailDF: 27952363
 ================== Cogx Performance Test clmgncnatpea2DF: 599586089
  ================== Cogx Performance Test bdfCombineClaimsDF: 46409788
  [COGX]INFO: CogX Row Count => 46100099
   ================== Cogx Performance Test CogaClaimDataset: 46100099
   ================== Cogx Performance Test groupedmemberHbaseDataSet1: 2532875
    ================== Cogx Performance Test df1WithAuditColumn: 2532875
    
 






