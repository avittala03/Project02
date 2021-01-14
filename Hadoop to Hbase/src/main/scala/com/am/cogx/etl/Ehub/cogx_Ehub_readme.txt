
Spark submit 

spark2-submit --master yarn --queue ndo_coca_yarn --deploy-mode cluster --executor-memory 40G --executor-cores 2 --driver-cores 2 --driver-memory 30G --name "COGX HBase ETL: cogx_um_load - by ag21866" --files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml --principal AG21866 --keytab /home/ag21866/ag21866.keytab --conf spark.sql.codegen.wholeStage=true --conf spark.yarn.maxAppAttempts=1 --conf spark.driver.memoryOverhead=5120 --conf spark.sql.parquet.cacheMetadata=false --conf spark.yarn.executor.memoryOverhead=16384 --conf spark.network.timeout=420000 --conf "spark.driver.maxResultSize=15g" --conf "spark.default.parallelism=1000" --conf spark.kryoserializer.buffer.max=1024m --conf spark.rpc.message.maxSize=1024 --conf spark.sql.broadcastTimeout=5800 --conf spark.executor.heartbeatInterval=30s --conf spark.dynamicAllocation.executorIdleTimeout=90 --conf spark.dynamicAllocation.initialExecutors=0 --conf spark.dynamicAllocation.maxExecutors=100 --conf spark.dynamicAllocation.minExecutors=30 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.sql.cbo.enabled=true --conf "spark.yarn.security.tokens.hbase.enabled=true" --conf "spark.sql.shuffle.partitions=4200" --files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml --jars /opt/cloudera/parcels/CDH/jars/hive-contrib-1.1.0-cdh5.12.2.jar --jars /usr/lib/tdch/1.5/lib/terajdbc4.jar,/usr/lib/tdch/1.5/lib/tdgssconfig.jar --class com.am.cogx.etl.Ehub.CogxEhubDriver  hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag21866Test/cogx_ehub.jar hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag21866Test dev cogxEhub 


1. Test the below commands on spark shell. Creating a list with one sample row.

case class Ehup(SOURCESYSTEMCODE: String, CONTRACT: String, START_DT: String, END_DT: String, REVISION_DT: String, MAJOR_HEADING: String, MINOR_HEADING: String, VARIABLE: String, VARIABLE_DESC: String, VARIABLE_FORMAT: String, VARIABLE_NETWORK: String, VARIABLE_VALUE: String, NOTES : String)

//Below command to execute for one sample row with a value in the last column

List("WPD_LG|~2RRP|~01012018|~12319999|~|~COMPREHENSIVE MAJOR MEDICAL|~PHYSICIAN HOME/OFFICE|~PDEDWVCPYSVC|~MM DED WVD FOR COPAY SERVICES|~Y/N|~PAR|~Y|~NA").map { list => list.toString().replace("|~", "@").split("@",-1)}.map(a => Ehup(a(0),a(1), a(2), a(3), a(4), a(5),a(6),a(7),a(8),a(9),a(10),a(11),a(12))).toDF.show

+----------------+--------+--------+--------+-----------+--------------------+--------------------+------------+--------------------+---------------+----------------+--------------+-----+
|SOURCESYSTEMCODE|CONTRACT|START_DT|  END_DT|REVISION_DT|       MAJOR_HEADING|       MINOR_HEADING|    VARIABLE|       VARIABLE_DESC|VARIABLE_FORMAT|VARIABLE_NETWORK|VARIABLE_VALUE|NOTES|
+----------------+--------+--------+--------+-----------+--------------------+--------------------+------------+--------------------+---------------+----------------+--------------+-----+
|          WPD_LG|    2RRP|01012018|12319999|           |COMPREHENSIVE MAJ...|PHYSICIAN HOME/OF...|PDEDWVCPYSVC|MM DED WVD FOR CO...|            Y/N|             PAR|             Y|  asd|
+----------------+--------+--------+--------+-----------+--------------------+--------------------+------------+--------------------+---------------+----------------+--------------+-----+

//Below command to execute for one sample row with empty value in last column

List("WPD_LG|~2RRP|~01012018|~12319999|~|~COMPREHENSIVE MAJOR MEDICAL|~PHYSICIAN HOME/OFFICE|~PDEDWVCPYSVC|~MM DED WVD FOR COPAY SERVICES|~Y/N|~PAR|~Y|~").map { list => list.toString().replace("|~", "@").split("@",-1)}.map(a => Ehup(a(0),a(1), a(2), a(3), a(4), a(5),a(6),a(7),a(8),a(9),a(10),a(11),a(12))).toDF.show

+----------------+--------+--------+--------+-----------+--------------------+--------------------+------------+--------------------+---------------+----------------+--------------+-----+
|SOURCESYSTEMCODE|CONTRACT|START_DT|  END_DT|REVISION_DT|       MAJOR_HEADING|       MINOR_HEADING|    VARIABLE|       VARIABLE_DESC|VARIABLE_FORMAT|VARIABLE_NETWORK|VARIABLE_VALUE|NOTES|
+----------------+--------+--------+--------+-----------+--------------------+--------------------+------------+--------------------+---------------+----------------+--------------+-----+
|          WPD_LG|    2RRP|01012018|12319999|           |COMPREHENSIVE MAJ...|PHYSICIAN HOME/OF...|PDEDWVCPYSVC|MM DED WVD FOR CO...|            Y/N|             PAR|             Y|     |
+----------------+--------+--------+--------+-----------+--------------------+--------------------+------------+--------------------+---------------+----------------+--------------+-----+


2. Copy the Ehub extract file from local to HDFS path. 
//Reading the text file from HDFS

spark.read.option("header", "true").textFile("/ts/hdfsapp/vs2/pdp/pndo/phi/no_gbd/r000/bin/ag21866Test/EHUB_WPD_Extract_1559156410928.txt").map { list => list.toString().replace("|~", "@").split("@",-1)}.map(a => Ehup(a(0),a(1), a(2), a(3), a(4), a(5),a(6),a(7),a(8),a(9),a(10),a(11),a(12))).show

+----------------+--------+--------+--------+-----------+------------------+--------------------+-----------+--------------------+---------------+----------------+--------------+-----+
|SOURCESYSTEMCODE|CONTRACT|START_DT|  END_DT|REVISION_DT|     MAJOR_HEADING|       MINOR_HEADING|   VARIABLE|       VARIABLE_DESC|VARIABLE_FORMAT|VARIABLE_NETWORK|VARIABLE_VALUE|NOTES|
+----------------+--------+--------+--------+-----------+------------------+--------------------+-----------+--------------------+---------------+----------------+--------------+-----+
|SOURCESYSTEMCODE|CONTRACT|START_DT|  END_DT|REVISION_DT|     MAJOR_HEADING|       MINOR_HEADING|   VARIABLE|       VARIABLE_DESC|VARIABLE_FORMAT|VARIABLE_NETWORK|VARIABLE_VALUE|NOTES|
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AGASTRIC01|GASTRIC RESTRICTI...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|   #AHHC001|    HOME HEALTH CARE|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AHOSPICE01|NO UM REQUIRED FO...|            VAL|             ALL|             4|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AINFHME001|HOME INFUSION THE...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...| #AINPAT001|  INPATIENT HOSPITAL|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...| #AMASTECCN|NO REVIEW FOR MAS...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AMATERN001|           MATERNITY|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AMATERN002| MATERNITY C-SECTION|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|   #ASNF001|SKILLED NURSING F...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ASPINESGCN|SPINE (BACK) SURG...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...| #AANKRPLCN|   ANKLE REPLACEMENT|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ABCCSPMM01|SPECIALTY PHARM M...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ABONEDENCN|  BONE DENSITY STUDY|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|    #ACNDME|DURABLE MEDICAL E...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|   #ACNDRUG|DRUGS REMICAID AN...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ACNPBA0001|PRE-SERVICE PROCE...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|  #ACNSTALL|SPEECH THERAPY AF...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ADENTRELCN|DENTAL RELATED SE...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AUNKNOWNCN|UNLISTED PROCEDUR...|            VAL|             ALL|             1|     |
+----------------+--------+--------+--------+-----------+------------------+--------------------+-----------+--------------------+---------------+----------------+--------------+-----+
only showing top 20 rows


case class EhubWpidExtract(SOURCESYSTEMCODE: String, CONTRACT: String, START_DT: String, END_DT: String, REVISION_DT: String, MAJOR_HEADING: String, MINOR_HEADING: String, VARIABLE: String, VARIABLE_DESC: String, VARIABLE_FORMAT: String, VARIABLE_NETWORK: String, VARIABLE_VALUE: String, NOTES : String)

val ehubExtractDf = spark.read.option("header", "false").textFile("/ts/hdfsapp/vs2/pdp/pndo/phi/no_gbd/r000/bin/ag21866Test/EHUB_WPD_Extract_1559156410928.txt")
val header = ehubExtractDf.first()
val ehubExtractDfWH = ehubExtractDf.filter(row => row!=header).map { list => list.toString().replace("|~", "¤").split("¤",-1)}.map(a => EhubWpidExtract(a(0),a(1), a(2), a(3), a(4), a(5),a(6),a(7),a(8),a(9),a(10),a(11),a(12)))

scala> ehubExtractDfWH.show
+----------------+--------+--------+--------+-----------+------------------+--------------------+-----------+--------------------+---------------+----------------+--------------+-----+
|SOURCESYSTEMCODE|CONTRACT|START_DT|  END_DT|REVISION_DT|     MAJOR_HEADING|       MINOR_HEADING|   VARIABLE|       VARIABLE_DESC|VARIABLE_FORMAT|VARIABLE_NETWORK|VARIABLE_VALUE|NOTES|
+----------------+--------+--------+--------+-----------+------------------+--------------------+-----------+--------------------+---------------+----------------+--------------+-----+
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AGASTRIC01|GASTRIC RESTRICTI...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|   #AHHC001|    HOME HEALTH CARE|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AHOSPICE01|NO UM REQUIRED FO...|            VAL|             ALL|             4|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AINFHME001|HOME INFUSION THE...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...| #AINPAT001|  INPATIENT HOSPITAL|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...| #AMASTECCN|NO REVIEW FOR MAS...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AMATERN001|           MATERNITY|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AMATERN002| MATERNITY C-SECTION|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|   #ASNF001|SKILLED NURSING F...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ASPINESGCN|SPINE (BACK) SURG...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...| #AANKRPLCN|   ANKLE REPLACEMENT|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ABCCSPMM01|SPECIALTY PHARM M...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ABONEDENCN|  BONE DENSITY STUDY|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|    #ACNDME|DURABLE MEDICAL E...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|   #ACNDRUG|DRUGS REMICAID AN...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ACNPBA0001|PRE-SERVICE PROCE...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|  #ACNSTALL|SPEECH THERAPY AF...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ADENTRELCN|DENTAL RELATED SE...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AUNKNOWNCN|UNLISTED PROCEDUR...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AVARVEINCN|VARICOSE VEIN PRO...|            VAL|             ALL|             1|     |
+----------------+--------+--------+--------+-----------+------------------+--------------------+-----------+--------------------+---------------+----------------+--------------+-----+
only showing top 20 rows


count 'dv_hb_ccpcogx_nogbd_r000_in:cogx_benefits'
Current count: 1000, row: 028a18dbT561
Current count: 2000, row: 04f7fd77L395
get 'dv_hb_ccpcogx_nogbd_r000_in:cogx_benefits', '04f7fd77L395'


count 'dv_hb_ccpcogx_nogbd_r000_in:cogx_benefits_v',  INTERVAL=> 50
Current count: 50, row: 1daae9f0Q7K6
get 'dv_hb_ccpcogx_nogbd_r000_in:cogx_benefits_v', '1daae9f0Q7K6'



Spark submit 

spark2-submit --master yarn --queue ndo_coca_yarn --deploy-mode cluster --executor-memory 40G --executor-cores 2 --driver-cores 2 --driver-memory 30G --name "COGX HBase ETL: cogx_um_load - by ag21866" --files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml --principal AG21866 --keytab /home/ag21866/ag21866.keytab --conf spark.sql.codegen.wholeStage=true --conf spark.yarn.maxAppAttempts=1 --conf spark.driver.memoryOverhead=5120 --conf spark.sql.parquet.cacheMetadata=false --conf spark.yarn.executor.memoryOverhead=16384 --conf spark.network.timeout=420000 --conf "spark.driver.maxResultSize=15g" --conf "spark.default.parallelism=1000" --conf spark.kryoserializer.buffer.max=1024m --conf spark.rpc.message.maxSize=1024 --conf spark.sql.broadcastTimeout=5800 --conf spark.executor.heartbeatInterval=30s --conf spark.dynamicAllocation.executorIdleTimeout=90 --conf spark.dynamicAllocation.initialExecutors=0 --conf spark.dynamicAllocation.maxExecutors=100 --conf spark.dynamicAllocation.minExecutors=30 --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.sql.cbo.enabled=true --conf "spark.yarn.security.tokens.hbase.enabled=true" --conf "spark.sql.shuffle.partitions=4200" --files /etc/alternatives/spark2-conf/yarn-conf/hive-site.xml,/opt/cloudera/parcels/CDH/lib/hbase/conf/hbase-site.xml --jars /opt/cloudera/parcels/CDH/jars/hive-contrib-1.1.0-cdh5.12.2.jar --jars /usr/lib/tdch/1.5/lib/terajdbc4.jar,/usr/lib/tdch/1.5/lib/tdgssconfig.jar --class com.am.cogx.etl.Ehub.CogxEhubDriver  hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag21866Test/cogx_ehub.jar hdfs:///dv/hdfsapp/ve2/ccp/cogx/phi/no_gbd/r000/control/ag21866Test dev cogxEhub 


1. Test the below commands on spark shell. Creating a list with one sample row.

case class Ehup(SOURCESYSTEMCODE: String, CONTRACT: String, START_DT: String, END_DT: String, REVISION_DT: String, MAJOR_HEADING: String, MINOR_HEADING: String, VARIABLE: String, VARIABLE_DESC: String, VARIABLE_FORMAT: String, VARIABLE_NETWORK: String, VARIABLE_VALUE: String, NOTES : String)

//Below command to execute for one sample row with a value in the last column

List("WPD_LG|~2RRP|~01012018|~12319999|~|~COMPREHENSIVE MAJOR MEDICAL|~PHYSICIAN HOME/OFFICE|~PDEDWVCPYSVC|~MM DED WVD FOR COPAY SERVICES|~Y/N|~PAR|~Y|~NA").map { list => list.toString().replace("|~", "@").split("@",-1)}.map(a => Ehup(a(0),a(1), a(2), a(3), a(4), a(5),a(6),a(7),a(8),a(9),a(10),a(11),a(12))).toDF.show

+----------------+--------+--------+--------+-----------+--------------------+--------------------+------------+--------------------+---------------+----------------+--------------+-----+
|SOURCESYSTEMCODE|CONTRACT|START_DT|  END_DT|REVISION_DT|       MAJOR_HEADING|       MINOR_HEADING|    VARIABLE|       VARIABLE_DESC|VARIABLE_FORMAT|VARIABLE_NETWORK|VARIABLE_VALUE|NOTES|
+----------------+--------+--------+--------+-----------+--------------------+--------------------+------------+--------------------+---------------+----------------+--------------+-----+
|          WPD_LG|    2RRP|01012018|12319999|           |COMPREHENSIVE MAJ...|PHYSICIAN HOME/OF...|PDEDWVCPYSVC|MM DED WVD FOR CO...|            Y/N|             PAR|             Y|  asd|
+----------------+--------+--------+--------+-----------+--------------------+--------------------+------------+--------------------+---------------+----------------+--------------+-----+

//Below command to execute for one sample row with empty value in last column

List("WPD_LG|~2RRP|~01012018|~12319999|~|~COMPREHENSIVE MAJOR MEDICAL|~PHYSICIAN HOME/OFFICE|~PDEDWVCPYSVC|~MM DED WVD FOR COPAY SERVICES|~Y/N|~PAR|~Y|~").map { list => list.toString().replace("|~", "@").split("@",-1)}.map(a => Ehup(a(0),a(1), a(2), a(3), a(4), a(5),a(6),a(7),a(8),a(9),a(10),a(11),a(12))).toDF.show

+----------------+--------+--------+--------+-----------+--------------------+--------------------+------------+--------------------+---------------+----------------+--------------+-----+
|SOURCESYSTEMCODE|CONTRACT|START_DT|  END_DT|REVISION_DT|       MAJOR_HEADING|       MINOR_HEADING|    VARIABLE|       VARIABLE_DESC|VARIABLE_FORMAT|VARIABLE_NETWORK|VARIABLE_VALUE|NOTES|
+----------------+--------+--------+--------+-----------+--------------------+--------------------+------------+--------------------+---------------+----------------+--------------+-----+
|          WPD_LG|    2RRP|01012018|12319999|           |COMPREHENSIVE MAJ...|PHYSICIAN HOME/OF...|PDEDWVCPYSVC|MM DED WVD FOR CO...|            Y/N|             PAR|             Y|     |
+----------------+--------+--------+--------+-----------+--------------------+--------------------+------------+--------------------+---------------+----------------+--------------+-----+


2. Copy the Ehub extract file from local to HDFS path. 
//Reading the text file from HDFS

spark.read.option("header", "true").textFile("/ts/hdfsapp/vs2/pdp/pndo/phi/no_gbd/r000/bin/ag21866Test/EHUB_WPD_Extract_1559156410928.txt").map { list => list.toString().replace("|~", "@").split("@",-1)}.map(a => Ehup(a(0),a(1), a(2), a(3), a(4), a(5),a(6),a(7),a(8),a(9),a(10),a(11),a(12))).show

+----------------+--------+--------+--------+-----------+------------------+--------------------+-----------+--------------------+---------------+----------------+--------------+-----+
|SOURCESYSTEMCODE|CONTRACT|START_DT|  END_DT|REVISION_DT|     MAJOR_HEADING|       MINOR_HEADING|   VARIABLE|       VARIABLE_DESC|VARIABLE_FORMAT|VARIABLE_NETWORK|VARIABLE_VALUE|NOTES|
+----------------+--------+--------+--------+-----------+------------------+--------------------+-----------+--------------------+---------------+----------------+--------------+-----+
|SOURCESYSTEMCODE|CONTRACT|START_DT|  END_DT|REVISION_DT|     MAJOR_HEADING|       MINOR_HEADING|   VARIABLE|       VARIABLE_DESC|VARIABLE_FORMAT|VARIABLE_NETWORK|VARIABLE_VALUE|NOTES|
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AGASTRIC01|GASTRIC RESTRICTI...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|   #AHHC001|    HOME HEALTH CARE|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AHOSPICE01|NO UM REQUIRED FO...|            VAL|             ALL|             4|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AINFHME001|HOME INFUSION THE...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...| #AINPAT001|  INPATIENT HOSPITAL|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...| #AMASTECCN|NO REVIEW FOR MAS...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AMATERN001|           MATERNITY|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AMATERN002| MATERNITY C-SECTION|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|   #ASNF001|SKILLED NURSING F...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ASPINESGCN|SPINE (BACK) SURG...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...| #AANKRPLCN|   ANKLE REPLACEMENT|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ABCCSPMM01|SPECIALTY PHARM M...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ABONEDENCN|  BONE DENSITY STUDY|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|    #ACNDME|DURABLE MEDICAL E...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|   #ACNDRUG|DRUGS REMICAID AN...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ACNPBA0001|PRE-SERVICE PROCE...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|  #ACNSTALL|SPEECH THERAPY AF...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ADENTRELCN|DENTAL RELATED SE...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AUNKNOWNCN|UNLISTED PROCEDUR...|            VAL|             ALL|             1|     |
+----------------+--------+--------+--------+-----------+------------------+--------------------+-----------+--------------------+---------------+----------------+--------------+-----+
only showing top 20 rows


case class EhubWpidExtract(SOURCESYSTEMCODE: String, CONTRACT: String, START_DT: String, END_DT: String, REVISION_DT: String, MAJOR_HEADING: String, MINOR_HEADING: String, VARIABLE: String, VARIABLE_DESC: String, VARIABLE_FORMAT: String, VARIABLE_NETWORK: String, VARIABLE_VALUE: String, NOTES : String)

val ehubExtractDf = spark.read.option("header", "false").textFile("/ts/hdfsapp/vs2/pdp/pndo/phi/no_gbd/r000/bin/ag21866Test/EHUB_WPD_Extract_1559156410928.txt")
val header = ehubExtractDf.first()
val ehubExtractDfWH = ehubExtractDf.filter(row => row!=header).map { list => list.toString().replace("|~", "¤").split("¤",-1)}.map(a => EhubWpidExtract(a(0),a(1), a(2), a(3), a(4), a(5),a(6),a(7),a(8),a(9),a(10),a(11),a(12)))

scala> ehubExtractDfWH.show
+----------------+--------+--------+--------+-----------+------------------+--------------------+-----------+--------------------+---------------+----------------+--------------+-----+
|SOURCESYSTEMCODE|CONTRACT|START_DT|  END_DT|REVISION_DT|     MAJOR_HEADING|       MINOR_HEADING|   VARIABLE|       VARIABLE_DESC|VARIABLE_FORMAT|VARIABLE_NETWORK|VARIABLE_VALUE|NOTES|
+----------------+--------+--------+--------+-----------+------------------+--------------------+-----------+--------------------+---------------+----------------+--------------+-----+
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AGASTRIC01|GASTRIC RESTRICTI...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|   #AHHC001|    HOME HEALTH CARE|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AHOSPICE01|NO UM REQUIRED FO...|            VAL|             ALL|             4|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AINFHME001|HOME INFUSION THE...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...| #AINPAT001|  INPATIENT HOSPITAL|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...| #AMASTECCN|NO REVIEW FOR MAS...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AMATERN001|           MATERNITY|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AMATERN002| MATERNITY C-SECTION|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|   #ASNF001|SKILLED NURSING F...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ASPINESGCN|SPINE (BACK) SURG...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...| #AANKRPLCN|   ANKLE REPLACEMENT|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ABCCSPMM01|SPECIALTY PHARM M...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ABONEDENCN|  BONE DENSITY STUDY|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|    #ACNDME|DURABLE MEDICAL E...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|   #ACNDRUG|DRUGS REMICAID AN...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ACNPBA0001|PRE-SERVICE PROCE...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|  #ACNSTALL|SPEECH THERAPY AF...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#ADENTRELCN|DENTAL RELATED SE...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AUNKNOWNCN|UNLISTED PROCEDUR...|            VAL|             ALL|             1|     |
|          WPD_LG|    2N5M|07012017|01012018|           |GENERAL PROVISIONS|UTILIZATION MANAG...|#AVARVEINCN|VARICOSE VEIN PRO...|            VAL|             ALL|             1|     |
+----------------+--------+--------+--------+-----------+------------------+--------------------+-----------+--------------------+---------------+----------------+--------------+-----+
only showing top 20 rows

