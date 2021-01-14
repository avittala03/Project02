package com.am.cogx.etl.config

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

object CogxSpark2Config extends Serializable {

val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

 /////////// When Run on Local ///////////////

  //val spark = SparkSession.builder().appName("FSI").master("local[*]").getOrCreate()


 
 ///////////  End of When Run on Local ///////////////

/////////////////// When Run on Cluster /////////////////////


 val spark = SparkSession
    .builder()
   // .config("spark.sql.warehouse.dir", warehouseLocation)
   // .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.max.dynamic.partitions", "15000")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("hive.warehouse.data.skipTrash", "true")
    .config("spark.sql.parquet.writeLegacyFormat", "true")

    .enableHiveSupport()
    .getOrCreate()
    
  spark.conf.set("spark.sql.shuffle.partitions", "4200")
  spark.conf.set("spark.sql.avro.compression.codec", "snappy")
  spark.conf.set("spark.kryo.referenceTracking",	"false")
  spark.conf.set("hive.exec.dynamic.partition", "true")
  spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  spark.conf.set("spark.sql.parquet.filterPushdown", "true")
  spark.conf.set("spark.driver.maxResultSize", "5G")
  //spark.sparkContext.hadoopConfiguration.addResource(com.am.cogx.etl.config.CogxSpark2Config.getClass.getClassLoader.getResourceAsStream("hbase-site.xml"))
  spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
  spark.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")





   
/////////////////// End of When Run on Cluster /////////////////////
    
  //  config("spark.sql.shuffle.partitions", 6)

  
 
  spark.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
  lazy val hdfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
}