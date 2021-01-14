//package com.am.hpip.config
//
//import org.apache.hadoop.fs.FileSystem
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.storage.StorageLevel
//
//object SparkConfig extends Serializable {
//
//  val conf = new SparkConf()
//    .set("hive.execution.engine", "spark")
//    .set("spark.acls.enable", "true")
// //   .set("spark.extraListeners","com.am.hpip.util.AuditListener")
//
//  val sc = new SparkContext(conf)
//  sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
//  //sc.addSparkListener(new AuditListener)
//
//  val hiveContext = new HiveContext(sc)
//  hiveContext.setConf("hive.exec.dynamic.partition", "true")
//  hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
//  hiveContext.setConf("spark.sql.parquet.compression.codec", "snappy")
//  hiveContext.setConf("hive.warehouse.data.skipTrash", "true")
//  //Below property is added to resolve double to decimal cast issue
//  hiveContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")
//
//  lazy val sqlContext = new SQLContext(sc)
//  lazy val hdfs: FileSystem = FileSystem.get(sc.hadoopConfiguration)
//}