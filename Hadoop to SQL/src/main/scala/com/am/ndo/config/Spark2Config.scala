package com.am.ndo.config

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

object Spark2Config extends Serializable {

  val spark = SparkSession
    .builder()
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("hive.warehouse.data.skipTrash", "true")
    .config("spark.sql.parquet.writeLegacyFormat", "true")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
  spark.sparkContext .hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  lazy val hdfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
}