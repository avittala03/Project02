/*
 * Copyright (c) 2017, Anthem Inc. All rights reserved.
 * DO NOT ALTER OR REMOVE THIS FILE HEADER.
 *
 */

package com.am.etl.dao

import scala.xml.XML
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import com.am.cogx.pi.etl.Driver
import com.am.etl.config.{DataSourceConfig, TableConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableOutputFormat}
import org.apache.hadoop.security.alias.CredentialProviderFactory

import scala.collection.JavaConverters.mapAsScalaMapConverter

/**
 * @author T Murali
 * @version 1.0
 *
 *  A generic class for boot-strapping the SparkContext for Extract Transform Load implementations
 */

class ETLStrategy {

  def getLoadLogKey(): String = {
    java.util.UUID.randomUUID.toString
  }

  def getLoadDateTime(): String = {
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(ZonedDateTime.now)
  }

  def getLoadDateTime(aFormat: String): String = {
    DateTimeFormatter.ofPattern(aFormat).format(ZonedDateTime.now)
  }

  def getSparkContext(): SparkContext = {
    ETLStrategy.sparkSession.sparkContext
  }

  def getSparkSession(): SparkSession = {
    ETLStrategy.sparkSession
  }

  def getHadoopConfiguration: Configuration = {
    ETLStrategy.sparkSession.sparkContext.hadoopConfiguration
  }
  
  def getMapReduceJobConfiguration(tableName:String):Configuration = {
      val job = Job.getInstance(HBaseConfiguration.create(), "HDFS-to-HBase ETL")
      job.setOutputFormatClass(new org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable].getClass)
      job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,tableName)
      job.getConfiguration
  }

  def getHBaseJobConfiguration(tableName:String): Configuration = {
    val conf = HBaseConfiguration.create
    val xmlFile = Option(Driver.getClass.getClassLoader.getResource("hbase-site.xml"))
    xmlFile.foreach(f => conf.addResource(f))
    val job = Job.getInstance(conf, "HDFS-to-HBase ETL")
    job.setOutputFormatClass(new org.apache.hadoop.hbase.mapreduce.TableOutputFormat[ImmutableBytesWritable].getClass)
    TableMapReduceUtil.initCredentials(job)
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,tableName)
    job.getConfiguration
  }

  def getCredentialSecret(aCredentialStore: String, aCredentialAlias: String): String = {
    val config = getHadoopConfiguration
    config.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, aCredentialStore)
    String.valueOf(config.getPassword(aCredentialAlias))
  }
  
  def insertLoadLogTable(table: TableConfig, load_log_key: String, load_strt_dtm: String, load_end_dtm: String, rows_prcsd: Long, success_flg: String, err_msg: String, trgt_type: String = "HBase") = {
    getSparkSession().sql("INSERT INTO TABLE " + table.auditTable + " SELECT \"" + load_log_key + "\",\"" + load_strt_dtm + "\",\"" + load_end_dtm + "\",\"" + table.dataSourceConfig.sourceType + "\",\"" + table.dataSourceConfig.sourceDesc + "\",\"" + trgt_type + "\",\"" + table.hbase_schema + "\",\"" + table.hbase_table_name + "\",\"" + rows_prcsd + "\",\"" + success_flg + "\",\"" + err_msg + "\"")
  }

  def insertLoadLogTableHive(aAuditTable: String, aLoadLogKey: String, aETLTable: String, aLoadStartDttm: String, aLoadEndDttm: String, aLoadMthd: String) = {
    getSparkSession().sql("INSERT INTO TABLE " + aAuditTable + " SELECT '" + aLoadLogKey + "','" + aETLTable + "','" + aLoadStartDttm + "','" + aLoadEndDttm + "','" + aLoadMthd + "'")
  }

  def getLastLoadDt(auditTable: String, srcType:String, trgtType: String, tbl: String, schema: String) : String = {
    val query = "SELECT COALESCE(MAX(CAST(load_strt_dtm AS STRING)), '1900-01-01 00:00:00') last_load_dt FROM " + auditTable + " WHERE source_type = '" + srcType + "' AND target_type = '" + trgtType + "' AND schema_name = '" + schema + "' AND table_name = '" + tbl + "' AND success_flg = 'Y'"
    println(getLoadDateTime() + " INFO: Load Log Table Query " + query)
    val df = getSparkSession().sql(query)
    val dt = df.first().getString(0)
    println(getLoadDateTime() + " INFO: getLastLoadDt returns " + dt)
    dt
  }

  def partitionLoaded(tableConfig: TableConfig) : Boolean = {
    val query = "SELECT COUNT(*) row_count FROM " + tableConfig.auditTable + " WHERE source_desc LIKE '%partition: " + tableConfig.partn_val + "' AND source_type = '" + tableConfig.dataSourceConfig.sourceType + "' AND target_type = '" + "HBase" + "' AND schema_name = '" + tableConfig.hbase_schema + "' AND table_name = '" + tableConfig.hbase_table_name + "' AND success_flg = 'Y'"
    val df = getSparkSession().sql(query)
    val dt = df.first().getLong(0)
    if(dt > 0)
      true
    else
      false
  }

  def getQueryBounds(table: TableConfig, lastLoadDate: String = ""): (String, String) = {
    val query = "(" + table.queries.get("bound_query") + ") T"
    val dboptions: Map[String, String] = Map(
      "url" -> table.dataSourceConfig.dbserverurl,
      "user" -> table.dataSourceConfig.username,
      "password" -> getCredentialSecret(table.dataSourceConfig.credentialproviderurl,table.dataSourceConfig.password),
      "driver" -> table.dataSourceConfig.jdbcdriver,
      "dbtable" -> query.replace("{{last_load_date}}", lastLoadDate).replace("{{mnth}}", lastLoadDate))
    val df = getSparkSession().read.format("jdbc").options(dboptions).load()
    val min:String = String.valueOf(BigDecimal(df.first().getAs(0).toString()).longValue())
    val max:String = String.valueOf(BigDecimal(df.first().getAs(1).toString()).longValue())
    (min, max)
  }

}

object ETLStrategy {
  val sparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()
  sparkSession.conf.set("spark.sql.shuffle.partitions", "2001")
  sparkSession.conf.set("spark.sql.avro.compression.codec", "snappy")
  sparkSession.conf.set("spark.kryo.referenceTracking",	"false")
  sparkSession.conf.set("hive.exec.dynamic.partition", "true")
  sparkSession.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  sparkSession.conf.set("spark.sql.parquet.filterPushdown", "true")
  sparkSession.conf.set("spark.driver.maxResultSize", "5G")
  sparkSession.sparkContext.hadoopConfiguration.addResource(Driver.getClass.getClassLoader.getResourceAsStream("hbase-site.xml"))
  sparkSession.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
  sparkSession.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
}

@SerialVersionUID(2017L)
case class JSONHolder(json:String)



  
  
