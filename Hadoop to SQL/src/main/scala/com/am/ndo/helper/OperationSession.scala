package com.am.ndo.helper

import java.io.File
import java.io.InputStreamReader

import org.apache.hadoop.fs.Path

import com.am.ndo.config.ConfigKey
import com.am.ndo.config.Spark2Config
import com.typesafe.config.ConfigFactory

import grizzled.slf4j.Logging

class OperationSession(configPath: String, env: String, queryFileCategory: String) extends Logging {

  
  val sc = Spark2Config.spark.sparkContext
  val spark = Spark2Config.spark
  val hdfs = Spark2Config.hdfs
  val appConfPath = configPath + File.separator + s"application_${env}.properties"
  val queryFilePath = configPath + File.separator + s"query_ndo.properties"
//  val configFilePath = configPath + File.separator + s"config_${queryFileCategory}.properties"

  info(s"[NDO-ETL] Application Config Path is $appConfPath")
  info(s"[NDO-ETL] Query File Path is $queryFilePath")
//  info(s"[NDO-ETL] Config File Path is $configFilePath")

  //loading application_<env>.properties file
  val appConfFile = hdfs.open(new Path(appConfPath))
  val appConfReader = new InputStreamReader(appConfFile)
  val appConf = ConfigFactory.parseReader(appConfReader)
  
  //loading query_<queryFileCategory>.properties file
  val queryConfFile = hdfs.open(new Path(queryFilePath))
  val queryConfReader = new InputStreamReader(queryConfFile)
  val queryConf = ConfigFactory.parseReader(queryConfReader)
  //loading config_${queryFileCategory}.properties file
//  val dateConfFile = hdfs.open(new Path(configFilePath))
//  val dateConfReader = new InputStreamReader(dateConfFile)
//  val dateConf = ConfigFactory.parseReader(dateConfReader)

  //merge both above conf file
//  val config = queryConf.withFallback(appConf).withFallback(dateConf).resolve()
  val config = queryConf.withFallback(appConf).resolve()
  val inboundHiveDB = config.getString(ConfigKey.inboundHiveDB)
  val inboundHiveDBNogbd = config.getString(ConfigKey.inboundHiveDBNogbd)
  val stagingHiveDB = config.getString(ConfigKey.stageHiveDB)
  val warehouseHiveDB = config.getString(ConfigKey.wareHouseHiveDB)
  val warehouseHiveDBPath = config.getString(ConfigKey.wareHouseHiveDBPath)
  val lastUpdatedDate = config.getString(ConfigKey.auditColumnName)
  

  info(s"[NDO-ETL] The Inbound Hive schema is $inboundHiveDB")
  info(s"[NDO-ETL] The Staging Hive schema is $stagingHiveDB")
  info(s"[NDO-ETL] The warehouse database schema is $warehouseHiveDB")
  info(s"[NDO-ETL] The Audit column name is $lastUpdatedDate")
 

  info(s"[NDO-ETL] Construct OperationStrategy")

}

object OperationSession {
  def apply(confFilePath: String, env: String, queryFileCategory: String): OperationSession = new OperationSession(confFilePath, env, queryFileCategory)
}