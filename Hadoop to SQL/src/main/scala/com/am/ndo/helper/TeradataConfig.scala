package com.am.ndo.helper
import java.io.InputStreamReader
import java.util.Calendar
import java.util.Date
import java.util.Properties

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Experimental
import org.apache.hadoop.fs.Path

import com.am.ndo.config.ConfigKey
import com.am.ndo.config.Spark2Config
import com.typesafe.config.ConfigFactory

import grizzled.slf4j.Logging
import java.text.SimpleDateFormat
import com.am.ndo.util.Encryption


class TeradataConfig(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory){
    val hdfsPath = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val appConfFilePath = hdfsPath.open(new Path(configPath+"/application.conf"))
    val appConfReaderNew = new InputStreamReader(appConfFilePath)
    val appConfig = ConfigFactory.parseReader(appConfReaderNew)
    val dbserverurl = appConfig.getString("ndo.secret.dbserverurl")
    val dbserverurl2 = appConfig.getString("ndo.secret.dbserverurl2")
    val dbserverurl3 = appConfig.getString("ndo.secret.dbserverurl3")
    val jdbcdriver = appConfig.getString("ndo.secret.jdbcdriver")
    val dbuserid = appConfig.getString("ndo.secret.dbuserid")
    val encriptedPassword = appConfig.getString("ndo.secret.dbpassword")
    val dbpassword = Encryption.decrypt(encriptedPassword)
    val hiveFileFormat = appConfig.getString("ndo.secret.hivedataformat")
     val hiveDB = appConfig.getString("ndo.secret.hivedb")
     val hiveDB2= appConfig.getString("ndo.secret.hivedb2")
     
    val saveMode = appConfig.getString("ndo.secret.insertmode")
    val readPartitionNum = appConfig.getString("ndo.secret.numpartitions").toInt

    
  }