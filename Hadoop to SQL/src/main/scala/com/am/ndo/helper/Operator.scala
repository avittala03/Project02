package com.am.ndo.helper

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.DataFrame
import grizzled.slf4j.Logging
import org.joda.time.DateTime

/**
 * Created by yuntliu on 10/11/2017.
 */
trait Operator extends Logging {

  //The following Functions need to be override on the operation detail
  def loadData(): Map[String, DataFrame]
  def writeData(outDFs: Map[String, DataFrame]): Unit
  def processData(inDFs: Map[String, DataFrame]): Map[String, DataFrame]

  def loadDataWithABC(): Map[String, DataFrame] = {
    beforeLoadData()
    val data = loadData()
    afterLoadData()
    (data)
  }

  def processDataWithABC(inDFs: Map[String, DataFrame]): Map[String, DataFrame] = {
    beforeProcData
    val resultDF = processData(inDFs)
    afterProcData
    (resultDF)
  }

  def writeDataWithABC(outDFs: Map[String, DataFrame]): Unit = {
    beforeWriteData
    writeData(outDFs)
    afterWriteData
  }

  def operation(): Unit = {
    writeDataWithABC(processDataWithABC(loadDataWithABC()));
  }

  def beforeLoadData(): Unit
  def afterWriteData(): Unit

  def afterLoadData(): Unit = {
    info("*********** End Loading Data **************")
  }

  def beforeProcData(): Unit = {
    info("*********** Start Proc Data **************")
    info("[NDO-ETL] Proc Start time: " + DateTime.now)
  }

  def afterProcData(): Unit = {
    info("[NDO-ETL] Proc End time: " + DateTime.now)
    info("*********** End Proc Data **************")
  }

  def beforeWriteData(): Unit = {
    info("*********** Start Write Data **************")
  }

}

