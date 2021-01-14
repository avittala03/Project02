package com.am.cogx.etl.helper

import java.text.SimpleDateFormat
import java.util.Calendar
import com.am.cogx.etl.config._
import org.apache.spark.sql.DataFrame
import grizzled.slf4j.Logging
import org.apache.spark.sql.functions._
/**
 * Created by yuntliu on 10/11/2017.
 */
trait CogxOperator extends Logging {

  val DATE_FORMAT = "yyyy-MM-dd-HH:mm:sss";
  var sdf = new SimpleDateFormat(DATE_FORMAT);


  
  
  //The following Functions need to be override on the operation detail
  def loadData(): Map[String, DataFrame]
  //def writeData(outDFs: Map[String, DataFrame]): Unit
  //def writeData(outDFs: RDD[(ImmutableBytesWritable, Put)]): Unit
  def writeData(outDFs: Map[String, DataFrame]): Unit
  //def processData(inDFs: Map[String, DataFrame]): Map[String, DataFrame] 
 // def processData(inDFs: Map[String, DataFrame]): RDD[(ImmutableBytesWritable, Put)] 
  def processData(inDFs: Map[String, DataFrame]): Map[String, DataFrame]
  def beforeLoadData(): Unit
  def afterWriteData(): Unit


  // This section define the AOP for Load
  //  def beforeLoadData(): Unit = {
  //    info("*********** Start Loading Data **************")
  //  }
  def afterLoadData(): Unit = {
    info("*********** End Loading Data **************")
  }
  def loadDataWithABC(): Map[String, DataFrame] = {
    beforeLoadData()
    val data = loadData()
    afterLoadData()
    return data;
  }

  // This section define the AOP for Load
  def beforeWriteData(): Unit = {
    info("*********** Start Write Data **************")
  }
  //  def afterWriteData(): Unit = {
  //    info("*********** End Write Data **************")
  //  }
  def writeDataWithABC(outDFs: Map[String, DataFrame]): Unit = {
  //  def writeDataWithABC(outDFs: RDD[(ImmutableBytesWritable, Put)]): Unit = {
    beforeWriteData
    writeData(outDFs)
    afterWriteData
  }

  def beforeProcData(): Unit = {
    info("*********** Start Proc Data **************")
    info(s"[COGX] Proc Start time: " + sdf.format(Calendar.getInstance().getTime()))
  }
  def afterProcData(): Unit = {
    info(s"[COGX] Proc End time: " + sdf.format(Calendar.getInstance().getTime()))
    info("*********** End Proc Data **************")
  }
  def processDataWithABC(inDFs: Map[String, DataFrame]): Map[String, DataFrame] = {
  // def processDataWithABC(inDFs: Map[String, DataFrame]): RDD[(ImmutableBytesWritable, Put)]  = {
    beforeProcData
    val resultDF = processData(inDFs)
    afterProcData
    return resultDF
  }

  def operation(): Unit = {
    writeDataWithABC(processDataWithABC(loadDataWithABC()));
  }

  //def processData(df:DataFrame):RDD[(String,Int)];

}

