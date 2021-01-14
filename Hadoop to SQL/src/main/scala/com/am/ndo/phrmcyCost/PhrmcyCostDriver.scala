package com.am.ndo.phrmcyCost

import grizzled.slf4j.Logging
import com.am.ndo.util.NDOCommonUtils
import com.am.ndo.config.Spark2Config
import org.joda.time.DateTime
import org.joda.time.Minutes

object PhrmcyCostDriver extends Logging {
   def main(args: Array[String]): Unit = {

    require(args != null && args.size == 3, NDOCommonUtils.argsErrorMsg)

    val Array(confFilePath, env, queryFileCategory) = args
    try {
      val startTime = DateTime.now
      println(s"[NDO-ETL]NDO Pharmacy Cost Started: $startTime")
      (new PhrmcyCostOperation(confFilePath, env, queryFileCategory)).operation()
      println(s"[NDO-ETL] NDO Pharmacy Cost program Completed at: " + DateTime.now())
      println(s"[NDO-ETL] Time Taken for Pharmacy Cost Completion :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    } catch {
      case th: Throwable =>
        error("[NDO-ETL] [main] Exception occurred " + th)
        throw th
    } finally {
      println("[NDO-ETL] Stopping spark Context")
      Spark2Config.spark.sparkContext.stop()
    }
  }
  
}