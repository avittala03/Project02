package com.am.ndo.medcrRltvty

import grizzled.slf4j.Logging
import com.am.ndo.util.NDOCommonUtils
import com.am.ndo.config.Spark2Config
import org.joda.time.DateTime
import org.joda.time.Minutes

object MedcrRltvtyDriver extends Logging {
   def main(args: Array[String]): Unit = {

    require(args != null && args.size == 3, NDOCommonUtils.argsErrorMsg)

    val Array(confFilePath, env, queryFileCategory) = args
    try {
      val startTime = DateTime.now
      println(s"[NDO-ETL]NDO Medcr Relativity program Started: $startTime")
      (new MedcrRltvtyOperation(confFilePath, env, queryFileCategory)).operation()
      println(s"[NDO-ETL] NDO Medcr Relativity program Completed at: " + DateTime.now())
      println(s"[NDO-ETL] Time Taken for Medcr Relativity program Completion :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

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
