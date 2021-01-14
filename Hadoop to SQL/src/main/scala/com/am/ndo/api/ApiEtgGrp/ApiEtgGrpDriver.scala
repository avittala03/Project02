package com.am.ndo.api.ApiEtgGrp


  import grizzled.slf4j.Logging
import com.am.ndo.util.NDOCommonUtils
import com.am.ndo.config.Spark2Config
import org.joda.time.DateTime
import org.joda.time.Minutes

object ApiEtgGrpDriver extends Logging {

  def main(args: Array[String]): Unit = {

    require(args != null && args.size == 3, NDOCommonUtils.argsErrorMsg)

    val Array(confFilePath, env, queryFileCategory) = args
    try {
      val startTime = DateTime.now
      println(s"[NDO-ETL] NDO API ETG GRP tables load Started: $startTime")
      (new ApiEtgGrpOperation(confFilePath, env, queryFileCategory)).operation()
      println(s"[NDO-ETL]  NDO API ETG GRP tables load Completed at: " + DateTime.now())
      println(s"[NDO-ETL] Time Taken for NDO API ETG GROUP table load Completion :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

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

