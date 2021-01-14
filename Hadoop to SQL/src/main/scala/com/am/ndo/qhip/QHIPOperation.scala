package com.am.ndo.qhip

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.coalesce
import org.joda.time.DateTime
import org.joda.time.Minutes
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Locale

import com.am.ndo.config.ConfigKey
import com.am.ndo.helper.Audit
import com.am.ndo.helper.OperationSession
import com.am.ndo.helper.Operator
import com.am.ndo.util.DateUtils
import com.am.ndo.helper.WriteExternalTblAppend
import com.am.ndo.helper.WriteManagedTblInsertOverwrite
import com.am.ndo.helper.WriteSaveAsTableOverwrite
import com.typesafe.config.ConfigException

class QHIPOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

  import spark.implicits._

  //Audit
  var program = ""
  var user_id = ""
  var app_id = ""
  var start_time: DateTime = DateTime.now()
  var start = ""

  var listBuffer = ListBuffer[Audit]()
  val tabQhip = config.getString("tab_qhip")
  val tabQhipBkp = config.getString("tab_qhip_bkp")
   
  def loadData(): Map[String, DataFrame] = {

    //Reading the data into Data frames
    val startTime = DateTime.now
    println(s"[NDO-ETL] The loading of Data started with start time at :  + $startTime")

    //Reading the queries from config file
    println("Reading the queries from config file")
    val qhipQuery = config.getString("query_qhip").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val ehoppaBaseQuery = config.getString("query_ehoppa_base").replaceAll(ConfigKey.warehouseDBPlaceHolder, warehouseHiveDB).toLowerCase()
   
    println(s"[NDO-ETL] Query for reading data from qhip table is $qhipQuery")
    println(s"[NDO-ETL] Query for reading data from ehoppa_base table is $ehoppaBaseQuery")
  
    val qhipDf = spark.sql(qhipQuery)

    val ehoppaBaseDf = spark.sql(ehoppaBaseQuery)

 
    val mapDF = Map("qhip" -> qhipDf, "ehoppa_base" -> ehoppaBaseDf)

    println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
    println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    mapDF
    

  }

  def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
    val startTime = DateTime.now
    println(s"[NDO-ETL] Processing Data Started: $startTime")
    //Reading the data frames as elements from Map
    val qhipDf = inMapDF.getOrElse("qhip", null).distinct().withColumn("QHIP_YEAR",year(to_date($"QHIP_END_DT","MM/dd/yyyy")))
    val ehoppaBaseDf = inMapDF.getOrElse("ehoppa_base", null).distinct().withColumn("MEDCR_ID",split($"hosp_nm",":")(0)).withColumn("HOSP_NM",split($"hosp_nm","(?<=^[^:]*)\\:")(1))

    val lastUpdatedDate = config.getString(ConfigKey.auditColumnName)

    val maxrun_id_qhip = getMAxRunId(tabQhip)
    println(s"[NDO-ETL] Max run id is : $maxrun_id_qhip")
    
    val qhipEhoppaDf = qhipDf.join(ehoppaBaseDf,qhipDf("MEDCR_ID")===ehoppaBaseDf("MEDCR_ID"),"left_outer").select(   
        qhipDf("MEDCR_ID"),
        qhipDf("PROV_NM"),
        qhipDf("QHIP_NBR"),
        qhipDf("QHIP_YEAR"),
        (to_date($"QHIP_STRT_DT","MM/dd/yyyy")).alias("QHIP_STRT_DT"),
        (to_date($"QHIP_END_DT","MM/dd/yyyy")).alias("QHIP_END_DT"),
        (coalesce(ehoppaBaseDf("PROV_ST_CD"), lit("N/A"))).alias("PROV_ST_CD"),
        coalesce(ehoppaBaseDf("HOSP_NM"), lit("N/A")).alias("HOSP_NM")
     ).distinct()
     
    val windowFunc = Window.partitionBy($"MEDCR_ID").orderBy($"QHIP_STRT_DT".desc)

			val qhipFilterDF = qhipEhoppaDf.withColumn("row_number", row_number().over(windowFunc)).filter($"row_number" === 1).select(   
        $"MEDCR_ID",
        $"PROV_NM",
        $"QHIP_NBR",
        $"QHIP_YEAR",
        $"QHIP_STRT_DT",
        $"QHIP_END_DT",
        $"PROV_ST_CD",
        $"HOSP_NM"
     ).withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
						withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
						withColumn(lastUpdatedDate, lit(current_timestamp())).
						withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).withColumn("run_id", lit(maxrun_id_qhip)).drop($"row_number")
    
		 runIDInsert(tabQhip, maxrun_id_qhip)
    
    val mapDF = Map(tabQhip -> qhipFilterDF)

    println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
    println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    mapDF
  }

  def writeData(map: Map[String, DataFrame]): Unit = {
    val startTime = DateTime.now()
    //Writing the data to a table in Hive
  
    val qhipEhoppaDf = map.getOrElse(tabQhip, null)
    
    println(s"[NDO-ETL] Write started for table $tabQhip at:" + DateTime.now())
    qhipEhoppaDf.write.mode("overwrite").insertInto(warehouseHiveDB + """.""" + tabQhip)
    qhipEhoppaDf.write.mode("append").insertInto(warehouseHiveDB + """.""" + tabQhipBkp)
        
    println(s"[HPIP-ETL] writing the data to target tables is Completed at: " + DateTime.now())
    println(s"[HPIP-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

  }
  
  def getMAxRunId(tablename: String): Long = {
			val startTime = DateTime.now()
					//Writing the data to a table in Hive
					println(s"[NDO-ETL] Derivation of MAX_RUN_ID started at: $startTime")
					val tab = f"$tablename".toUpperCase()
					//***  Query   for Max run_id column value*****//
					val checkMaxRunId = s"SELECT run_id from (SELECT  run_id,rank() over (order by run_id desc) as r from  $warehouseHiveDB.run_id_table where sub_area='$tab' ) S where S.r=1"
					println(s"[NDO-ETL] Max run id for the table $warehouseHiveDB.$tablename is" + checkMaxRunId)

					//***  Getting the max value of run_id *****//
					val checkMaxRunIdDF = spark.sql(checkMaxRunId).collect()
					val maxRunIdVal = checkMaxRunIdDF.head.getLong(0)

					//***  Incrementing the value  of max run_id*****//
					val maxrun_id = maxRunIdVal + 1

					println(s"[NDO-ETL] Derivation of MAX_RUN_ID is Completed at: " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for the Derivation of MAX_RUN_ID :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")
					maxrun_id
	}

	def runIDInsert(tablename: String, maxrun_id: Long) = {
			spark.sql(f"use $warehouseHiveDB")
			val run_id_tbl = f"$tablename".toUpperCase()
			println(s"[NDO-ETL] Inserting RUN ID into target table $warehouseHiveDB.$tablename")
			//append run_id and subject area to the run_id table
			val runid_insert_query = "select  " + maxrun_id + " as run_id,'" + run_id_tbl + "' as sub_area ";
			val data = spark.sql(runid_insert_query)
					data.write.mode("append").insertInto(f"$warehouseHiveDB.run_id_table")
					println(s"[NDO-ETL] Inserting RUN ID into target table completed for $warehouseHiveDB.$tablename")
	}

  @Override
  def beforeLoadData() {

    program = sc.appName
    user_id = sc.sparkUser
    app_id = sc.applicationId

    start_time = DateTime.now()
    start = DateUtils.getCurrentDateTime

    listBuffer += Audit(program, user_id, app_id, start, "0min", "Started")
    val ndoAuditDF = listBuffer.toDS().withColumn("last_updt_dtm", lit(current_timestamp()))
    ndoAuditDF.printSchema()
    ndoAuditDF.write.insertInto(warehouseHiveDB + """.""" + "ndo_audit")
  }

  @Override
  def afterWriteData() {
    val warehouseHiveDB = config.getString("warehouse-hive-db")
    var listBuffer = ListBuffer[Audit]()
    val duration = Minutes.minutesBetween(start_time, DateTime.now()).getMinutes + "mins"

    listBuffer += Audit(program, user_id, app_id, start, duration, "completed")
    val ndoAuditDF = listBuffer.toDS().withColumn("last_updt_dtm", current_timestamp())
    ndoAuditDF.printSchema()
    ndoAuditDF.show
    ndoAuditDF.write.insertInto(warehouseHiveDB + """.""" + "ndo_audit")
  }

}
