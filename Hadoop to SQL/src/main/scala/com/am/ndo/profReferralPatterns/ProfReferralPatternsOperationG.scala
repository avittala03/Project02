package com.am.ndo.profReferralPatterns

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Locale

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import org.joda.time.DateTime
import org.joda.time.Minutes

import com.am.ndo.config.ConfigKey
import com.am.ndo.helper.Audit
import com.am.ndo.helper.OperationSession
import com.am.ndo.helper.Operator
import com.am.ndo.helper.WriteExternalTblAppend
import com.am.ndo.helper.WriteManagedTblInsertOverwrite
import com.am.ndo.helper.WriteSaveAsTableOverwrite
import com.am.ndo.util.DateUtils
import com.typesafe.config.ConfigException
class ProfReferralPatternsOperationG(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

	import spark.implicits._

	//Audit
	var program = ""
	var user_id = ""
	var app_id = ""
	var start_time: DateTime = DateTime.now()
	var start = ""

	var listBuffer = ListBuffer[Audit]()

	def loadData(): Map[String, DataFrame] = {

					val diagQuery = config.getString("query_prof_referral_pattern_diag").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)
					val bkbnIpBvQuery = config.getString("query_prof_referral_pattern_bkbn_ip_bv").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)

  					println(s"[NDO-ETL] Query for reading data from diag table is $diagQuery")
  					println(s"[NDO-ETL] Query for reading data from bkbn_ip_bv table is $bkbnIpBvQuery")

					val diagDf = spark.sql(diagQuery)
					val bkbnIpBvDF = spark.sql(bkbnIpBvQuery)

					val outputMap = Map(
							"diag" -> diagDf,
							"bkbn_ip_bv" -> bkbnIpBvDF)

					outputMap
	}

	def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
			val startTime = DateTime.now
					println(s"[NDO-ETL] Processing Data Started: $startTime")

					val diagDf = inMapDF.getOrElse("diag", null)
					
					val snapNbr = getSnapNbr(inMapDF)

					
					val tabProf = config.getString("tab_prof")
					val tabProfNppes = config.getString("tab_prof_nppes")
					val tabProfPdl = config.getString("tab_prof_pdl")
					val tabProfPdlNpi = config.getString("tab_prof_pdl_npi")
					val tabProfPdlTax = config.getString("tab_prof_pdl_tax")			
					val tabProfBilNpi = config.getString("tab_prof_bil_npi")
					val tabProfBilfTax = config.getString("tab_prof_bil_tax")		
					val tabProfRefNpi = config.getString("tab_prof_ref_npi")
					val tabProfRefTax = config.getString("tab_prof_ref_tax")
					val tabProfRenNpi = config.getString("tab_prof_ren_npi")
					val tabProfRenTax = config.getString("tab_prof_ren_tax")
					val tabprofFinalReport = config.getString("tab_prof_final_report")
					val tabprofFinalReportBkp = config.getString("tab_prof_final_report_bkp")
					
						val maxrun_id = getMAxRunId(tabprofFinalReport)
					println(s"[NDO-ETL] Max run id is : $maxrun_id")

			val f = for {


				////-------------------------------- File g -------------------------------------------

				profFinalReportDf <- profFinalReport(tabProf, tabProfRefNpi, tabProfRefTax, tabProfBilNpi, tabProfBilfTax, tabProfRenNpi, tabProfRenTax, tabProfNppes, diagDf, stagingHiveDB)

				val profFinalReportWC = profFinalReportDf.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
				withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
				withColumn(lastUpdatedDate, lit(current_timestamp())).
				withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).withColumn("run_id", lit(maxrun_id)).withColumn("SNAP_YEAR_MNTH_NBR",lit(snapNbr)).alias("SNAP_YEAR_MNTH_NBR")

				WriteprofFinalReportDf <- WriteManagedTblInsertOverwrite(profFinalReportWC, warehouseHiveDB, tabprofFinalReport)
				WriteprofFinalReportDfBkp <- WriteExternalTblAppend(profFinalReportWC, warehouseHiveDB, tabprofFinalReportBkp)

			} yield {
				println("done")
			}
			f.run(spark)

			runIDInsert(tabprofFinalReport, maxrun_id)
			null
	}

	def writeData(map: Map[String, DataFrame]): Unit = {
			val startTime = DateTime.now()
					//Writing the data to a table in Hive
					println(s"[NDO-ETL] Writing Dataframes to Hive started at: $startTime")
					println(s"[NDO-ETL] writing the data to target tables is Completed at: " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

	}


	def getMAxRunId(tablename: String): Long = {
			val startTime = DateTime.now()
					//Writing the data to a table in Hive
					println(s"[NDO-ETL] Writing Dataframes to Hive started at: $startTime")
					val tab = f"$tablename".toUpperCase()
					//***  Query   for Max run_id column value*****//
					val checkMaxRunId = s"SELECT run_id from (SELECT  run_id,rank() over (order by run_id desc) as r from  $warehouseHiveDB.run_id_table where sub_area='$tab' ) S where S.r=1"
					println(s"[NDO-ETL] Max run id for the table $warehouseHiveDB.$tablename is" + checkMaxRunId)

					//***  Getting the max value of run_id *****//
					val checkMaxRunIdDF = spark.sql(checkMaxRunId).collect()
					val maxRunIdVal = checkMaxRunIdDF.head.getLong(0)

					//***  Incrementing the value  of max run_id*****//
					val maxrun_id = maxRunIdVal + 1

					println(s"[NDO-ETL] writing the data to target tables is Completed at: " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")
					maxrun_id
	}

	def runIDInsert(tablename: String, maxrun_id: Long) = {
			spark.sql(f"use $warehouseHiveDB")
			val run_id_tbl = f"$tablename".toUpperCase()
			//append run_id and subject area to the run_id table
			val runid_insert_query = "select  " + maxrun_id + " as run_id,'" + run_id_tbl + "' as sub_area ";
			val data = spark.sql(runid_insert_query)
					data.write.mode("append").insertInto(f"$warehouseHiveDB.run_id_table")
	}
	
	def getSnapNbr(inMapDF: Map[String, DataFrame]): Long = {
			val startTime = DateTime.now()
					//Writing the data to a table in Hive
					println(s"[HPIP-ETL] SNAP_YEAR_MNTH_NBR derivation started at : $startTime")

					val bkbnIpBv = inMapDF.getOrElse("bkbn_ip_bv", null).select($"SNAP_YEAR_MNTH_NBR").distinct

					val snapNbr = bkbnIpBv.orderBy($"SNAP_YEAR_MNTH_NBR".desc).head.getLong(0)

					println(s"[HPIP-ETL] latest SNAP_YEAR_MNTH_NBR available in all the tables is $snapNbr")
					println(s"[HPIP-ETL]  SNAP_YEAR_MNTH_NBR derivation Completed at: " + DateTime.now())
					println(s"[HPIP-ETL] Time Taken for derivation :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")
					snapNbr

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
				//ndoAuditDF.write.insertInto(warehouseHiveDB + """.""" + "ndo_audit")
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
				//ndoAuditDF.write.insertInto(warehouseHiveDB + """.""" + "ndo_audit")
	}

}


