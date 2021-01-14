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
class ProfReferralPatternsOperationE(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

	import spark.implicits._

	//Audit
	var program = ""
	var user_id = ""
	var app_id = ""
	var start_time: DateTime = DateTime.now()
	var start = ""

	var listBuffer = ListBuffer[Audit]()

	def loadData(): Map[String, DataFrame] = {

					val clmProvQuery = config.getString("query_prof_referral_pattern_clm_prov").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)
					val clmLineProvQuery = config.getString("query_prof_referral_pattern_clm_line_prov").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)

					println(s"[NDO-ETL] Query for reading data from clm_prov table is $clmProvQuery")
					println(s"[NDO-ETL] Query for reading data from clm_line_prov table is $clmLineProvQuery")

					val clmProvDF = spark.sql(clmProvQuery)
					val clmLineProvDF = spark.sql(clmLineProvQuery)

					val outputMap = Map(
							"clm_prov" -> clmProvDF,
							"clm_line_prov" -> clmLineProvDF
							)

					outputMap
	}

	def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
			val startTime = DateTime.now
					println(s"[NDO-ETL] Processing Data Started: $startTime")

					val clmProv = inMapDF.getOrElse("clm_prov", null)
					val clmLineProv = inMapDF.getOrElse("clm_line_prov", null)

					val tabProfPdl = config.getString("tab_prof_pdl")
					val tabProfPdlNpi = config.getString("tab_prof_pdl_npi")
					val tabProfPdlTax = config.getString("tab_prof_pdl_tax")
					val tabProfBilNpi = config.getString("tab_prof_bil_npi")
					val tabProfBilfTax = config.getString("tab_prof_bil_tax")
					

			val f = for {


				////-------------------------------- File e -------------------------------------------
				yueProfBilNpi <- yueProfBilNpi(clmProv, clmLineProv, "prof", tabProfPdlNpi, stagingHiveDB)
				WriteyueProfBilNpi <- WriteManagedTblInsertOverwrite(yueProfBilNpi, stagingHiveDB, tabProfBilNpi)

				yueProfBilTax <- yueProfBilTax(clmProv, clmLineProv, "prof", tabProfPdlTax, stagingHiveDB)
				WriteyueProfBilNpi <- WriteManagedTblInsertOverwrite(yueProfBilTax, stagingHiveDB, tabProfBilfTax)


			} yield {
				println("done")
			}
			f.run(spark)

			null
	}

	def writeData(map: Map[String, DataFrame]): Unit = {
			val startTime = DateTime.now()
					//Writing the data to a table in Hive
					println(s"[NDO-ETL] Writing Dataframes to Hive started at: $startTime")
					println(s"[NDO-ETL] writing the data to target tables is Completed at: " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

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


