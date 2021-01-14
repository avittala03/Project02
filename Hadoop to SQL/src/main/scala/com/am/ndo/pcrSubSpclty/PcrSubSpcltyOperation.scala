package com.am.ndo.pcrSubSpclty

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.when
import org.joda.time.DateTime
import org.joda.time.Minutes

import com.am.ndo.config.ConfigKey
import com.am.ndo.helper.Audit
import com.am.ndo.helper.OperationSession
import com.am.ndo.helper.Operator
import com.am.ndo.util.DateUtils
import javassist.expr.Cast
import java.sql.Date

class PcrSubSpcltyOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

	import spark.implicits._

	//Audit
	var program = ""
	var user_id = ""
	var app_id = ""
	var start_time: DateTime = DateTime.now()
	var start = ""

	var listBuffer = ListBuffer[Audit]()

	def loadData(): Map[String, DataFrame] = {

			//Reading the data into Data frames
			val startTime = DateTime.now
					println(s"[NDO-ETL] The loading of Data started with start time at :  + $startTime")

					//Reading the queries from config file
					println("Reading the queries from config file")
					val pcrSubSpcltyExprncQuery = config.getString("query_pcr_sub_spclty_exprnc").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					
					println(s"[NDO-ETL] Query for reading data from factRsttdMbrshpQuery table is $pcrSubSpcltyExprncQuery")
					
					val  pcrSubSpcltyExprncDf= spark.sql(pcrSubSpcltyExprncQuery)
					println(s"[NDO-ETL] Showing sample data for pcrSubSpcltyExprncDf")
					pcrSubSpcltyExprncDf.show

					val mapDF = Map("pcr_sub_spclty_exprnc" -> pcrSubSpcltyExprncDf)

					println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					mapDF

	}

	def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
			val startTime = DateTime.now
					println(s"[NDO-ETL] Processing Data Started: $startTime")
					//Reading the data frames as elements from Map
					val pcrSubSpcltyExprncDf = inMapDF.getOrElse("pcr_sub_spclty_exprnc", null)
					val maxEtgRunIdQuery = config.getString("query_max_etg_run_id_pcr_sub_spclty").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val maxEtgRunIdDf = spark.sql(maxEtgRunIdQuery).collect()
					val maxEtgRunId = maxEtgRunIdDf.head.getLong(0)
					println(s"[NDO-ETL] The max etg_run_id is " + maxEtgRunId)
					val targetTablePcrSubSpclty = config.getString("target_pcr_sub_spclty")
					println(s"[NDO-ETL] the target table name from prop file is :" + targetTablePcrSubSpclty)
				
				val pcrSubSpcltyDf = pcrSubSpcltyExprncDf.filter(pcrSubSpcltyExprncDf("ETG_RUN_ID")===maxEtgRunId )
							    

					println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					val mapDF = Map(targetTablePcrSubSpclty -> pcrSubSpcltyDf)
					mapDF
	}

	def writeData(map: Map[String, DataFrame]): Unit = {
			val startTime = DateTime.now()
					//Writing the data to a table in Hive
					println(s"[NDO-ETL] Writing Dataframes to Hive started at: $startTime")

					map.par.foreach(x => {
						val tableName = x._1
								val startTime = DateTime.now()
								println(s"[NDO-ETL] Writing Dataframes to Hive table $tableName started at: $startTime")

								//Displaying the sample of data
								val df = x._2

								df.write.mode("overwrite").insertInto( inboundHiveDB+ """.""" + tableName)
								println(s"[NDO-ETL] Data load completed for table $inboundHiveDB.$tableName")

							
								println(s"[NDO-ETL] writing the data to target table $tableName is Completed at: " + DateTime.now())
								println(s"[NDO-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					})
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
