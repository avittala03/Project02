package com.am.ndo.controlTotals


import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.Path
import java.util.Properties
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import org.apache.hadoop.fs.FileSystem
import org.joda.time.DateTime
import org.joda.time.Minutes
import com.am.ndo.util.DateUtils
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Locale
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.functions.month
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.types.IntegerType
import com.am.ndo.helper.TeradataConfig
import com.typesafe.config.ConfigFactory
import com.am.ndo.util.Encryption
import com.am.ndo.config.ConfigKey
import com.am.ndo.helper.Audit
import com.am.ndo.helper.OperationSession
import com.am.ndo.helper.Operator
import com.am.ndo.util.DateUtils
import com.am.ndo.helper.WriteExternalTblAppend
import com.typesafe.config.ConfigException
import org.apache.spark.sql.functions.upper
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.Path
import java.util.Properties
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import org.apache.hadoop.fs.FileSystem
import org.joda.time.DateTime
import org.joda.time.Minutes
import com.am.ndo.util.DateUtils
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Locale
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.functions.month
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.types.IntegerType
import com.am.ndo.helper.TeradataConfig
import com.typesafe.config.ConfigFactory
import com.am.ndo.util.Encryption
import com.am.ndo.config.ConfigKey
import com.am.ndo.helper.Audit
import com.am.ndo.helper.OperationSession
import com.am.ndo.helper.Operator
import com.am.ndo.util.DateUtils
import com.am.ndo.helper.WriteExternalTblAppend
import org.apache.spark.storage.StorageLevel
import com.typesafe.config.ConfigException
import org.apache.spark.sql.catalyst.expressions.Substring
import com.am.ndo.helper.WriteManagedTblInsertOverwrite
import com.am.ndo.helper.WriteSaveAsTableOverwrite
import org.apache.spark.sql.functions.upper

import java.time.Period

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

import com.am.ndo.helper.NDOMonad

import grizzled.slf4j.Logging;
import org.apache.spark.sql.Dataset
import javassist.expr.Cast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import scala.collection.immutable.StringOps._


class ControlTotalOperationProfSpend(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator{ 

  
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
					
	val ndoUiPrfsnlSpndSmryQuery = config.getString("query_ui_prfsnl_spnd_smry").replaceAll(ConfigKey.warehouseDBPlaceHolder, warehouseHiveDB).toLowerCase()
	println(s"[NDO-ETL] Query for reading data from ndoUiPrfsnlSpndSmryQuery table is $ndoUiPrfsnlSpndSmryQuery")
	val ndoUiPrfsnlSpndSmryDf = spark.sql(ndoUiPrfsnlSpndSmryQuery)
	
	val mapDF = Map(
							"ui_prfsnl_spnd_smry" -> ndoUiPrfsnlSpndSmryDf)
							

					println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					mapDF

	}
  
  
  def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
			val startTime = DateTime.now
					println(s"[NDO-ETL] Processing Data Started: $startTime")
					//Reading the data frames as elements from Map

					val tabUiPrfsnlSpndSmry = config.getString("tab_ui_prfsnl_spnd_smry")
					val tabApiPrfsnlSpndCntl = config.getString("tab_api_prfsnl_spnd_cntrl")

					val ndoUiPrfsnlSpndSmryDf = inMapDF.getOrElse(tabUiPrfsnlSpndSmry, null)
					
						val map = getDateRange()
					  val rangeEndDate = map.getOrElse("rangeEndDate", null)
					  val rangeStartDate = map.getOrElse("rangeStartDate", null)
					  var toBeInsertedDf : DataFrame = null 
					  
          /*inMapDF.foreach(x => {
						val tableName = x._1
					
								if (tableName.equalsIgnoreCase(tabUiPrfsnlSpndSmry)) {
								  println(s"[NDO-ETL] The table name is $tableName")*/
									val controlInsertDf = ndoUiPrfsnlSpndSmryDf.select($"SNAP_YEAR_MNTH_NBR", $"LOB_ID", $"billg_chrg_amt", $"ALWD_AMT", $"PAID_AMT").
											groupBy($"SNAP_YEAR_MNTH_NBR", $"LOB_ID")
											.agg(sum($"billg_chrg_amt").alias("BILLD_CHRG_AMT"),
													sum($"ALWD_AMT").alias("ALWD_AMT"),
													sum($"PAID_AMT").alias("PAID_AMT")).
											select($"SNAP_YEAR_MNTH_NBR", $"LOB_ID", $"BILLD_CHRG_AMT", $"ALWD_AMT", $"PAID_AMT").withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
						        withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
						        withColumn(lastUpdatedDate, lit(current_timestamp())).
						        withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser))
											
											println(s"Write started for appending the data to final  table $warehouseHiveDB.$tabApiPrfsnlSpndCntl at:" + DateTime.now())
                      controlInsertDf.write.mode("overwrite").insertInto(warehouseHiveDB + """.""" + tabApiPrfsnlSpndCntl)
                      println(s"[NDO-ETL] Table loaded as $warehouseHiveDB.$tabApiPrfsnlSpndCntl")
											
	//							}})
								
								println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					null
	}

	
def writeData(map: Map[String, DataFrame]): Unit = {
			val startTime = DateTime.now()
					//Writing the data to a table in Hive
					println(s"[HPIP-ETL] Writing Dataframes to Hive started at: $startTime")

					println(s"[HPIP-ETL] writing the data to target tables is Completed at: " + DateTime.now())
					println(s"[HPIP-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

	}
	def getDateRange(): Map[String, String] =
		{
				val calender = Calendar.getInstance()
				val formatter2 = new SimpleDateFormat("MM", Locale.ENGLISH)
				val formatter3 = new SimpleDateFormat("yyyy", Locale.ENGLISH)

				val monthUF = calender.getTime()
				val month = formatter2.format(monthUF).toInt

						var priorEndNotFormatted = calender.getTime()
						var rangeStartDate = ""
								var rangeEndDate = ""

								if (month == 1 || month == 2 || month == 3) {
									calender.add(Calendar.YEAR, -2)

									priorEndNotFormatted = calender.getTime()
									rangeStartDate = formatter3.format(priorEndNotFormatted) + "01"
											println("The range start date is " + rangeStartDate)
									rangeEndDate = formatter3.format(priorEndNotFormatted) + "12"
									println("The range end date is" + rangeEndDate)

								} else {
									calender.add(Calendar.YEAR, -1)
									priorEndNotFormatted = calender.getTime()
									rangeStartDate = formatter3.format(priorEndNotFormatted) + "01"
											println("The range start date is " + rangeStartDate)
									rangeEndDate = formatter3.format(priorEndNotFormatted) + "12"
									println("The range end date is" + rangeEndDate)
								}

						val map = Map(
								"rangeStartDate" -> rangeStartDate,
								"rangeEndDate" -> rangeEndDate)

								map

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