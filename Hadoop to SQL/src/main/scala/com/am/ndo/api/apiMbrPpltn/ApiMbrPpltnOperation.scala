package com.am.ndo.api.apiMbrPpltn

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
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

class ApiMbrPpltnOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

	import spark.implicits._

	//Audit
	var program = ""
	var user_id = ""
	var app_id = ""
	var start_time: DateTime = DateTime.now()
	var start = ""

	var listBuffer = ListBuffer[Audit]()
	val tabApiMbrPpltn = config.getString("tab_api_mbr_ppltn")
	val tabApiMbrPpltnBkp = config.getString("tab_api_mbr_ppltn_bkp")

	def loadData(): Map[String, DataFrame] = {

			//Reading the data into Data frames
			val startTime = DateTime.now
					println(s"[NDO-ETL] The loading of Data started with start time at :  + $startTime")

					//Reading the queries from config file
					println("Reading the queries from config file")

					val rsttdMbrshpMnthCntQuery = config.getString("api_mbr_query_rsttd_mbrshp_mnth_cnt").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val mbrAdrsQuery = config.getString("api_mbr_query_mbr_adrs").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val botPcrFnclMbuCfQuery = config.getString("api_mbr_query_bot_pcr_fncl_mbu_cf").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val botPcrFnclProdCfQuery = config.getString("api_mbr_query_bot_pcr_fncl_prod_cf").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val fnclProdCfQuery = config.getString("api_mbr_query_fncl_prod_cf").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val botSubmrktQuery = config.getString("api_mbr_query_bot_submrkt").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()

					println(s"[NDO-ETL] Query for reading data from rsttd_mbrshp_mnth_cnt table is $rsttdMbrshpMnthCntQuery")
					println(s"[NDO-ETL] Query for reading data from mbr_adrs table is $mbrAdrsQuery")
					println(s"[NDO-ETL] Query for reading data from bot_pcr_fncl_mbu_cf table is $botPcrFnclMbuCfQuery")
					println(s"[NDO-ETL] Query for reading data from bot_pcr_fncl_prod_cf table is $botPcrFnclProdCfQuery")
					println(s"[NDO-ETL] Query for reading data from fncl_prod_cf table is $fnclProdCfQuery")
					println(s"[NDO-ETL] Query for reading data from bot_submrkt table is $botSubmrktQuery")

					val rsttdMbrshpMnthCntDf = spark.sql(rsttdMbrshpMnthCntQuery)
					val mbrAdrsDf = spark.sql(mbrAdrsQuery)
					val botPcrFnclMbuCfDf = spark.sql(botPcrFnclMbuCfQuery)
					val botPcrFnclProdCfDf = spark.sql(botPcrFnclProdCfQuery)
					val fnclProdCfDf = spark.sql(fnclProdCfQuery)
					val botSubmrktDf = spark.sql(botSubmrktQuery)

					val mapDF = Map("rsttd_mbrshp_mnth_cnt" -> rsttdMbrshpMnthCntDf, "mbr_adrs" -> mbrAdrsDf, "bot_pcr_fncl_mbu_cf" -> botPcrFnclMbuCfDf,
							"bot_pcr_fncl_prod_cf" -> botPcrFnclProdCfDf, "fncl_prod_cf" -> fnclProdCfDf, "bot_submrkt" -> botSubmrktDf)

					println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					mapDF
	}

	def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
			val startTime = DateTime.now
					println(s"[NDO-ETL] Processing Data Started: $startTime")

					//Reading the data frames as elements from Map
					val rsttdMbrshpMnthCntDf = inMapDF.getOrElse("rsttd_mbrshp_mnth_cnt", null)
					val mbrAdrsDf = inMapDF.getOrElse("mbr_adrs", null)
					val botPcrFnclMbuCfDf = inMapDF.getOrElse("bot_pcr_fncl_mbu_cf", null)
					val botPcrFnclProdCfDf = inMapDF.getOrElse("bot_pcr_fncl_prod_cf", null)
					val fnclProdCfDf = inMapDF.getOrElse("fncl_prod_cf", null)
					val botSubmrktDf = inMapDF.getOrElse("bot_submrkt", null)

					val maxrun_id = getMAxRunId(tabApiMbrPpltn)
					println(s"[NDO-ETL] Max run id is : $maxrun_id")

					val requiredDate = getDate()
					println(s"[NDO-ETL] last date is : $requiredDate")

					val rsttdMbrJoin = rsttdMbrshpMnthCntDf.join(mbrAdrsDf,
							((when(trim(mbrAdrsDf("MBRSHP_SOR_CD")).isin("199", "202", "808", "809", "815", "822", "823", "868", "886", "888", "877", "891", "896", "919", "994", "1104"), lit("13"))
									when (trim(mbrAdrsDf("MBRSHP_SOR_CD")).isin("824", "867", "878"), lit("2")) otherwise (lit("13"))))
							=== trim(mbrAdrsDf("ADRS_TYPE_CD")) &&
							trim(rsttdMbrshpMnthCntDf("MBR_KEY")) === trim(mbrAdrsDf("MBR_KEY")), "left")
					.join(botPcrFnclMbuCfDf, trim(rsttdMbrshpMnthCntDf("MBU_CF_CD")) === trim(botPcrFnclMbuCfDf("MBU_CF_CD")), "left_outer")
					.join(botPcrFnclProdCfDf, trim(rsttdMbrshpMnthCntDf("PROD_CF_CD")) === trim(botPcrFnclProdCfDf("PROD_CF_CD")), "left_outer")
					.join(fnclProdCfDf, trim(rsttdMbrshpMnthCntDf("PROD_CF_CD")) === trim(fnclProdCfDf("PROD_CF_CD")), "left_outer")
					.join(botSubmrktDf, trim(mbrAdrsDf("ZIP_CD")) === trim(botSubmrktDf("ADRS_POSTL_CD")), "left_outer")
					.filter($"ELGBLTY_CLNDR_MNTH_END_DT" === requiredDate && trim($"RCRD_STTS_CD") =!= "DEL" && trim($"PROD_LVL_1_DESC") === "MEDICAL")

					val rsttdMbrJoinFilter1 = rsttdMbrJoin.filter(trim($"ST_PRVNC_CD") === "CA" && trim($"PCR_PROD_DESC") === "PPO" && trim($"PCR_LOB_DESC") === "COMMERCIAL")
					.withColumn("RUN_ID", lit(maxrun_id))
					.withColumn("SNAP_YEAR_MNTH_NBR", ((from_unixtime(unix_timestamp(col("ELGBLTY_CLNDR_MNTH_END_DT"), "YYYY-MM-DD HH:MM:SS"), "yyyyMM")).cast("String")).cast("Int"))
					.withColumn("LOB", when(trim($"PCR_LOB_DESC") === "MEDICARE", "MEDICARE ADVANTAGE") otherwise trim(($"PCR_LOB_DESC")))
					.select(
							$"RUN_ID",
							$"SNAP_YEAR_MNTH_NBR",
							$"ST_PRVNC_CD".alias("ST_CD"),
							(when(trim($"PCR_LOB_DESC") === "MEDICARE", "MEDICARE ADVANTAGE") otherwise trim($"PCR_LOB_DESC")).alias("LOB_ID"),
							(when(trim($"ST_PRVNC_CD") === "CA" && trim($"PCR_PROD_DESC") === "PPO" && $"LOB" === "COMMERCIAL", "PPO SELECT") otherwise trim($"PCR_PROD_DESC")).alias("PROD_ID"),
							$"SUBMRKT_DESC",
							$"SUBMRKT_CD",
							$"ZIP_CD".alias("ZIP"),
							$"MDCL_EXPSR_NBR")
					.groupBy(
							$"RUN_ID",
							$"SNAP_YEAR_MNTH_NBR",
							$"ST_CD",
							$"LOB_ID",
							$"PROD_ID",
							$"SUBMRKT_DESC",
							$"SUBMRKT_CD",
							$"ZIP").agg(sum($"MDCL_EXPSR_NBR").cast("Int").alias("MBR_PPLTN_CNT"))

					val rsttdMbrJoinFilter2 = rsttdMbrJoin.filter(((trim($"ST_PRVNC_CD").isin("CA", "CO", "CT", "FL", "GA", "IA", "IN", "KS", "KY", "LA", "MD", "ME", "MO", "NH", "NJ", "NM", "NV", "NY", "OH", "SC", "TN", "TX", "VA", "WA", "WI", "WV"))) &&
							(trim($"PCR_PROD_DESC").isin("OTHER", "EXCLUSION") === false) && (trim($"PCR_LOB_DESC") =!= "OTHER"))
					.withColumn("RUN_ID", lit(maxrun_id))
					.withColumn("SNAP_YEAR_MNTH_NBR", ((from_unixtime(unix_timestamp(col("ELGBLTY_CLNDR_MNTH_END_DT"), "YYYY-MM-DD HH:MM:SS"), "yyyyMM")).cast("String")).cast("Int"))
					.select(
							$"RUN_ID",
							$"SNAP_YEAR_MNTH_NBR",
							$"ST_PRVNC_CD".alias("ST_CD"),
							(when(trim($"PCR_LOB_DESC") === "MEDICARE", "MEDICARE ADVANTAGE") otherwise trim($"PCR_LOB_DESC")).alias("LOB_ID"),
							$"PCR_PROD_DESC".alias("PROD_ID"),
							$"SUBMRKT_DESC",
							$"SUBMRKT_CD",
							$"ZIP_CD".alias("ZIP"),
							$"MDCL_EXPSR_NBR")
					.groupBy(
							$"RUN_ID",
							$"SNAP_YEAR_MNTH_NBR",
							$"ST_CD",
							$"LOB_ID",
							$"PROD_ID",
							$"SUBMRKT_DESC",
							$"SUBMRKT_CD",
							$"ZIP").agg(sum($"MDCL_EXPSR_NBR").cast("Int").alias("MBR_PPLTN_CNT"))

					val rsttdMbrJoinFilter3 = rsttdMbrJoin.filter(((trim($"ST_PRVNC_CD").isin("CA", "CO", "CT", "FL", "GA", "IA", "IN", "KS", "KY", "LA", "MD", "ME", "MO", "NH", "NJ", "NM", "NV", "NY", "OH", "SC", "TN", "TX", "VA", "WA", "WI", "WV"))) &&
							(trim($"PCR_PROD_DESC").isin("OTHER", "EXCLUSION") === false) && (trim($"PCR_LOB_DESC") =!= "OTHER"))
					.withColumn("RUN_ID", lit(maxrun_id))
					.withColumn("SNAP_YEAR_MNTH_NBR", ((from_unixtime(unix_timestamp(col("ELGBLTY_CLNDR_MNTH_END_DT"), "YYYY-MM-DD HH:MM:SS"), "yyyyMM")).cast("String")).cast("Int"))
					.select(
							$"RUN_ID",
							$"SNAP_YEAR_MNTH_NBR",
							$"ST_PRVNC_CD".alias("ST_CD"),
							(when($"PCR_LOB_DESC" === "MEDICARE", "MEDICARE ADVANTAGE") otherwise ($"PCR_LOB_DESC")).alias("LOB_ID"),
							lit("ALL").alias("PROD_ID"),
							$"SUBMRKT_DESC",
							$"SUBMRKT_CD",
							$"ZIP_CD".alias("ZIP"),
							$"MDCL_EXPSR_NBR")
					.groupBy(
							$"RUN_ID",
							$"SNAP_YEAR_MNTH_NBR",
							$"ST_CD",
							$"LOB_ID",
							$"PROD_ID",
							$"SUBMRKT_DESC",
							$"SUBMRKT_CD",
							$"ZIP").agg(sum($"MDCL_EXPSR_NBR").cast("Int").alias("MBR_PPLTN_CNT"))

					val rsttdMbrJoinFilterUnion = ((rsttdMbrJoinFilter1.union(rsttdMbrJoinFilter2)).distinct.union(rsttdMbrJoinFilter3)).distinct()
					.select(
							$"RUN_ID",
							$"SNAP_YEAR_MNTH_NBR",
							coalesce($"ST_CD", lit("NA")).alias("ST_CD"),
							coalesce($"LOB_ID", lit("NA")).alias("LOB_ID"),
							coalesce($"PROD_ID", lit("NA")).alias("PROD_ID"),
							$"SUBMRKT_DESC",
							coalesce($"SUBMRKT_CD", lit("NA")).alias("SUBMRKT_CD"),
							coalesce($"ZIP", lit("NA")).alias("ZIP"),
							$"MBR_PPLTN_CNT").filter(trim($"ZIP").isin("NA", "") === false)
					.withColumn("RCRD_CREATN_DTM", lit(current_timestamp()))
					.withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser))
					.withColumn(lastUpdatedDate, lit(current_timestamp()))
					.withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser))

					writeTblInsertOverwrite(rsttdMbrJoinFilterUnion, warehouseHiveDB, tabApiMbrPpltn)
					writeTblAppendOverwrite(rsttdMbrJoinFilterUnion, warehouseHiveDB, tabApiMbrPpltnBkp)

					runIDInsert(tabApiMbrPpltn, maxrun_id)

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

	def writeTblInsertOverwrite(df: DataFrame, hiveDB: String, tableName: String) {
		println(s"[NDO-ETL] Write started for table $tableName at:" + DateTime.now())
		df.write.mode("overwrite").insertInto(hiveDB + """.""" + tableName)
		println(s"[NDO-ETL] Table created as $hiveDB.$tableName")
	}

	def writeTblAppendOverwrite(df: DataFrame, hiveDB: String, tableName: String) {
		println(s"[NDO-ETL] Write started for table $tableName at:" + DateTime.now())
		df.write.mode("append").insertInto(hiveDB + """.""" + tableName)
		println(s"[NDO-ETL] Table created as $hiveDB.$tableName")
	}

	def getDate(): String =
		{
				val calender = Calendar.getInstance()
						val formatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH)
						val fomatter1 = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH)

						val DateUF = calender.getTime()
						val Date = formatter.format(DateUF)

						println("Current Date is : " + Date)

						calender.add(Calendar.MONTH, -4)
						val previousDateUnFormatted = calender.getTime()
						val previousDate = formatter.format(previousDateUnFormatted)
						println("fourth previous month Date" + previousDate)

						val LastDay = calender.getActualMaximum(Calendar.DAY_OF_MONTH)
						val requiredDate = fomatter1.format(previousDateUnFormatted) + "-" + LastDay

						println("last available date for the fourth previous month is : " + requiredDate)
						requiredDate
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
