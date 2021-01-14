package com.am.ndo.api.apiQltGroup

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

class ApiQltGroupOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

	import spark.implicits._

	//Audit
	var program = ""
	var user_id = ""
	var app_id = ""
	var start_time: DateTime = DateTime.now()
	var start = ""

	var listBuffer = ListBuffer[Audit]()
	val tabApiQltGroup = config.getString("tab_api_qlt_group")
	val tabApiQltGroupBkp = config.getString("tab_api_qlt_group_bkp")

	def loadData(): Map[String, DataFrame] = {

			//Reading the data into Data frames
			val startTime = DateTime.now
					println(s"[NDO-ETL] The loading of Data started with start time at :  + $startTime")

					//Reading the queries from config file
					println("Reading the queries from config file")

					val pcrGrpEfcncyRatioMultiSpltQuery = config.getString("api_query_pcr_grp_efcncy_ratio_multi_splt").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val pcrGrpEfcncyRatioQuery = config.getString("api_query_pcr_grp_efcncy_ratio").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val pcrElgblProvQuery = config.getString("api_query_pcr_elgbl_prov").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val qltyNrwNtwkTaxSpcltyQuery = config.getString("api_query_qlty_nrw_ntwk_tax_spclty").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val qltyNrwNtwkNpiQuery = config.getString("api_query_qlty_nrw_ntwk_npi").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val apiProvWrkQuery = config.getString("api_query_api_prov_wrk").replaceAll(ConfigKey.warehouseDBPlaceHolder, warehouseHiveDB).toLowerCase()

					println(s"[NDO-ETL] Query for reading data from pcr_grp_efcncy_ratio_multi_splt table is $pcrGrpEfcncyRatioMultiSpltQuery")
					println(s"[NDO-ETL] Query for reading data from pcr_grp_efcncy_ratio table is $pcrGrpEfcncyRatioQuery")
					println(s"[NDO-ETL] Query for reading data from pcr_elgbl_prov table is $pcrElgblProvQuery")
					println(s"[NDO-ETL] Query for reading data from qlty_nrw_ntwk_tax_spclty table is $qltyNrwNtwkTaxSpcltyQuery")
					println(s"[NDO-ETL] Query for reading data from qlty_nrw_ntwk_npi table is $qltyNrwNtwkNpiQuery")
					println(s"[NDO-ETL] Query for reading data from api_prov_wrk table is $apiProvWrkQuery")

					val pcrGrpEfcncyRatioMultiSpltDf = spark.sql(pcrGrpEfcncyRatioMultiSpltQuery)
					val pcrGrpEfcncyRatioDf = spark.sql(pcrGrpEfcncyRatioQuery)
					val pcrElgblProvDf = spark.sql(pcrElgblProvQuery)
					val qltyNrwNtwkTaxSpcltyDf = spark.sql(qltyNrwNtwkTaxSpcltyQuery)
					val qltyNrwNtwkNpiDf = spark.sql(qltyNrwNtwkNpiQuery)
					val apiProvWrkDf = spark.sql(apiProvWrkQuery)

					val mapDF = Map("pcr_grp_efcncy_ratio_multi_splt" -> pcrGrpEfcncyRatioMultiSpltDf, "pcr_grp_efcncy_ratio" -> pcrGrpEfcncyRatioDf, "pcr_elgbl_prov" -> pcrElgblProvDf,
							"qlty_nrw_ntwk_tax_spclty" -> qltyNrwNtwkTaxSpcltyDf, "qlty_nrw_ntwk_npi" -> qltyNrwNtwkNpiDf, "api_prov_wrk" -> apiProvWrkDf)

					println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					mapDF
	}

	def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
			val startTime = DateTime.now
					println(s"[NDO-ETL] Processing Data Started: $startTime")

					//Reading the data frames as elements from Map
					val qltyNrwNtwkTaxSpcltyDf = inMapDF.getOrElse("qlty_nrw_ntwk_tax_spclty", null)
					val qltyNrwNtwkNpiDf = inMapDF.getOrElse("qlty_nrw_ntwk_npi", null)
					val apiProvWrkDf = inMapDF.getOrElse("api_prov_wrk", null)

					val maxrun_id = getMAxRunId(tabApiQltGroup)
					println(s"[NDO-ETL] Max run id is : $maxrun_id")

					val snapNbr = getSnapNbr(inMapDF)
					println(s"[NDO-ETL] Available snapNbr is : $snapNbr")

					/* TIN level quality data */
					val qltyNrwNtwkTaxSpcltyDfFilter = qltyNrwNtwkTaxSpcltyDf.filter(qltyNrwNtwkTaxSpcltyDf("SNAP_YEAR_MNTH_NBR") === snapNbr &&
					trim(qltyNrwNtwkTaxSpcltyDf("SCRBL_IND_CD")) === "Y" &&
					trim(qltyNrwNtwkTaxSpcltyDf("PEER_MRKT_CD")) === "Q2")

					val apiProvWrkDfDist1 = apiProvWrkDf.select($"BKBN_SNAP_YEAR_MNTH_NBR",
							$"ST_CD",
							$"LOB_ID",
							$"PROD_ID",
							$"PROV_TAX_ID",
							$"SPCLTY_ID",
							$"AGRGTN_TYPE_CD",
							$"SUBMRKT_CD",
							$"PEER_MRKT_CD",
							$"PROV_CNTY_NM").distinct

					val apiProvWrkqltyNrwNtwkTaxSpcltyJoin = qltyNrwNtwkTaxSpcltyDfFilter.join(apiProvWrkDfDist1,
							trim(qltyNrwNtwkTaxSpcltyDfFilter("TAX_ID")) === trim(apiProvWrkDfDist1("PROV_TAX_ID")) &&
							trim(apiProvWrkDfDist1("AGRGTN_TYPE_CD")) === "TIN" &&
							trim(apiProvWrkDfDist1("BKBN_SNAP_YEAR_MNTH_NBR")) === trim(qltyNrwNtwkTaxSpcltyDfFilter("SNAP_YEAR_MNTH_NBR")), "inner")
					.select(qltyNrwNtwkTaxSpcltyDfFilter("SNAP_YEAR_MNTH_NBR").alias("SNAP_YEAR_MNTH_NBR"),
							apiProvWrkDfDist1("ST_CD").alias("ST_CD"),
							apiProvWrkDfDist1("LOB_ID").alias("LOB_ID"),
							apiProvWrkDfDist1("PROD_ID").alias("PROD_ID"),
							qltyNrwNtwkTaxSpcltyDfFilter("TAX_ID").alias("AGRGTN_ID"),
							lit("TIN").alias("AGRGTN_TYPE_CD"),
							apiProvWrkDfDist1("SPCLTY_ID").alias("SPCLTY_ID"),
							apiProvWrkDfDist1("SUBMRKT_CD").alias("SUBMRKT_CD"),
							apiProvWrkDfDist1("PROV_CNTY_NM").alias("PROV_CNTY_NM"),
							qltyNrwNtwkTaxSpcltyDfFilter("OE_QLTY_RT").alias("OE_QLTY_RT"),
							qltyNrwNtwkTaxSpcltyDfFilter("CNFDNC_INTRVL_LOWR_NBR").alias("LOWR_CNFDNC_LVL_NBR"),
							qltyNrwNtwkTaxSpcltyDfFilter("CNFDNC_INTRVL_UPR_NBR").alias("UPR_CNFDNC_LVL_NBR")).distinct()

					/* NPI level quality data */
					val qltyNrwNtwkNpiDfFilter = qltyNrwNtwkNpiDf.filter(qltyNrwNtwkNpiDf("SNAP_YEAR_MNTH_NBR") === snapNbr &&
					trim(qltyNrwNtwkNpiDf("SCRBL_IND_CD")) === "Y" &&
					trim(qltyNrwNtwkNpiDf("PEER_MRKT_CD")) === "Q2")

					val apiProvWrkDfDist2 = apiProvWrkDf.select($"BKBN_SNAP_YEAR_MNTH_NBR",
							$"ST_CD",
							$"LOB_ID",
							$"PROD_ID",
							$"NPI",
							$"SPCLTY_ID",
							$"AGRGTN_TYPE_CD",
							$"SUBMRKT_CD",
							$"PEER_MRKT_CD").distinct

					val apiProvWrkqltyNrwNtwkNpiJoin = qltyNrwNtwkNpiDfFilter.join(apiProvWrkDfDist2,
							trim(qltyNrwNtwkNpiDfFilter("NPI")) === trim(apiProvWrkDfDist2("NPI")) &&
							trim(apiProvWrkDfDist2("AGRGTN_TYPE_CD")) === "NPI" &&
							trim(apiProvWrkDfDist2("BKBN_SNAP_YEAR_MNTH_NBR")) === trim(qltyNrwNtwkNpiDfFilter("SNAP_YEAR_MNTH_NBR")), "inner").select(qltyNrwNtwkNpiDfFilter("SNAP_YEAR_MNTH_NBR").alias("SNAP_YEAR_MNTH_NBR"),
									apiProvWrkDfDist2("ST_CD").alias("ST_CD"),
									apiProvWrkDfDist2("LOB_ID").alias("LOB_ID"),
									apiProvWrkDfDist2("PROD_ID").alias("PROD_ID"),
									qltyNrwNtwkNpiDfFilter("NPI").alias("AGRGTN_ID"),
									lit("NPI").alias("AGRGTN_TYPE_CD"),
									apiProvWrkDfDist2("SPCLTY_ID").alias("SPCLTY_ID"),
									apiProvWrkDfDist2("SUBMRKT_CD").alias("SUBMRKT_CD"),
									lit("NA").alias("PROV_CNTY_NM"),
									qltyNrwNtwkNpiDfFilter("OE_QLTY_RT").alias("OE_QLTY_RT"),
									qltyNrwNtwkNpiDfFilter("CNFDNC_INTRVL_LOWR_NBR").alias("LOWR_CNFDNC_LVL_NBR"),
									qltyNrwNtwkNpiDfFilter("CNFDNC_INTRVL_UPR_NBR").alias("UPR_CNFDNC_LVL_NBR")).distinct()

					val apiqltGroupFinalJoin = apiProvWrkqltyNrwNtwkTaxSpcltyJoin.union(apiProvWrkqltyNrwNtwkNpiJoin).distinct()

					val apiQltGroupDf = apiqltGroupFinalJoin.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
					withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
					withColumn(lastUpdatedDate, lit(current_timestamp())).
					withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).
					withColumn("run_id", lit(maxrun_id))

					writeTblInsertOverwrite(apiQltGroupDf, warehouseHiveDB, tabApiQltGroup)
					writeTblAppendOverwrite(apiQltGroupDf, warehouseHiveDB, tabApiQltGroupBkp)

					runIDInsert(tabApiQltGroup, maxrun_id)

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

	def getSnapNbr(inMapDF: Map[String, DataFrame]): Long = {
			val pcrGrpEfcncyRatioDf = inMapDF.getOrElse("pcr_grp_efcncy_ratio", null).select($"BKBN_SNAP_YEAR_MNTH_NBR".alias("SNAP_YEAR_MNTH_NBR")).distinct()
					val qltyNrwNtwkTaxSpcltyDf = inMapDF.getOrElse("qlty_nrw_ntwk_tax_spclty", null).select($"SNAP_YEAR_MNTH_NBR").distinct()
					val pcrGrpEfcncyRatioMultiSpltDf = inMapDF.getOrElse("pcr_grp_efcncy_ratio_multi_splt", null).select($"BKBN_SNAP_YEAR_MNTH_NBR".alias("SNAP_YEAR_MNTH_NBR")).distinct()
					val pcrElgblProvDf = inMapDF.getOrElse("pcr_elgbl_prov", null).select($"BKBN_SNAP_YEAR_MNTH_NBR").orderBy($"BKBN_SNAP_YEAR_MNTH_NBR".desc).limit(1)
					val qltyNrwNtwkNpiDf = inMapDF.getOrElse("qlty_nrw_ntwk_npi", null).select($"SNAP_YEAR_MNTH_NBR").distinct()

					val startTime = DateTime.now()
					//Writing the data to a table in Hive
					println(s"[HPIP-ETL] snapNbr derivation started at : $startTime")
					val snapNbr = pcrElgblProvDf.join(pcrGrpEfcncyRatioMultiSpltDf, pcrElgblProvDf("BKBN_SNAP_YEAR_MNTH_NBR") === pcrGrpEfcncyRatioMultiSpltDf("SNAP_YEAR_MNTH_NBR"))
					.join(pcrGrpEfcncyRatioDf, Seq("SNAP_YEAR_MNTH_NBR"))
					.join(qltyNrwNtwkTaxSpcltyDf, Seq("SNAP_YEAR_MNTH_NBR"))
					.join(qltyNrwNtwkNpiDf, Seq("SNAP_YEAR_MNTH_NBR"))
					.orderBy($"SNAP_YEAR_MNTH_NBR".desc).head.getLong(0)

					println(s"[NDO-ETL] latest BKBN_SNAP_YEAR_MNTH_NBR available in all the tables is $snapNbr")
					println(s"[NDO-ETL]  BKBN_SNAP_YEAR_MNTH_NBR derivation Completed at: " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for derivation :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")
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
