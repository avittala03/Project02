package com.am.ndo.api.apiEtgGroupSpclty

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

class ApiEtgGroupSpcltyOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

	import spark.implicits._

	//Audit
	var program = ""
	var user_id = ""
	var app_id = ""
	var start_time: DateTime = DateTime.now()
	var start = ""

	var listBuffer = ListBuffer[Audit]()
	val tabApiEtgGroupSpclty = config.getString("tab_api_etg_group_spclty")
	val tabApiEtgGroupSpcltyBkp = config.getString("tab_api_etg_group_spclty_bkp")

	def loadData(): Map[String, DataFrame] = {

			//Reading the data into Data frames
			val startTime = DateTime.now
					println(s"[NDO-ETL] The loading of Data started with start time at :  + $startTime")

					//Reading the queries from config file
					println("Reading the queries from config file")

					val pcrGrpEfcncyRatioMultiSpltQuery = config.getString("api_query_pcr_grp_efcncy_ratio_multi_splt").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val pcrGrpEfcncyRatioQuery = config.getString("api_query_pcr_grp_efcncy_ratio").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val qltyNrwNtwkTaxSpcltyQuery = config.getString("api_query_qlty_nrw_ntwk_tax_spclty").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val pcrElgblProvQuery = config.getString("api_query_pcr_elgbl_prov").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val apiProvWrkQuery = config.getString("api_query_api_prov_wrk").replaceAll(ConfigKey.warehouseDBPlaceHolder, warehouseHiveDB).toLowerCase()

					println(s"[NDO-ETL] Query for reading data from pcr_grp_efcncy_ratio_multi_splt table is $pcrGrpEfcncyRatioMultiSpltQuery")
					println(s"[NDO-ETL] Query for reading data from pcr_grp_efcncy_ratio table is $pcrGrpEfcncyRatioQuery")
					println(s"[NDO-ETL] Query for reading data from qlty_nrw_ntwk_tax_spclty table is $qltyNrwNtwkTaxSpcltyQuery")
					println(s"[NDO-ETL] Query for reading data from pcr_elgbl_prov table is $pcrElgblProvQuery")
					println(s"[NDO-ETL] Query for reading data from api_prov_wrk table is $apiProvWrkQuery")

					val pcrGrpEfcncyRatioMultiSpltDf = spark.sql(pcrGrpEfcncyRatioMultiSpltQuery)
					val pcrGrpEfcncyRatioDf = spark.sql(pcrGrpEfcncyRatioQuery)
					val qltyNrwNtwkTaxSpcltyDf = spark.sql(qltyNrwNtwkTaxSpcltyQuery)
					val pcrElgblProvDf = spark.sql(pcrElgblProvQuery)
					val apiProvWrkDf = spark.sql(apiProvWrkQuery)

					val mapDF = Map("pcr_grp_efcncy_ratio_multi_splt" -> pcrGrpEfcncyRatioMultiSpltDf, "pcr_grp_efcncy_ratio" -> pcrGrpEfcncyRatioDf, "qlty_nrw_ntwk_tax_spclty" -> qltyNrwNtwkTaxSpcltyDf,
							"pcr_elgbl_prov" -> pcrElgblProvDf, "api_prov_wrk" -> apiProvWrkDf)

					println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					mapDF
	}

	def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
			val startTime = DateTime.now
					println(s"[NDO-ETL] Processing Data Started: $startTime")

					//Reading the data frames as elements from Map
					val pcrGrpEfcncyRatioDf = inMapDF.getOrElse("pcr_grp_efcncy_ratio", null)
					val apiProvWrkDf = inMapDF.getOrElse("api_prov_wrk", null)

					val maxrun_id = getMAxRunId(tabApiEtgGroupSpclty)
					println(s"[NDO-ETL] Max run id is : $maxrun_id")

					val snapNbr = getSnapNbr(inMapDF)
					println(s"[NDO-ETL] Available snapNbr is : $snapNbr")

					val pcrGrpEfcncyRatioDfFilter = pcrGrpEfcncyRatioDf.filter(trim(pcrGrpEfcncyRatioDf("NTWK_ST_CD")).isin("UNK", "NA") === false &&
					pcrGrpEfcncyRatioDf("BKBN_SNAP_YEAR_MNTH_NBR") === snapNbr &&
					pcrGrpEfcncyRatioDf("GRP_DSTNCT_EPSD_CNT") >= 20 &&
					((trim(pcrGrpEfcncyRatioDf("NTWK_ST_CD")).isin("CA", "NY", "VA") && trim(pcrGrpEfcncyRatioDf("PEER_MRKT_CD")).isin("8", "33"))
							|| (trim(pcrGrpEfcncyRatioDf("NTWK_ST_CD")).isin("CA", "NY", "VA") === false && trim(pcrGrpEfcncyRatioDf("PEER_MRKT_CD")).isin("4", "32"))
							|| (trim(pcrGrpEfcncyRatioDf("NTWK_ST_CD")) === "NY" and trim(pcrGrpEfcncyRatioDf("PEER_MRKT_CD")).isin("3", "34"))))

					/* apiProvWrkDfDist - df4 */
					val apiProvWrkDfDist = apiProvWrkDf.select($"ST_CD",
							$"LOB_ID",
							$"PROD_ID",
							$"PROV_TAX_ID",
							$"NPI",
							$"SPCLTY_ID",
							$"SUBMRKT_CD",
							$"AGRGTN_TYPE_CD",
							$"PRIMARY_SPECIALITY_CODE",
							$"PEER_MRKT_CD",
							$"RPTG_NTWK_DESC",
							$"PROV_CNTY_NM").distinct

					val apiProvWrkPcrGrpEfcncyRatioJoin = pcrGrpEfcncyRatioDfFilter.join(apiProvWrkDfDist,
							trim(pcrGrpEfcncyRatioDfFilter("GRP_AGRGTN_TYPE_CD")) === trim(apiProvWrkDfDist("AGRGTN_TYPE_CD")) &&
							trim(pcrGrpEfcncyRatioDfFilter("GRP_AGRGTN_ID")) === (when(trim(apiProvWrkDfDist("AGRGTN_TYPE_CD")) === "TIN", trim(apiProvWrkDfDist("PROV_TAX_ID")))
									when (trim(apiProvWrkDfDist("AGRGTN_TYPE_CD")) === "NPI", trim(apiProvWrkDfDist("NPI")))
									otherwise lit("0")) &&
							trim(pcrGrpEfcncyRatioDfFilter("NTWK_ST_CD")) === trim(apiProvWrkDfDist("ST_CD")) &&
							trim(pcrGrpEfcncyRatioDfFilter("PEER_MRKT_CD")) === trim(apiProvWrkDfDist("PEER_MRKT_CD")) &&
							trim(apiProvWrkDfDist("LOB_ID")) === (when(trim(pcrGrpEfcncyRatioDfFilter("PCR_LOB_DESC")) === "MEDICARE", lit("MEDICARE ADVANTAGE"))
									otherwise trim(pcrGrpEfcncyRatioDfFilter("PCR_LOB_DESC"))) &&
							trim(pcrGrpEfcncyRatioDfFilter("PRMRY_SUBMRKT_CD")) === trim(apiProvWrkDfDist("SUBMRKT_CD")) &&
							trim(pcrGrpEfcncyRatioDfFilter("SPCLTY_PRMRY_CD")) === trim(apiProvWrkDfDist("PRIMARY_SPECIALITY_CODE")) &&
							((trim(apiProvWrkDfDist("PEER_MRKT_CD")).isin("32", "33", "34") && trim(apiProvWrkDfDist("RPTG_NTWK_DESC")) === trim(pcrGrpEfcncyRatioDfFilter("BNCHMRK_PROD_DESC")))
									|| (trim(apiProvWrkDfDist("PEER_MRKT_CD")).isin("3", "4", "8") &&
											trim(apiProvWrkDfDist("prod_id")) === (when(trim(pcrGrpEfcncyRatioDfFilter("BNCHMRK_PROD_DESC")) === "NO GROUPING", lit("ALL"))
													otherwise trim(pcrGrpEfcncyRatioDfFilter("BNCHMRK_PROD_DESC"))))), "inner")
					.select(pcrGrpEfcncyRatioDfFilter("BKBN_SNAP_YEAR_MNTH_NBR"),
							pcrGrpEfcncyRatioDfFilter("NTWK_ST_CD").alias("ST_CD"),
							(when(trim(pcrGrpEfcncyRatioDfFilter("PCR_LOB_DESC")) === "MEDICARE", lit("MEDICARE ADVANTAGE")) otherwise trim(pcrGrpEfcncyRatioDfFilter("PCR_LOB_DESC"))).alias("LOB_ID"),
							apiProvWrkDfDist("PROD_ID"),
							pcrGrpEfcncyRatioDfFilter("GRP_AGRGTN_ID").alias("AGRGTN_ID"),
							pcrGrpEfcncyRatioDfFilter("GRP_AGRGTN_TYPE_CD").alias("AGRGTN_TYPE_CD"),
							apiProvWrkDfDist("SPCLTY_ID").alias("SPCLTY_ID"),
							pcrGrpEfcncyRatioDfFilter("PRMRY_SUBMRKT_CD").alias("SUBMRKT_CD"),
							apiProvWrkDfDist("PROV_CNTY_NM").alias("PROV_CNTY_NM"),
							(when(apiProvWrkDfDist("PEER_MRKT_CD").isin("3", "4", "8"), lit("NONE"))
									when (apiProvWrkDfDist("PEER_MRKT_CD").isin("32", "33", "34"), pcrGrpEfcncyRatioDfFilter("BNCHMRK_PROD_DESC"))
									otherwise lit("NOTF")).alias("RPTG_NTWK_DESC"),
							pcrGrpEfcncyRatioDfFilter("GRP_DSTNCT_EPSD_CNT").alias("GRP_EPSD_VOL_NBR"),
							pcrGrpEfcncyRatioDfFilter("GRP_SCRBL_AVG_EPSD_OE_VOL_WGTD_NBR").alias("GRP_VOL_WGTD_MEAN_ETG_INDX_NBR"),
							pcrGrpEfcncyRatioDfFilter("GRP_OE_LOWR_CNFDNC_LVL_VOL_WGTD_NBR").alias("GRP_90_PCT_LOWR_CL_NBR"),
							pcrGrpEfcncyRatioDfFilter("GRP_OE_UPR_CNFDNC_LVL_VOL_WGTD_NBR").alias("GRP_90_PCT_UPR_CL_NBR"),
							pcrGrpEfcncyRatioDfFilter("GRP_SCRBL_AVG_EPSD_NRMLZD_OE_VOL_WGTD_NBR").alias("GRP_NRMLZD_VOL_WGTD_MEAN_ETG_INDX_NBR"),
							pcrGrpEfcncyRatioDfFilter("GRP_OE_LOWR_CNFDNC_LVL_NRMLZD_VOL_WGTD_NBR").alias("GRP_NRMLZD_90_PCT_LOWR_CL_NBR"),
							pcrGrpEfcncyRatioDfFilter("GRP_OE_UPR_CNFDNC_LVL_NRMLZD_VOL_WGTD_NBR").alias("GRP_NRMLZD_90_PCT_UPR_CL_NBR")).distinct()

					val apiEtgGroupSpcltyDf = apiProvWrkPcrGrpEfcncyRatioJoin.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
					withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
					withColumn(lastUpdatedDate, lit(current_timestamp())).
					withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser))
					.withColumn("run_id", lit(maxrun_id))

					writeTblInsertOverwrite(apiEtgGroupSpcltyDf, warehouseHiveDB, tabApiEtgGroupSpclty)
					writeTblAppendOverwrite(apiEtgGroupSpcltyDf, warehouseHiveDB, tabApiEtgGroupSpcltyBkp)

					runIDInsert(tabApiEtgGroupSpclty, maxrun_id)

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

					val startTime = DateTime.now()
					//Writing the data to a table in Hive
					println(s"[HPIP-ETL] snapNbr derivation started at : $startTime")
					val snapNbr = pcrElgblProvDf.join(pcrGrpEfcncyRatioMultiSpltDf, pcrElgblProvDf("BKBN_SNAP_YEAR_MNTH_NBR") === pcrGrpEfcncyRatioMultiSpltDf("SNAP_YEAR_MNTH_NBR"))
					.join(pcrGrpEfcncyRatioDf, Seq("SNAP_YEAR_MNTH_NBR"))
					.join(qltyNrwNtwkTaxSpcltyDf, Seq("SNAP_YEAR_MNTH_NBR"))
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
