package com.am.ndo.api.apiProv

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.concat
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
import org.apache.spark.sql.catalyst.expressions.Substring
import com.am.ndo.helper.WriteExternalTblAppend

class ApiProvOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

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

					val pcrElgblProvQuery = config.getString("api_query_pcr_elgbl_prov").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val pcrGrpEfcncyRatioQuery = config.getString("api_query_pcr_grp_efcncy_ratio").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val bkbnIpQuery = config.getString("api_query_bkbn_ip").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val qltyNrwNtwkTaxSpcltyQuery = config.getString("api_query_qlty_nrw_ntwk_tax_spclty").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val pcrGrpEfcncyRatioMultiSpltQuery = config.getString("api_query_pcr_grp_efcncy_ratio_multi_splt").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val bkbnTaxIdQuery = config.getString("api_query_bkbn_tax_id").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val botSubmrktQuery = config.getString("api_query_bot_submrkt").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val bkbnIpBvQuery = config.getString("api_query_bkbn_ip_bv").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val bkbnAdrsRltnshpQuery = config.getString("api_query_bkbn_adrs_rltnshp").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()

					println(s"[NDO-ETL] Query for reading data from pcr_elgbl_prov table is $pcrElgblProvQuery")
					println(s"[NDO-ETL] Query for reading data from pcr_grp_efcncy_ratio table is $pcrGrpEfcncyRatioQuery")
					println(s"[NDO-ETL] Query for reading data from bkbn_ip table is $bkbnIpQuery")
					println(s"[NDO-ETL] Query for reading data from qlty_nrw_ntwk_tax_spclty table is $qltyNrwNtwkTaxSpcltyQuery")
					println(s"[NDO-ETL] Query for reading data from pcr_grp_efcncy_ratio_multi_splt table is $pcrGrpEfcncyRatioMultiSpltQuery")
					println(s"[NDO-ETL] Query for reading data from bkbn_tax_id table is $bkbnTaxIdQuery")
					println(s"[NDO-ETL] Query for reading data from bot_submrkt table is $botSubmrktQuery")
					println(s"[NDO-ETL] Query for reading data from bkbn_ip_bv table is $bkbnIpBvQuery")
					println(s"[NDO-ETL] Query for reading data from bkbn_adrs_rltnshp table is $bkbnAdrsRltnshpQuery")

					val pcrElgblProvDf = spark.sql(pcrElgblProvQuery)
					val pcrGrpEfcncyRatioDf = spark.sql(pcrGrpEfcncyRatioQuery)
					val bkbnIpDf = spark.sql(bkbnIpQuery)
					val qltyNrwNtwkTaxSpcltyDf = spark.sql(qltyNrwNtwkTaxSpcltyQuery)
					val pcrGrpEfcncyRatioMultiSpltDf = spark.sql(pcrGrpEfcncyRatioMultiSpltQuery)
					val bkbnTaxIdDf = spark.sql(bkbnTaxIdQuery)
					val botSubmrktDf = spark.sql(botSubmrktQuery)
					val bkbnIpBvDf = spark.sql(bkbnIpBvQuery)
					val bkbnAdrsRltnshpDf = spark.sql(bkbnAdrsRltnshpQuery)

					val mapDF = Map("pcr_elgbl_prov" -> pcrElgblProvDf,
							"pcr_grp_efcncy_ratio" -> pcrGrpEfcncyRatioDf,
							"BKBN_IP" -> bkbnIpDf, "qlty_nrw_ntwk_tax_spclty" -> qltyNrwNtwkTaxSpcltyDf,
							"pcr_grp_efcncy_ratio_multi_splt" -> pcrGrpEfcncyRatioMultiSpltDf,
							"bkbn_tax_id" -> bkbnTaxIdDf,
							"bot_submrkt" -> botSubmrktDf,
							"bkbn_ip_bv" -> bkbnIpBvDf,
							"bkbn_adrs_rltnshp" -> bkbnAdrsRltnshpDf)

					println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					mapDF

	}

	def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
			val startTime = DateTime.now
					println(s"[NDO-ETL] Processing Data Started: $startTime")
					//Reading the data frames as elements from Map
					val pcrElgblProvDf = inMapDF.getOrElse("pcr_elgbl_prov", null)
					val pcrGrpEfcncyRatioDf = inMapDF.getOrElse("pcr_grp_efcncy_ratio", null)
					val bkbnIpDf = inMapDF.getOrElse("BKBN_IP", null)
					val pcrGrpEfcncyRatioMultiSpltDf = inMapDF.getOrElse("pcr_grp_efcncy_ratio_multi_splt", null)
					val bkbnTaxIdDf = inMapDF.getOrElse("bkbn_tax_id", null)
					val botSubmrktDf = inMapDF.getOrElse("bot_submrkt", null)
					val bkbnIpBvDf = inMapDF.getOrElse("bkbn_ip_bv", null)
					val bkbnAdrsRltnshpDf = inMapDF.getOrElse("bkbn_adrs_rltnshp", null)

					val tabApiProvWrk1 = config.getString("tab_api_prov_wrk1")
					val tabApiProvWrk = config.getString("tab_api_prov_wrk")
					val tabApiWrkProvCnty = config.getString("tab_api_wrk_prov_cnty")
					val tabApiProv = config.getString("tab_api_prov")
					val tabApiProvBkp = config.getString("tab_api_prov_bkp")

					val maxrun_id_api_prov_wrk = getMAxRunId(tabApiProvWrk)
					val maxrun_id = getMAxRunId(tabApiProv)
					println(s"[NDO-ETL] Max run id is : $maxrun_id")

					val snapNbr = getSnapNbr(inMapDF)
					println(s"[NDO-ETL] Available snapNbr is : $snapNbr")

					val sixMonthHold = getsixMonthHold(snapNbr)
					println(s"[NDO-ETL] Available snapNbr is : $sixMonthHold")

					val pcrGrpEfcncyRatioFilTin = pcrGrpEfcncyRatioDf.filter($"GRP_AGRGTN_TYPE_CD" === "TIN" && $"BKBN_SNAP_YEAR_MNTH_NBR" === snapNbr
					&& (($"NTWK_ST_CD".isin("CA", "NY", "VA") && $"PEER_MRKT_CD".isin("8","20")) ||
					($"NTWK_ST_CD".isin("CA", "NY", "VA") === false && $"PEER_MRKT_CD".isin( "4","19"))) )
					.select($"BKBN_SNAP_YEAR_MNTH_NBR",
							$"GRP_AGRGTN_ID",
							$"NTWK_ST_CD",
							$"PCR_LOB_DESC",
							$"BNCHMRK_PROD_DESC",
							$"PEER_MRKT_CD").distinct
					val pcrGrpEfcncyRatioFilNpi = pcrGrpEfcncyRatioDf.filter($"GRP_AGRGTN_TYPE_CD" === "NPI" && $"BKBN_SNAP_YEAR_MNTH_NBR" === snapNbr
					&& ($"NTWK_ST_CD".isin("NY") && $"PEER_MRKT_CD" === "3"))
					.select($"BKBN_SNAP_YEAR_MNTH_NBR",
							$"GRP_AGRGTN_ID",
							$"NTWK_ST_CD",
							$"PCR_LOB_DESC",
							$"BNCHMRK_PROD_DESC",
							$"PEER_MRKT_CD").distinct


					val pcrElgblProvFilTin = pcrElgblProvDf.filter(pcrElgblProvDf("BKBN_SNAP_YEAR_MNTH_NBR") === snapNbr && pcrElgblProvDf("BNCHMRK_PROD_DESC").isin("NP", "NA", "PAR") === false &&
					pcrElgblProvDf("SPCLTY_PRMRY_CD").isin("UNK", "NA", "53", "17", "A0", "A5", "69", " 32", "B4", "51", "87", "99", "43", "54", "88", "61") === false &&
					pcrElgblProvDf("PCR_LOB_DESC").isin("NP") === false &&
					pcrElgblProvDf("NTWK_ST_CD").isin("CA", "CO", "CT", "FL", "GA", "IA", "IN", "KS", "KY", "LA", "MD", "ME", "MO", "NH",
							"NJ", "NM", "NV", "NY", "OH", "SC", "TN", "TX", "VA", "WA", "WI", "WV") &&
					((pcrElgblProvDf("NTWK_ST_CD").isin("CA", "NY", "VA") && pcrElgblProvDf("PEER_MRKT_CD").isin("8","20")) ||
							(pcrElgblProvDf("NTWK_ST_CD").isin("CA", "NY", "VA") === false && pcrElgblProvDf("PEER_MRKT_CD").isin("4","19"))))

					val pcrElgblProvFilNpi = pcrElgblProvDf.filter(pcrElgblProvDf("BKBN_SNAP_YEAR_MNTH_NBR") === snapNbr && pcrElgblProvDf("BNCHMRK_PROD_DESC").isin("NP", "NA", "PAR") === false &&
					pcrElgblProvDf("SPCLTY_PRMRY_CD").isin("UNK", "NA", "53", "17", "A0", "A5", "69", " 32", "B4", "51", "87", "99", "43", "54", "88", "61") === false &&
					pcrElgblProvDf("PCR_LOB_DESC").isin("NP") === false &&
					pcrElgblProvDf("NTWK_ST_CD") === "NY" && pcrElgblProvDf("PEER_MRKT_CD").isin("1","3") )

					val bkbnIpBvFilDf = bkbnIpBvDf.filter($"SNAP_YEAR_MNTH_NBR" === snapNbr && $"PADRS_TIN_CNTY_NM".!==("NA") && $"PRMRY_ST_IND" === "Y")
					.select($"EP_TAX_ID", $"NTWK_ST_CD", $"PADRS_TIN_ST_CD", $"PADRS_TIN_CNTY_NM").distinct()

					val botSubmrktDistDf = botSubmrktDf.select($"SUBMRKT_CD", $"SUBMRKT_DESC", $"SUBMRKT_ST_CD").distinct()

					val bkbnAdrsRltnshpFilDf = bkbnAdrsRltnshpDf.filter(bkbnAdrsRltnshpDf("CNTY_NM") !== "")

					val columnfilterTin = "TIN"
					val columnfilterNpi = "NPI"

					val f = for {

						/* service area for all service areas */

						apiProvWrk1Df1 <- apiProvWrk1(pcrElgblProvFilTin, bkbnTaxIdDf, bkbnIpDf, pcrGrpEfcncyRatioFilTin, botSubmrktDistDf, snapNbr, columnfilterTin)
						WriteapiProvWrk1Df1 <- WriteManagedTblInsertOverwrite(apiProvWrk1Df1, stagingHiveDB, tabApiProvWrk1)

						apiProvWrk1Df2 <- apiProvWrk1(pcrElgblProvFilNpi, bkbnTaxIdDf, bkbnIpDf, pcrGrpEfcncyRatioFilNpi, botSubmrktDistDf, snapNbr, columnfilterNpi)
						WriteapiProvWrk1Df2 <- WriteExternalTblAppend(apiProvWrk1Df2, stagingHiveDB, tabApiProvWrk1)

						apiProvWrkCnty <- apiProvCnty(bkbnIpBvFilDf, bkbnAdrsRltnshpFilDf, snapNbr, sixMonthHold, stagingHiveDB, tabApiProvWrk1)
						WriteapiProvWrk2Df2 <- WriteManagedTblInsertOverwrite(apiProvWrkCnty, stagingHiveDB, tabApiWrkProvCnty)

						apiProvWrkDf1 <- apiProvWrk(stagingHiveDB, tabApiProvWrk1, tabApiWrkProvCnty)
						val apiProvWrkDf1runid = apiProvWrkDf1.withColumn("run_id", lit(maxrun_id_api_prov_wrk))
						WriteapiProvWrk2Df2 <- WriteManagedTblInsertOverwrite(apiProvWrkDf1runid, warehouseHiveDB, tabApiProvWrk)

						
						apiProvDf <- apiProv(warehouseHiveDB, tabApiProvWrk)
						val apiProv = apiProvDf.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
						withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
						withColumn(lastUpdatedDate, lit(current_timestamp())).
						withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).withColumn("run_id", lit(maxrun_id))
						WriteapiProvWrk2Df2 <- WriteManagedTblInsertOverwrite(apiProv, warehouseHiveDB, tabApiProv)
						WriteapiProvWrk2Df2 <- WriteExternalTblAppend(apiProv, warehouseHiveDB, tabApiProvBkp)

					} yield {
						println("done")
					}
					f.run(spark)
					runIDInsert(tabApiProvWrk, maxrun_id_api_prov_wrk)
					runIDInsert(tabApiProv, maxrun_id)

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
					val tab = tablename.toUpperCase()

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

	def getSnapNbr(inMapDF: Map[String, DataFrame]): Long = {
			val startTime = DateTime.now()
					//Writing the data to a table in Hive
					println(s"[HPIP-ETL] SNAP_YEAR_MNTH_NBR derivation started at : $startTime")

					val pcrElgblProvDf = inMapDF.getOrElse("pcr_elgbl_prov", null).select($"BKBN_SNAP_YEAR_MNTH_NBR".alias("SNAP_YEAR_MNTH_NBR")).distinct
					val pcrGrpEfcncyRatioDf = inMapDF.getOrElse("pcr_grp_efcncy_ratio", null).select($"BKBN_SNAP_YEAR_MNTH_NBR".alias("SNAP_YEAR_MNTH_NBR")).distinct
					val qltyNrwNtwkTaxSpcltyDf = inMapDF.getOrElse("qlty_nrw_ntwk_tax_spclty", null).select($"SNAP_YEAR_MNTH_NBR").distinct()
					val pcrGrpEfcncyRatioMultiSpltDf = inMapDF.getOrElse("pcr_grp_efcncy_ratio_multi_splt", null).select($"BKBN_SNAP_YEAR_MNTH_NBR".alias("SNAP_YEAR_MNTH_NBR")).distinct()

					val snapNbr = pcrElgblProvDf.join(pcrGrpEfcncyRatioDf, Seq("SNAP_YEAR_MNTH_NBR"))
					.orderBy($"SNAP_YEAR_MNTH_NBR".desc).head.getLong(0)

					println(s"[HPIP-ETL] latest SNAP_YEAR_MNTH_NBR available in all the tables is $snapNbr")
					println(s"[HPIP-ETL]  SNAP_YEAR_MNTH_NBR derivation Completed at: " + DateTime.now())
					println(s"[HPIP-ETL] Time Taken for derivation :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")
					snapNbr

	}

	def getmaxEtgRunId(inMapDF: Map[String, DataFrame]): Long = {
			val startTime = DateTime.now()
					//Writing the data to a table in Hive
					info(s"[HPIP-ETL] Writing Dataframes to Hive started at: $startTime")

					val pcrCostEpsdDf = inMapDF.getOrElse("pcr_cost_epsd", null).select($"ETG_RUN_ID").distinct

					val maxEtgRunId = pcrCostEpsdDf.orderBy($"ETG_RUN_ID".desc).head.getLong(0)

					info(s"[HPIP-ETL] writing the data to target tables is Completed at: " + DateTime.now())
					info(s"[HPIP-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")
					maxEtgRunId

	}

	def getsixMonthHold(snapNbr: Long): Long =
		{

				val tobeDerived = snapNbr.toString
						val formatter = new SimpleDateFormat("yyyyMM", Locale.ENGLISH);
				val snapNbrFormated = formatter.parse(tobeDerived)

						val calender = Calendar.getInstance();
				calender.setTime(snapNbrFormated);
				calender.add(Calendar.MONTH, -6)
				val priorBeginNotFormatted = calender.getTime();
				val sixMonthHold = formatter.format(priorBeginNotFormatted).toLong

						println(s"[NDO-ETL] the snapnbr prior six months is : $sixMonthHold")

						sixMonthHold
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
