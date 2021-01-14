package com.am.ndo.epsdCostCnty

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.substring
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

class EpsdCostCntyOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

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
					val pcrCostEpsdQuery = config.getString("query_pcr_cost_epsd").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val fnclProdCfQuery = config.getString("query_fncl_prod_cf").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val ndoZipSubmarketXwalkQuery = config.getString("query_ndo_zip_submarket_xwalk").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val pcrEtgMbrSmryQuery = config.getString("query_pcr_etg_mbr_smry").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val fnclMbuCfQuery = config.getString("query_fncl_mbu_cf").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val etgBaseClsCdQuery = config.getString("query_etg_base_cls_cd").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val mlsaZipCdQuery = config.getString("query_mlsa_zip_cd").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val apiProvWrkQuery = config.getString("query_api_prov_wrk").replaceAll(ConfigKey.warehouseDBPlaceHolder, warehouseHiveDB).toLowerCase()
					val epsdNdowrkRatgAddrQuery = config.getString("query_epsd_ndowrk_ratg_addr").replaceAll(ConfigKey.warehouseDBPlaceHolder, warehouseHiveDB)
					val pcrEtgSmryQuery = config.getString("query_pcr_etg_smry").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    
					val pcrElgblProvQuery = config.getString("query_pcr_elgbl_prov").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val pcrGrpEfcncyRatioQuery = config.getString("query_pcr_grp_efcncy_ratio").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val bkbnIpBvQuery = config.getString("query_BKBN_IP_BV").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val qltyNrwNtwkTaxSpcltyQuery = config.getString("query_qlty_nrw_ntwk_tax_spclty").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val pcrGrpEfcncyRatioMultiSpltQuery = config.getString("query_pcr_grp_efcncy_ratio_multi_splt").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()

					println(s"[NDO-ETL] Query for reading data from pcr_cost_epsd table is $pcrCostEpsdQuery")
					println(s"[NDO-ETL] Query for reading data from fncl_prod_cf table is $fnclProdCfQuery")
					println(s"[NDO-ETL] Query for reading data from ndo_zip_submarket_xwalk table is $ndoZipSubmarketXwalkQuery")
					println(s"[NDO-ETL] Query for reading data from pcr_etg_mbr_smry table is $pcrEtgMbrSmryQuery")
					println(s"[NDO-ETL] Query for reading data from fncl_mbu_cf table is $fnclMbuCfQuery")
					println(s"[NDO-ETL] Query for reading data from etg_base_cls_cd table is $etgBaseClsCdQuery")
					println(s"[NDO-ETL] Query for reading data from mlsa_zip_cd table is $mlsaZipCdQuery")
					println(s"[NDO-ETL] Query for reading data from api_prov_wrk table is $apiProvWrkQuery")
					println(s"[NDO-ETL] Query for reading data from ndowrk_ratg_addr table is $epsdNdowrkRatgAddrQuery")
					println(s"[NDO-ETL] Query for reading data from pcr_etg_smry table is $pcrEtgSmryQuery")

					println(s"[NDO-ETL] Query for reading data from pcr_elgbl_prov table is $pcrElgblProvQuery")
					println(s"[NDO-ETL] Query for reading data from pcr_grp_efcncy_ratio table is $pcrGrpEfcncyRatioQuery")
					println(s"[NDO-ETL] Query for reading data from BKBN_IP_BV table is $bkbnIpBvQuery")
					println(s"[NDO-ETL] Query for reading data from qlty_nrw_ntwk_tax_spclty table is $qltyNrwNtwkTaxSpcltyQuery")
					println(s"[NDO-ETL] Query for reading data from pcr_grp_efcncy_ratio_multi_splt table is $pcrGrpEfcncyRatioMultiSpltQuery")

					val pcrCostEpsdDf = spark.sql(pcrCostEpsdQuery)
					val fnclProdCfDf = spark.sql(fnclProdCfQuery)
					val ndoZipSubmarketXwalkDf = spark.sql(ndoZipSubmarketXwalkQuery)
					val pcrEtgMbrSmryDf = spark.sql(pcrEtgMbrSmryQuery)
					val fnclMbuCfDf = spark.sql(fnclMbuCfQuery)
					val etgBaseClsCdDf = spark.sql(etgBaseClsCdQuery)
					val mlsaZipCdDf = spark.sql(mlsaZipCdQuery)
					val apiProvWrkDf = spark.sql(apiProvWrkQuery)
					val ndowrkRatgAddrDf = spark.sql(epsdNdowrkRatgAddrQuery)
					val pcrEtgSmryDf = spark.sql(pcrEtgSmryQuery)
					val pcrElgblProvDf = spark.sql(pcrElgblProvQuery)
					val pcrGrpEfcncyRatioDf = spark.sql(pcrGrpEfcncyRatioQuery)
					val bkbnIpBvDf = spark.sql(bkbnIpBvQuery)
					val qltyNrwNtwkTaxSpcltyDf = spark.sql(qltyNrwNtwkTaxSpcltyQuery)
					val pcrGrpEfcncyRatioMultiSpltDf = spark.sql(pcrGrpEfcncyRatioMultiSpltQuery)

					val mapDF = Map("pcr_cost_epsd" -> pcrCostEpsdDf, "fncl_prod_cf" -> fnclProdCfDf, "ndo_zip_submarket_xwalk" -> ndoZipSubmarketXwalkDf,
							"pcr_etg_mbr_smry" -> pcrEtgMbrSmryDf,
							"fncl_mbu_cf" -> fnclMbuCfDf, "etg_base_cls_cd" -> etgBaseClsCdDf, "mlsa_zip_cd" -> mlsaZipCdDf, "api_prov_wrk" -> apiProvWrkDf,"ndowrk_ratg_addr" -> ndowrkRatgAddrDf,
							"pcr_etg_smry" -> pcrEtgSmryDf, "pcr_elgbl_prov" -> pcrElgblProvDf,
							"pcr_grp_efcncy_ratio" -> pcrGrpEfcncyRatioDf,
							"BKBN_IP_BV" -> bkbnIpBvDf, "qlty_nrw_ntwk_tax_spclty" -> qltyNrwNtwkTaxSpcltyDf, "pcr_grp_efcncy_ratio_multi_splt" -> pcrGrpEfcncyRatioMultiSpltDf)

					println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					mapDF

	}

	def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
			val startTime = DateTime.now
					println(s"[NDO-ETL] Processing Data Started: $startTime")
					//Reading the data frames as elements from Map
					val pcrCostEpsdDf = inMapDF.getOrElse("pcr_cost_epsd", null)
					val fnclProdCfDf = inMapDF.getOrElse("fncl_prod_cf", null)
					val ndoZipSubmarketXwalkDf = inMapDF.getOrElse("ndo_zip_submarket_xwalk", null)
					val pcrEtgMbrSmryDf = inMapDF.getOrElse("pcr_etg_mbr_smry", null)
					val fnclMbuCfDf = inMapDF.getOrElse("fncl_mbu_cf", null)
					val etgBaseClsCdDf = inMapDF.getOrElse("etg_base_cls_cd", null)
					val mlsaZipCdDf = inMapDF.getOrElse("mlsa_zip_cd", null)
					val apiProvWrkDf = inMapDF.getOrElse("api_prov_wrk", null)
					val ndowrkRatgAddrDf = inMapDF.getOrElse("ndowrk_ratg_addr", null)
					val pcrEtgSmryDf = inMapDF.getOrElse("pcr_etg_smry", null)
					
					
					val tabApiEtgEpsdCost = config.getString("tab_api_etg_epsd_cost_cnty")
					val tabApiEtgEpsdCostBkp = config.getString("tab_api_etg_epsd_cost_cnty_bkp")

					val maxrun_id =  getMAxRunId(tabApiEtgEpsdCost)
					println(s"[NDO-ETL] Max run id is : $maxrun_id")

					val snapNbr = getSnapNbr(inMapDF)
					println(s"[NDO-ETL] Max run id is : $snapNbr")
					
				
					val apiProvWrkDfFil = apiProvWrkDf.filter(apiProvWrkDf("AGRGTN_TYPE_CD") === "TIN").select("PROV_TAX_ID").distinct()

					val mlsaZipCdDfFil = mlsaZipCdDf.filter(mlsaZipCdDf("ACTV_ZIP_IND") === "Y").select($"ZIP_CD", $"ST_CD",substring($"CNTY_NM",3,3).alias("MS_CNTY_CD")).distinct()

					val pcrEtgSmryFilDf = pcrEtgSmryDf.select($"ETG_RUN_ID",$"EPSD_NBR",$"RPTG_NTWK_DESC").distinct()
					
					val f = for {

						/* service area for all service areas */

						ndoWrkEpsdCost2Df <- ndoWrkEpsdCost2(pcrCostEpsdDf, fnclProdCfDf, ndoZipSubmarketXwalkDf, pcrEtgMbrSmryDf, fnclMbuCfDf, etgBaseClsCdDf, mlsaZipCdDfFil ,apiProvWrkDfFil, snapNbr, ndowrkRatgAddrDf,pcrEtgSmryFilDf)

						ndoWrkEpsdCostDf <- ndoWrkEpsdCost(ndoWrkEpsdCost2Df)
						
						val ndoWrkEpsdCostDfWc = ndoWrkEpsdCostDf.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
						withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
						withColumn(lastUpdatedDate, lit(current_timestamp())).
						withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).withColumn("run_id", lit(maxrun_id))

						WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(ndoWrkEpsdCostDfWc, warehouseHiveDB, tabApiEtgEpsdCost)
						WriteapiMlrSmryWC <- WriteExternalTblAppend(ndoWrkEpsdCostDfWc, warehouseHiveDB, tabApiEtgEpsdCostBkp)

					} yield {
						println("Episode Cost - Monad execution completed!")
					}
					f.run(spark)
					runIDInsert(tabApiEtgEpsdCost, maxrun_id)

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
					val bkbnIpBvDf = inMapDF.getOrElse("BKBN_IP_BV", null).select($"SNAP_YEAR_MNTH_NBR").distinct
					val qltyNrwNtwkTaxSpcltyDf = inMapDF.getOrElse("qlty_nrw_ntwk_tax_spclty", null).select($"SNAP_YEAR_MNTH_NBR").distinct
					val pcrGrpEfcncyRatioMultiSpltDf = inMapDF.getOrElse("pcr_grp_efcncy_ratio_multi_splt", null).select($"BKBN_SNAP_YEAR_MNTH_NBR".alias("SNAP_YEAR_MNTH_NBR")).distinct

					val snapNbr = pcrElgblProvDf.join(pcrGrpEfcncyRatioDf, Seq("SNAP_YEAR_MNTH_NBR"))
					.join(bkbnIpBvDf, Seq("SNAP_YEAR_MNTH_NBR"))
					.join(qltyNrwNtwkTaxSpcltyDf, Seq("SNAP_YEAR_MNTH_NBR"))
					.join(pcrGrpEfcncyRatioMultiSpltDf, Seq("SNAP_YEAR_MNTH_NBR"))
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
					val pcrEtgSmryDf = inMapDF.getOrElse("pcr_etg_smry", null).select($"ETG_RUN_ID").distinct

					val maxEtgRunId = pcrCostEpsdDf.join(pcrEtgSmryDf,Seq("ETG_RUN_ID")).orderBy($"ETG_RUN_ID".desc).head.getLong(0)

					info(s"[HPIP-ETL] writing the data to target tables is Completed at: " + DateTime.now())
					info(s"[HPIP-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")
					maxEtgRunId

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
