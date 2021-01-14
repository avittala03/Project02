package com.am.ndo.eHoppa

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Locale

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.functions.upper
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
import org.apache.spark.sql.types.DecimalType

class EHoppaOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

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
					/*
					-----------------------------IMPLEMENTED TDCH FAST EXPORT - Commented the JDBC approach
					//Reading the queries from config file
					println(s"[NDO-ETL] Reading the queries from config file")

					val configTeradata = new TeradataConfig(configPath, env, queryFileCategory)

					val dbserverurl3 = configTeradata.dbserverurl3
					println(s"[NDO-ETL] dbserverurl :" + dbserverurl3)

					val hiveFileFormat = configTeradata.hiveFileFormat
					println(s"[NDO-ETL] hiveFileFormat :" + hiveFileFormat)

					val jdbcdriver = configTeradata.jdbcdriver
					val dbuserid = configTeradata.dbuserid
					val encriptedPassword = configTeradata.encriptedPassword
					val dbpassword = Encryption.decrypt(encriptedPassword)

					println(s"[NDO-ETL] queryFilePath:" + queryFilePath)

					val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
					val queryPath = new Path(queryFilePath)
					val queryInputStream = fs.open(queryPath)

					val queryProperties = new Properties();
					queryProperties.load(queryInputStream);

					val properties = new java.util.Properties()

					properties.setProperty("driver", jdbcdriver)
					properties.setProperty("user", dbuserid)
					properties.setProperty("password", dbpassword)
					properties.setProperty("numPartitions", "5")
					properties.setProperty("fetchSize", "5000")

					//Get teradata source variables
					val outpatientDetail_src_tab = config.getString("src_tab_outpatient_detail")
					val outpatientSummary_src_tab = config.getString("src_tab_outpatient_summary")
					val inpatientSummary_src_tab = config.getString("src_tab_inpatient_summary")
					val facilityAttributeProfile_src_tab = config.getString("src_tab_facility_attribute_profile")

					//Get Hive source variables
  				val tabinpatientsummary=config.getString("tab_inpatient_summary")
  				val taboutpatientsummary = config.getString("tab_outpatient_summary")
  				val taboutpatientdetail = config.getString("tab_outpatient_detail")
  				val tabfacilityattributeprofile = config.getString("tab_facility_attribute_profile")

  				//Read data from Teradata
  				val outpatientDetail_srcDf = spark.read.jdbc(dbserverurl3, outpatientDetail_src_tab, properties)
  				println(s"[NDO-ETL] Teradata table read succesfully for query_outpatient_detail")
  				val outpatientSummary_srcDf = spark.read.jdbc(dbserverurl3, outpatientSummary_src_tab, properties)
  				println(s"[NDO-ETL] Teradata table read succesfully for query_outpatient_summary")
  				val inpatientSummary_srcDf = spark.read.jdbc(dbserverurl3, inpatientSummary_src_tab, properties)
  				println(s"[NDO-ETL] Teradata table read succesfully for query_inpatient_summary")
  				val facilityAttributeProfile_srcDf = spark.read.jdbc(dbserverurl3, facilityAttributeProfile_src_tab, properties)
  				println(s"[NDO-ETL] Teradata table read succesfully for query_facility_attribute_profile")


  				//Persist data in Hive tables
  				WriteManagedTblInsertOverwrite(outpatientDetail_srcDf, stagingHiveDB, taboutpatientdetail)
  				println(s"[NDO-ETL] Data written succesfully for " + stagingHiveDB+'.'+taboutpatientdetail)
  				WriteManagedTblInsertOverwrite(outpatientSummary_srcDf, stagingHiveDB, taboutpatientsummary)
  				println(s"[NDO-ETL] Data written succesfully for " + stagingHiveDB+'.'+ taboutpatientsummary )
  				WriteManagedTblInsertOverwrite(inpatientSummary_srcDf, stagingHiveDB, tabinpatientsummary)
  				println(s"[NDO-ETL] Data written read succesfully for "  + stagingHiveDB+'.'+ tabinpatientsummary)
  				WriteManagedTblInsertOverwrite(facilityAttributeProfile_srcDf, stagingHiveDB, tabfacilityattributeprofile)
  				println(s"[NDO-ETL] Data written read succesfully for "  + stagingHiveDB+'.'+ tabfacilityattributeprofile) */

					//Get SQLs of the persited table
					val outpatientdetailQuery = config.getString("query_outpatient_detail").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val outpatientsummaryQuery = config.getString("query_outpatient_summary").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val inpatientsummaryQuery = config.getString("query_inpatient_summary").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val facilityattributeprofileQuery = config.getString("query_facility_attribute_profile").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val ndoZipSubmarketXxwalkQuery = config.getString("query_ndo_zip_submarket_xwalk_Ehoppa").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()

					val outpatientDetailDf = spark.sql(outpatientdetailQuery)
					val outpatientSummaryDf = spark.sql(outpatientsummaryQuery)
					val inpatientSummaryDf = spark.sql(inpatientsummaryQuery)
					val facilityAttributeProfileDf = spark.sql(facilityattributeprofileQuery)
					val ndoZipSubmarketXxwalkDf = spark.sql(ndoZipSubmarketXxwalkQuery)

					val mapDF = Map(
							"outpatient_detail" -> outpatientDetailDf,
							"inpatient_summary" -> inpatientSummaryDf,
							"facility_attribute_profile" -> facilityAttributeProfileDf,
							"outpatient_summary" -> outpatientSummaryDf,
							"ndo_zip_submarket_xwalk" -> ndoZipSubmarketXxwalkDf)

					println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					mapDF

	}

	def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
			val startTime = DateTime.now
					println(s"[NDO-ETL] Processing Data Started: $startTime")
					//Reading the data frames as elements from Map
					val outpatientDetailUnFil = inMapDF.getOrElse("outpatient_detail", null)
					val inpatientSummaryUnFil = inMapDF.getOrElse("inpatient_summary", null)
					//val facilityAttributeProfileDf = inMapDF.getOrElse("facility_attribute_profile", null)
					val facilityAttributeProfileDf1 = inMapDF.getOrElse("facility_attribute_profile", null)
					val outpatientSummaryUnFil = inMapDF.getOrElse("outpatient_summary", null)
					val ndoZipSubmarketXxwalkDf = inMapDF.getOrElse("ndo_zip_submarket_xwalk", null)
					
					
					val tabndoEhoppaBase = config.getString("tab_ndo_ehoppa_base")
					val tabndoEhoppaBaseBkp = config.getString("tab_ndo_ehoppa_base_bkp")
					val tabndoEhoppaBaseMbrCnty = config.getString("tab_ndo_ehoppa_base_mbr_cnty")
					val tabndoEhoppaBaseMbrCntyBkp = config.getString("tab_ndo_ehoppa_base_mbr_cnty_bkp")
					val tabndoEhoppaBaseMbr = config.getString("tab_ndo_ehoppa_base_mbr")
					val tabndoEhoppaBaseMbrBkp = config.getString("tab_ndo_ehoppa_base_mbr_bkp")

					val tabndowrkHoppaCmadBenchDf = config.getString("tab_ndowrk_hoppa_cmad_bench")
					val tabndowrkHoppaOutp = config.getString("tab_ndowrk_hoppa_outp")
					val tabndowrkHoppaInp = config.getString("tab_ndowrk_hoppa_inp")
					val tabndowrkHoppaMbrCntyInp = config.getString("tab_ndowrk_hoppa_mbr_cnty_inp")
					val tabndowrkHoppaMbrCntyOutp = config.getString("tab_ndowrk_hoppa_mbr_cnty_outp")
					val tabndowrkHoppaMbrRatgInp = config.getString("tab_ndowrk_hoppa_mbr_ratg_inp")
					val tabndowrkHoppaMbrRatgOutp = config.getString("tab_ndowrk_hoppa_mbr_ratg_outp")

					val tabndowrkOutpZip1 = "ndowrk_outp_zip1"
					val tabndowrkOutpZip2 = "ndowrk_outp_zip2"
					val maxrun_id_base = getMAxRunId(tabndoEhoppaBase)
					val maxrun_id_base_mbr = getMAxRunId(tabndoEhoppaBaseMbr)
					val maxrun_id_base_mbr_cnty = getMAxRunId(tabndoEhoppaBaseMbrCnty)
					val map = getDateRange()
			  	val rangeStartDate = map.getOrElse("rangeStartDate", null)
					val rangeEndDate = map.getOrElse("rangeEndDate", null)
					
					//need to remove in next release
					val withOutSpclChar = facilityAttributeProfileDf1.filter(!$"hospital".contains("NYU Langone Hospital"))
          val withSpclChar = facilityAttributeProfileDf1.filter($"hospital".contains("NYU Langone Hospital"))
          val spclCharRpls = withSpclChar.withColumn("hospital", lit("NYU Langone Hospital—Brooklyn"))
          val facilityAttributeProfileDf = withOutSpclChar.union(spclCharRpls)
					
					val outpatientDetailDf = outpatientDetailUnFil.filter($"CLM_LINE_ENCNTR_CD".isin("N", "NA", "UNK") && $"brand" === concat(lit("Blue "), $"MBR_State"))
					val inpatientSummaryDf = inpatientSummaryUnFil.filter($"CLM_LINE_ENCNTR_CD".isin("N", "NA", "UNK") && $"brand" === concat(lit("Blue "), $"MBR_State") && $"prov_county".isNull === false )
					val outpatientSummaryDf = outpatientSummaryUnFil.filter($"CLM_LINE_ENCNTR_CD".isin("N", "NA", "UNK") && $"brand" === concat(lit("Blue "), $"MBR_State") && $"prov_county".isNull === false)

					val f = for {

						/*
							Analysis queris for NDO_EHOPPA_BASE_MBR_CNTY data
							NDOWRK_OUTP_ZIP1  & NDOWRK_OUTP_ZIP2  used for out patient rating area
							NDOWRK_HOPPA_PROV_LIST1  -  first step in creating PROV_LIST
							NDOWRK_HOPPA_PROV_LIST - primary driver table for eHoppa data tables
							NDOWRK_HOPPA_CMAD_BENCH  - reference table holding the CMAD Index values (CMI) for all eHoppa service type categories
							NDO_ZIP_SUBMARKET_XWALK  - business owned rating area to zip code xwalk (reference table)
						 */

	//					ndowrkOutpZip1Df <- ndowrkOutpZip1(outpatientDetailDf, rangeStartDate, rangeEndDate)
//						WritendowrkHoppaCmadBenchDf <- WriteSaveAsTableOverwrite(ndowrkOutpZip1Df, stagingHiveDB, tabndowrkOutpZip1)

	//					ndowrkOutpZip2Df <- ndowrkOutpZip2(ndowrkOutpZip1Df, ndoZipSubmarketXxwalkDf)
//						WritendowrkHoppaCmadBenchDf <- WriteSaveAsTableOverwrite(ndowrkOutpZip2Df, stagingHiveDB, tabndowrkOutpZip2)

	  				ndowrkHoppaCmadBenchDf <- ndowrkHoppaCmadBench(facilityAttributeProfileDf, inpatientSummaryDf, outpatientSummaryDf, stagingHiveDB, rangeStartDate, rangeEndDate)
						WritendowrkHoppaCmadBenchDf <- WriteManagedTblInsertOverwrite(ndowrkHoppaCmadBenchDf, stagingHiveDB, tabndowrkHoppaCmadBenchDf)

						/* -----------------------Analysis queris for NDO_EHOPPA_BASE data*/

						ndowrkHoppaOutpDf <- ndowrkHoppaOutp(facilityAttributeProfileDf, outpatientSummaryDf, tabndowrkHoppaCmadBenchDf, ndoZipSubmarketXxwalkDf, stagingHiveDB, rangeStartDate, rangeEndDate)
						WritenndowrkHoppaInpDf <- WriteManagedTblInsertOverwrite(ndowrkHoppaOutpDf, stagingHiveDB, tabndowrkHoppaOutp)

						ndowrkHoppaInpDf <- ndowrkHoppaInp(facilityAttributeProfileDf, inpatientSummaryDf, tabndowrkHoppaCmadBenchDf, ndoZipSubmarketXxwalkDf, stagingHiveDB, rangeStartDate, rangeEndDate)
						WritenndowrkHoppaInpDf <- WriteManagedTblInsertOverwrite(ndowrkHoppaInpDf, stagingHiveDB, tabndowrkHoppaInp)

						ndowrkEhoppaBaseDf <- ndowrkEhoppaBase(tabndowrkHoppaInp, tabndowrkHoppaOutp, stagingHiveDB)

						//WRAP FUNCTION TO ALIGN WITH SQL METADATA AND DEFAULTING COLUMNS
						ndoEhoppaBaseDf <- ndoEhoppaBase(ndowrkEhoppaBaseDf)

						val ndoEhoppaBaseDfWC = ndoEhoppaBaseDf.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
						withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
						withColumn(lastUpdatedDate, lit(current_timestamp())).
						withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).withColumn("run_id", lit(maxrun_id_base))

						WritendoEhoppaBaseDf <- WriteManagedTblInsertOverwrite(ndoEhoppaBaseDfWC, warehouseHiveDB, tabndoEhoppaBase)
						WritendoEhoppaBaseDfBkp <- WriteExternalTblAppend(ndoEhoppaBaseDfWC, warehouseHiveDB, tabndoEhoppaBaseBkp)

						/*
						Analysis queries for NDO_EHOPPA_BASE_MBR data (member rating area)
						eHoppa member base tables NDOWRK_HOPPA_PROV_LIST  and NDOWRK_HOPPA_CMAD_BENCH must exist
						 */

						ndowrkHoppaMbrRatgOutpDf <- ndowrkHoppaMbrRatgOutp(facilityAttributeProfileDf, outpatientSummaryDf, tabndowrkHoppaCmadBenchDf, ndoZipSubmarketXxwalkDf, stagingHiveDB, rangeStartDate, rangeEndDate)
						WritendowrkHoppaMbrRatgOutpDf <- WriteManagedTblInsertOverwrite(ndowrkHoppaMbrRatgOutpDf, stagingHiveDB, tabndowrkHoppaMbrRatgOutp)

						ndowrkHoppaMbrRatgInpDf <- ndowrkHoppaMbrRatgInp(facilityAttributeProfileDf, inpatientSummaryDf, tabndowrkHoppaCmadBenchDf, ndoZipSubmarketXxwalkDf, stagingHiveDB, rangeStartDate, rangeEndDate)
						WritendowrkHoppaMbrRatgInpDf <- WriteManagedTblInsertOverwrite(ndowrkHoppaMbrRatgInpDf, stagingHiveDB, tabndowrkHoppaMbrRatgInp)

						ndowrkEhoppaMbrRatgBaseDf <- ndowrkEhoppaMbrRatgBase(tabndowrkHoppaMbrRatgInp, tabndowrkHoppaMbrRatgOutp, stagingHiveDB)

						ndoEhoppaBaseMbrDf <- ndoEhoppaBaseMbr(ndowrkEhoppaMbrRatgBaseDf)

						val ndoEhoppaBaseMbrDfWC = ndoEhoppaBaseMbrDf.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
						withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
						withColumn(lastUpdatedDate, lit(current_timestamp())).
						withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).withColumn("run_id", lit(maxrun_id_base_mbr))

						WritendoEhoppaBaseMbrDfWC <- WriteManagedTblInsertOverwrite(ndoEhoppaBaseMbrDfWC, warehouseHiveDB, tabndoEhoppaBaseMbr)
						WritendoEhoppaBaseMbrDfWCBkp <- WriteExternalTblAppend(ndoEhoppaBaseMbrDfWC, warehouseHiveDB, tabndoEhoppaBaseMbrBkp)

						//						/*
						//						Analysis queries for NDO_EHOPPA_BASE_MBR_CNTY data
						//						eHoppa member base county tables NDOWRK_HOPPA_PROV_LIST  and NDOWRK_HOPPA_CMAD_BENCH must exist
						//						 */
						//
						ndowrkHoppaMbrCntyInpDf <- ndowrkHoppaMbrCntyInp(facilityAttributeProfileDf, inpatientSummaryDf, tabndowrkHoppaCmadBenchDf, ndoZipSubmarketXxwalkDf, stagingHiveDB, rangeStartDate, rangeEndDate)
						WritendowrkHoppaMbrRatgInpDf <- WriteSaveAsTableOverwrite(ndowrkHoppaMbrCntyInpDf, stagingHiveDB, tabndowrkHoppaMbrCntyInp)

						ndowrkHoppaMbrCntyOutpDf <- ndowrkHoppaMbrCntyOutp(facilityAttributeProfileDf, outpatientSummaryDf, tabndowrkHoppaCmadBenchDf, ndoZipSubmarketXxwalkDf, stagingHiveDB, rangeStartDate, rangeEndDate)
						WritendowrkHoppaMbrRatgOutpDf <- WriteSaveAsTableOverwrite(ndowrkHoppaMbrCntyOutpDf, stagingHiveDB, tabndowrkHoppaMbrCntyOutp)

						ndowrkEhoppaMbrCntyBaseDf <- ndowrkEhoppaMbrCntyBase(tabndowrkHoppaMbrCntyInp, tabndowrkHoppaMbrCntyOutp, stagingHiveDB)

						ndoEhoppaBaseMbrCntyDf <- ndoEhoppaBaseMbrCnty(ndowrkEhoppaMbrCntyBaseDf)

						val ndoEhoppaBaseMbrCntyDfWC = ndoEhoppaBaseMbrCntyDf.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
						withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
						withColumn(lastUpdatedDate, lit(current_timestamp())).
						withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).withColumn("run_id", lit(maxrun_id_base_mbr_cnty))

						WritendoEhoppaBaseMbrCntyDfWC <- WriteManagedTblInsertOverwrite(ndoEhoppaBaseMbrCntyDfWC, warehouseHiveDB, tabndoEhoppaBaseMbrCnty)
						WritendoEhoppaBaseMbrCntyDfWCBkp <- WriteExternalTblAppend(ndoEhoppaBaseMbrCntyDfWC, warehouseHiveDB, tabndoEhoppaBaseMbrCntyBkp)

					} yield {
						println(s"[NDO-ETL] done")
					}
					f.run(spark)

					runIDInsert(tabndoEhoppaBase, maxrun_id_base)
					runIDInsert(tabndoEhoppaBaseMbr, maxrun_id_base_mbr)
					runIDInsert(tabndoEhoppaBaseMbrCnty, maxrun_id_base_mbr_cnty)

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

	def getDateRange(): Map[String, String] =
		{
				val calender = Calendar.getInstance()
						val formatter2 = new SimpleDateFormat("MM", Locale.ENGLISH)
						val formatter3 = new SimpleDateFormat("yyyy", Locale.ENGLISH)

						val monthUF = calender.getTime()
						val month = formatter2.format(monthUF).toInt

						println("month" + month)

						var priorEndNotFormatted = calender.getTime()
						var rangeStartDate = ""
						var rangeEndDate = ""

						if (month == 1 || month == 2 || month == 3) {
							calender.add(Calendar.YEAR, -2)

							priorEndNotFormatted = calender.getTime()
							rangeStartDate = formatter3.format(priorEndNotFormatted) + "01"
							println("rangeStartDate" + rangeStartDate)
							rangeEndDate = formatter3.format(priorEndNotFormatted) + "12"
							println("rangeStartDate" + rangeEndDate)

						} else {
							calender.add(Calendar.YEAR, -1)
							priorEndNotFormatted = calender.getTime()
							rangeStartDate = formatter3.format(priorEndNotFormatted) + "01"
							println("rangeStartDate" + rangeStartDate)
							rangeEndDate = formatter3.format(priorEndNotFormatted) + "12"
							println("rangeStartDate" + rangeEndDate)
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
