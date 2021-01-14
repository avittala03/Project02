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
class ProfReferralPatternsOperationABC(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

	import spark.implicits._

	//Audit
	var program = ""
	var user_id = ""
	var app_id = ""
	var start_time: DateTime = DateTime.now()
	var start = ""

	var listBuffer = ListBuffer[Audit]()

	def loadData(): Map[String, DataFrame] = {

			val clmMaxRvsnQuery = config.getString("query_prof_referral_pattern_clm_max_rvsn").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)
					val clmLineQuery = config.getString("query_prof_referral_pattern_clm_line").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)
					val clmQuery = config.getString("query_prof_referral_pattern_clm").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)
					val clmLineCoaQuery = config.getString("query_prof_referral_pattern_clm_line_coa").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)
					val botPcrFnclMbuCfQuery = config.getString("query_prof_referral_pattern_bot_pcr_fncl_mbu_cf").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)
					val botPcrFnclProdCfQuery = config.getString("query_prof_referral_pattern_bot_pcr_fncl_prod_cf").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)
					val mbrQuery = config.getString("query_prof_referral_pattern_mbr").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)
					val mdmPpltnXwalkQuery = config.getString("query_prof_referral_pattern_mdm_ppltn_xwalk").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)
					val mbrAdrsHistQuery = config.getString("query_prof_referral_pattern_mbr_adrs_hist").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)
					val srcCmsNppesQuery = config.getString("query_prof_referral_pattern_src_cms_nppes").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)
					val bkbnIpBvQuery = config.getString("query_prof_referral_pattern_bkbn_ip_bv").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)

					println(s"[NDO-ETL] Query for reading data from clmMaxRvsn table is $clmMaxRvsnQuery")
					println(s"[NDO-ETL] Query for reading data from clmLine table is $clmLineQuery")
					println(s"[NDO-ETL] Query for reading data from clm table is $clmQuery")
					println(s"[NDO-ETL] Query for reading data from clm_line_coa table is $clmLineCoaQuery")
					println(s"[NDO-ETL] Query for reading data from bot_pcr_fncl_mbu_cf table is $botPcrFnclMbuCfQuery")
					println(s"[NDO-ETL] Query for reading data from bot_pcr_fncl_prod_cf table is $botPcrFnclProdCfQuery")
					println(s"[NDO-ETL] Query for reading data from mbr table is $mbrQuery")
					println(s"[NDO-ETL] Query for reading data from mdm_ppltn_xwalk table is $mdmPpltnXwalkQuery")
					println(s"[NDO-ETL] Query for reading data from mbr_adrs_hist table is $mbrAdrsHistQuery")
					println(s"[NDO-ETL] Query for reading data from src_cms_nppes table is $srcCmsNppesQuery")
					println(s"[NDO-ETL] Query for reading data from bkbn_ip_bv table is $bkbnIpBvQuery")

					val clmMaxRvsnDF = spark.sql(clmMaxRvsnQuery)
					val clmLineDF = spark.sql(clmLineQuery)
					val clmDF = spark.sql(clmQuery)
					val clmLineCoaDF = spark.sql(clmLineCoaQuery)
					val botPcrFnclMbuCfDF = spark.sql(botPcrFnclMbuCfQuery)
					val botPcrFnclProdCfDF = spark.sql(botPcrFnclProdCfQuery)
					val mbrDF = spark.sql(mbrQuery)
					val mdmPpltnXwalkDF = spark.sql(mdmPpltnXwalkQuery)
					val mbrAdrsHistDF = spark.sql(mbrAdrsHistQuery)
					val srcCmsNppesDF = spark.sql(srcCmsNppesQuery)
					val bkbnIpBvDF = spark.sql(bkbnIpBvQuery)

					val outputMap = Map(
							"clm_max_rvsn" -> clmMaxRvsnDF,
							"clm_line" -> clmLineDF,
							"clm" -> clmDF,
							"clm_line_coa" -> clmLineCoaDF,
							"bot_pcr_fncl_mbu_cf" -> botPcrFnclMbuCfDF,
							"bot_pcr_fncl_prod_cf" -> botPcrFnclProdCfDF,
							"mbr" -> mbrDF,
							"mdm_ppltn_xwalk" -> mdmPpltnXwalkDF,
							"mbr_adrs_hist" -> mbrAdrsHistDF,
							"src_cms_nppes" -> srcCmsNppesDF,
							"bkbn_ip_bv" -> bkbnIpBvDF
				      )

					outputMap
	}

	def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
			val startTime = DateTime.now
					println(s"[NDO-ETL] Processing Data Started: $startTime")

					val clmMaxRvsn = inMapDF.getOrElse("clm_max_rvsn", null)
					val clmLine = inMapDF.getOrElse("clm_line", null)
					val clm = inMapDF.getOrElse("clm", null)
					val clmLineCoa = inMapDF.getOrElse("clm_line_coa", null)
					val botPcrFnclMbuCf = inMapDF.getOrElse("bot_pcr_fncl_mbu_cf", null)
					val botPcrFnclProdCf = inMapDF.getOrElse("bot_pcr_fncl_prod_cf", null)
					val mbr = inMapDF.getOrElse("mbr", null)
					val mdmPpltnXwalk = inMapDF.getOrElse("mdm_ppltn_xwalk", null)
					val mbrAdrsHist = inMapDF.getOrElse("mbr_adrs_hist", null)
					val srcCmsNppes = inMapDF.getOrElse("src_cms_nppes", null)
					val bkbnIpBv = inMapDF.getOrElse("bkbn_ip_bv", null)

					val snapNbr = getSnapNbr(inMapDF)

					var adjdctnStrtDt = ""
					var adjdctnEndDt = ""
					var clmLineSrvcStrtDt = ""
					var clmLineSrvcEndDt = ""

					try {
						//Date params read from config file
						adjdctnStrtDt = config.getString("adjdctn_start_dt")
								adjdctnEndDt = config.getString("adjdctn_end_dt")
								clmLineSrvcStrtDt = config.getString("clm_line_srvc_strt_dt")
								clmLineSrvcEndDt = config.getString("clm_line_srvc_end_dt")
								
								println(s"[NDO-ETL] The configuration found for  adjdctnStrtDt is $adjdctnStrtDt")
								println(s"[NDO-ETL] The configuration found for  adjdctnEndDt is $adjdctnEndDt")
								println(s"[NDO-ETL] The configuration found for  clmLineSrvcStrtDt is $clmLineSrvcStrtDt")
								println(s"[NDO-ETL] The configuration found for  clmLineSrvcEndDt is $clmLineSrvcEndDt")
								
					} catch {

					case e: ConfigException =>
					println(s"[NDO-ETL] No configuration found for the date params or a parameter is missing in properties file. Deriving the date params from scala code. : $e")

					adjdctnStrtDt = getAdjdctnStrtDt(snapNbr)
					adjdctnEndDt = getAdjdctnEndDt(snapNbr)
					clmLineSrvcStrtDt = getClmLineSrvcStrtDt(snapNbr)
					clmLineSrvcEndDt = getClmLineSrvcEndDt(snapNbr)
					}
					
					val tabProf = config.getString("tab_prof")
					val tabProfNppes = config.getString("tab_prof_nppes")
					val tabProfPdl = config.getString("tab_prof_pdl")
					val tabProfPdlNpi = config.getString("tab_prof_pdl_npi")
					val tabProfPdlTax = config.getString("tab_prof_pdl_tax")
					

			val f = for {

				////-------------------------------- File a -------------------------------------------
				yue2017ProfDf <- yue2017Prof(clmMaxRvsn, clmLine, clm, clmLineCoa, botPcrFnclMbuCf, botPcrFnclProdCf, mbr, mdmPpltnXwalk, mbrAdrsHist, adjdctnStrtDt, adjdctnEndDt, clmLineSrvcStrtDt, clmLineSrvcEndDt)

				yueProfLineDf <- yueProfLine(yue2017ProfDf, clmLine, clmLineCoa)

				yueProfLobDf <- yueProfLob(yueProfLineDf, botPcrFnclMbuCf, botPcrFnclProdCf)

				yueProfFirstDFDf <- yueProfFirstDF(yueProfLineDf)

				yueProfLastDFdf <- yueProfLastDF(yueProfLineDf)

				yueProfClmDf <- yueProfClm(yue2017ProfDf, yueProfFirstDFDf, yueProfLastDFdf, yueProfLobDf)

				yueProfMcid <- yueProfMcid(mdmPpltnXwalk, yueProfClmDf, mbr)

				yueProfDF <- yueProfDF(yueProfMcid, mbrAdrsHist)
				WriteyueProfDF <- WriteManagedTblInsertOverwrite(yueProfDF, stagingHiveDB, tabProf)

				////-------------------------------- File b -------------------------------------------
				ProfNppes <- ProfNppes(srcCmsNppes)
				WriteyueProfDF <- WriteManagedTblInsertOverwrite(ProfNppes, stagingHiveDB, tabProfNppes)

				////-------------------------------- File c -------------------------------------------
				yueProfPdl <- yueProfPdl(bkbnIpBv,snapNbr)
				WriteyueProfDF <- WriteManagedTblInsertOverwrite(yueProfPdl, stagingHiveDB, tabProfPdl)

				yueProfPdlNpi <- yueProfPdlNpi(bkbnIpBv,snapNbr)
				WriteyueProfDF <- WriteManagedTblInsertOverwrite(yueProfPdlNpi, stagingHiveDB, tabProfPdlNpi)

				yueProfPdlTax <- yueProfPdlTax(bkbnIpBv,snapNbr)
				WriteyueProfDF <- WriteManagedTblInsertOverwrite(yueProfPdlTax, stagingHiveDB, tabProfPdlTax)

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

	def getAdjdctnStrtDt(snapNbr: Long): String =
		{

				val tobeDerived = snapNbr.toString
						val formatter1 = new SimpleDateFormat("yyyyMM", Locale.ENGLISH);
				val formatter2 = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				val snapNbrFormated = formatter1.parse(tobeDerived)

						val calender = Calendar.getInstance();
				calender.setTime(snapNbrFormated);
				calender.add(Calendar.YEAR, -1)
				calender.add(Calendar.MONTH, -2)
				val adjdctnStrtDtNotFormatted = calender.getTime();
				val adjdctnStrtDt = formatter2.format(adjdctnStrtDtNotFormatted)

						println(s"[NDO-ETL] After formatting adjdctnStrtDt : $adjdctnStrtDt")

						adjdctnStrtDt
		}

	def getAdjdctnEndDt(snapNbr: Long): String =
		{

				val tobeDerived = snapNbr.toString
						val formatter1 = new SimpleDateFormat("yyyyMM", Locale.ENGLISH);
				val formatter2 = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				val snapNbrFormated = formatter1.parse(tobeDerived)

						val calender = Calendar.getInstance();
				calender.setTime(snapNbrFormated);
				calender.add(Calendar.MONTH, 1)
				calender.add(Calendar.DAY_OF_MONTH, -1)
				val adjdctnEndDtNotFormatted = calender.getTime();
				val adjdctnEndDtd = formatter2.format(adjdctnEndDtNotFormatted)

						println(s"[NDO-ETL] After formatting adjdctnEndDtd $adjdctnEndDtd")

						adjdctnEndDtd
		}

	def getClmLineSrvcStrtDt(snapNbr: Long): String =
		{

				val tobeDerived = snapNbr.toString
						val formatter1 = new SimpleDateFormat("yyyyMM", Locale.ENGLISH);
				val formatter2 = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				val snapNbrFormated = formatter1.parse(tobeDerived)

						val calender = Calendar.getInstance();
				calender.setTime(snapNbrFormated);
				calender.add(Calendar.YEAR, -1)
				calender.add(Calendar.MONTH, -2)
				val clmLineSrvcStrtDtNotFormatted = calender.getTime();
				val clmLineSrvcStrtDt = formatter2.format(clmLineSrvcStrtDtNotFormatted)

						println(s"[NDO-ETL] After formatting clmLineSrvcStrtDt $clmLineSrvcStrtDt")

						clmLineSrvcStrtDt
		}

	def getClmLineSrvcEndDt(snapNbr: Long): String =
		{

				val tobeDerived = snapNbr.toString
						val formatter1 = new SimpleDateFormat("yyyyMM", Locale.ENGLISH);
				val formatter2 = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				val snapNbrFormated = formatter1.parse(tobeDerived)

						val calender = Calendar.getInstance();
				calender.setTime(snapNbrFormated);
				calender.add(Calendar.MONTH, -2)
				calender.add(Calendar.DAY_OF_MONTH, -1)
				val clmLineSrvcEndDtNotFormatted = calender.getTime();
				val clmLineSrvcEndDt = formatter2.format(clmLineSrvcEndDtNotFormatted)

						println(s"[NDO-ETL] After formatting clmLineSrvcEndDt $clmLineSrvcEndDt")

						clmLineSrvcEndDt
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


