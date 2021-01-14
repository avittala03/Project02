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

class ControlTotalOperationEhoppa(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

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
					println(s"[NDO-ETL] Reading the queries from config file")

					val ndoEhoppaBaseQuery = config.getString("query_ndo_ehoppa_base").replaceAll(ConfigKey.warehouseDBPlaceHolder, warehouseHiveDB).toLowerCase()
					val ndoEhoppaBaseMbrCntyQuery = config.getString("query_ndo_ehoppa_base_mbr_cnty").replaceAll(ConfigKey.warehouseDBPlaceHolder, warehouseHiveDB).toLowerCase()
					val ndoEhoppaBaseMbrQuery = config.getString("query_ndo_ehoppa_base_mbr").replaceAll(ConfigKey.warehouseDBPlaceHolder, warehouseHiveDB).toLowerCase()
					

					println(s"[NDO-ETL] Query for reading data from ndo_ehoppa_base table is $ndoEhoppaBaseQuery")
					println(s"[NDO-ETL] Query for reading data from ndo_ehoppa_base_mbr_cnty table is $ndoEhoppaBaseMbrCntyQuery")
					println(s"[NDO-ETL] Query for reading data from ndo_ehoppa_base_mbr table is $ndoEhoppaBaseMbrQuery")
					

					val ndoEhoppaBaseDf = spark.sql(ndoEhoppaBaseQuery)
					val ndoEhoppaBaseMbrCntyDf = spark.sql(ndoEhoppaBaseMbrCntyQuery)
					val ndoEhoppaBaseMbrDf = spark.sql(ndoEhoppaBaseMbrQuery)
					

					val mapDF = Map(
							"ndo_ehoppa_base" -> ndoEhoppaBaseDf,
							"ndo_ehoppa_base_mbr_cnty" -> ndoEhoppaBaseMbrCntyDf,
							"ndo_ehoppa_base_mbr" -> ndoEhoppaBaseMbrDf)
							

					println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					mapDF

	}

	def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {

			val startTime = DateTime.now
					println(s"[NDO-ETL] Processing Data Started: $startTime")

					//Reading the data frames as elements from Map

					println(s"[NDO-ETL] Reading the table names for Control M table load.")

					val tabndoEhoppaBase = config.getString("tab_ndo_ehoppa_base")
					val tabndoEhoppaBaseMbrCnty = config.getString("tab_ndo_ehoppa_base_mbr_cnty")
					val tabndoEhoppaBaseMbr = config.getString("tab_ndo_ehoppa_base_mbr")
					val tabndoEhoppaCntrl = config.getString("tab_ndo_ehoppa_cntrl")
				
					println(s"[NDO-ETL] Reading the dataframes for source table data.")

					val ndoEhoppaBaseDf = inMapDF.getOrElse(tabndoEhoppaBase, null)
					val ndoEhoppaBaseMbrCntyDf = inMapDF.getOrElse(tabndoEhoppaBaseMbrCnty, null)
					val ndoEhoppaBaseMbrDf = inMapDF.getOrElse(tabndoEhoppaBaseMbr, null)
					

					val mapDF = loadSourceDataFromTD()

					val inpatientSummaryDf = mapDF.getOrElse("inpatient_summary", null)
					val facilityAttributeProfileDf = mapDF.getOrElse("facility_attribute_profile", null)
					val outpatientSummaryDf = mapDF.getOrElse("outpatient_summary", null)

					println(s"[NDO-ETL] Reading the Range start and end dates")

					val map = getDateRange()
					val rangeEndDate = map.getOrElse("rangeEndDate", null) 
					val rangeStartDate = map.getOrElse("rangeStartDate", null) 

					val inpatExclAlwdAmnt = getSumAlwdAmt(inpatientSummaryDf, facilityAttributeProfileDf, rangeStartDate, rangeEndDate)
					println(s"[NDO-ETL] Inpat Allowed Amount excluding the filters is $inpatExclAlwdAmnt")

					val outpatExclAlwdAmnt = getSumAlwdAmt(outpatientSummaryDf, facilityAttributeProfileDf, rangeStartDate, rangeEndDate)
					println(s"[NDO-ETL] Outpat Allowed Amount excluding the filters is $outpatExclAlwdAmnt")

					val allwdExcludedAmt = inpatExclAlwdAmnt + outpatExclAlwdAmnt
					println(s"[NDO-ETL] Sum of both the excluded allowed amounts is $allwdExcludedAmt")

			    spark.sql(s"truncate table $warehouseHiveDB.$tabndoEhoppaCntrl")
			
					var toBeInsertedDf: DataFrame = null

					inMapDF.foreach(x => {
						val tableName = x._1

									println(s"[NDO-ETL] The table name is $tableName")
									println(s"[NDO-ETL] The SNAP_YEAR_MNTH_NBR for $tableName = $rangeEndDate")

									if (tableName.equalsIgnoreCase(tabndoEhoppaBase)) { toBeInsertedDf = ndoEhoppaBaseDf }
									else if (tableName.equalsIgnoreCase(tabndoEhoppaBaseMbr)) { toBeInsertedDf = ndoEhoppaBaseMbrDf }
									else if (tableName.equalsIgnoreCase(tabndoEhoppaBaseMbrCnty)) { toBeInsertedDf = ndoEhoppaBaseMbrCntyDf }

									val controlInsertDf = toBeInsertedDf.select(
											$"inpat_allwd_amt",
											$"outpat_allwd_amt",
											$"TOTAL_ALLWD_AMT").agg(
													sum($"inpat_allwd_amt").alias("INPAT_ALWD_AMT"),
													sum($"outpat_allwd_amt").alias("OUTPAT_ALWD_AMT"),
													sum($"TOTAL_ALLWD_AMT").alias("TOTAL_ALWD_AMT"))
											.withColumn("SNAP_YEAR_MNTH_NBR", lit(rangeEndDate))
											.withColumn("CNTRL_TYPE_NM", lit(tableName)).
											select(
													$"SNAP_YEAR_MNTH_NBR",
													$"CNTRL_TYPE_NM",
													$"INPAT_ALWD_AMT",
													(lit(inpatExclAlwdAmnt)).alias("INPAT_ALLWD_EXCLD_AMT"),
													$"OUTPAT_ALWD_AMT",
													(lit(outpatExclAlwdAmnt)).alias("OUTPAT_ALLWD_EXCLD_AMT"),
													$"TOTAL_ALWD_AMT",
													(lit(allwdExcludedAmt)).alias("TOTAL_ALLWD_EXCLD_AMT"),
													($"TOTAL_ALWD_AMT" + lit(allwdExcludedAmt)).alias("TOTAL_EXPCT_ALLWD_AMT"))
											.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
											withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
											withColumn(lastUpdatedDate, lit(current_timestamp())).
											withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser))
											
											println(s"Write started for appending the data to final  table $warehouseHiveDB.$tabndoEhoppaCntrl at:" + DateTime.now())
											
											controlInsertDf.write.mode("append").insertInto(warehouseHiveDB + """.""" + tabndoEhoppaCntrl)
											println(s"[NDO-ETL] Table loaded as $warehouseHiveDB.$tabndoEhoppaCntrl")
							
					})

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

	def getSumAlwdAmt(sourceTableDf: DataFrame, facilityAttributeProfileDf: DataFrame, rangeStartDate: String, rangeEndDate: String): BigDecimal = {
			val startTime = DateTime.now()
					println(s"[HPIP-ETL] Derivation of excluded allowed amounts started: $startTime")
			
					val joinFacility1 = sourceTableDf.join(facilityAttributeProfileDf,
							trim(facilityAttributeProfileDf("Medcr_ID")) === trim(sourceTableDf("Medcr_ID")), "inner")
					.filter(trim(sourceTableDf("Prov_ST_NM")).isNull ||
							trim(sourceTableDf("Prov_ST_NM")) == ("") ||
							trim(sourceTableDf("prov_county")).isNull ||
							trim(sourceTableDf("MBUlvl2")).isNull ||
							trim(sourceTableDf("prodlvl3")).isNull ||
							trim(sourceTableDf("MCS")) === ("Y") ||
							trim(sourceTableDf("liccd")).!==("01") ||
							trim(sourceTableDf("liccd")).isNull ||
							trim(sourceTableDf("CLM_LINE_ENCNTR_CD")).isin("N","NA","UNK")===false ||
							trim(sourceTableDf("brand")).!==(concat(lit("Blue "),trim(sourceTableDf("MBR_state")))) ||
							sourceTableDf("MBU_CF_CD").isin("MCKYGP", "SRLGUN", "SRLGCO", "SRLGVA", "MCOHGP", "SRLGNV",
									"MCINGP", "SRLGME", "SRLGNH", "SRLGKS", "SRLGGA", "SRLGNA", "SRNAHS", "SRLGMO", "SRLGWI",
									"SRLGNY", "SRLGCT", "SRLGBC", "MJCAMC", "SPCAMC") ||
							facilityAttributeProfileDf("factype").isin("Delete", "Long Term Care Hospital", "Snf", "Physical Rehab"))
					.agg((sum(sourceTableDf("ALWD_AMT"))).alias("ALWD_AMT_AGG"))
					.select($"ALWD_AMT_AGG").head.getDecimal(0)
					
					val joinFacility2 = sourceTableDf.join(facilityAttributeProfileDf,
							trim(facilityAttributeProfileDf("Medcr_ID")) === trim(sourceTableDf("Medcr_ID")), "left_outer")
					.filter(trim(facilityAttributeProfileDf("Medcr_ID")).isNull &&
							$"Inc_Month".between(rangeStartDate, rangeEndDate))
					.agg((sum(sourceTableDf("ALWD_AMT"))).alias("ALWD_AMT_AGG"))
					.select($"ALWD_AMT_AGG").head.getDecimal(0)
						
					val sourceAlwdAmt = joinFacility1.add(joinFacility2)

					println(s"[HPIP-ETL] Derivation of excluded allowed amounts Completed at: " + DateTime.now())
					println(s"[HPIP-ETL] Time Taken for completion :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					sourceAlwdAmt

	}

	def loadSourceDataFromTD(): Map[String, DataFrame] = {

			val facilityattributeprofileQuery = config.getString("query_facility_attribute_profile").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val outpatientsummaryQuery = config.getString("query_outpatient_summary").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val inpatientsummaryQuery = config.getString("query_inpatient_summary").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()

					println(s"[NDO-ETL] Query for reading data from facility_attribute_profile table is $facilityattributeprofileQuery")
					println(s"[NDO-ETL] Query for reading data from outpatient_summary table is $outpatientsummaryQuery")
					println(s"[NDO-ETL] Query for reading data from inpatient_summary table is $inpatientsummaryQuery")

					val outpatientSummary_src_hiveDf = spark.sql(outpatientsummaryQuery)
					val inpatientSummary_src_hiveDf = spark.sql(inpatientsummaryQuery)
					val facilityAttributeProfile_src_hiveDf = spark.sql(facilityattributeprofileQuery)

					println(s"[NDO-ETL] Trimming all the columns of outpatient_summary table")
					
					
					val outpatientSummaryDf = outpatientSummary_src_hiveDf
					.select(upper(trim($"MBR_County")).alias("MBR_County"),
							trim($"MBR_State").alias("MBR_State"),
							trim($"MBR_ZIP3").alias("MBR_ZIP3"),
							trim($"INN_CD").alias("INN_CD"),
							trim($"brand").alias("brand"),
							trim($"MEDCR_ID").alias("MEDCR_ID"),
							trim($"liccd").alias("liccd"),
							trim($"PROV_ST_NM").alias("PROV_ST_NM"),
							trim($"PROV_ZIP_CD").alias("PROV_ZIP_CD"),
							upper(trim($"prov_county")).alias("prov_county"),
							trim($"ER_Flag").alias("ER_Flag"),
							trim($"EXCHNG_IND_CD").alias("EXCHNG_IND_CD"),
							trim($"cat1").alias("cat1"),
							trim($"cat2").alias("cat2"),
							trim($"MBUlvl2").alias("MBUlvl2"),
							trim($"MBUlvl3").alias("MBUlvl3"),
							trim($"MBUlvl4").alias("MBUlvl4"),
							trim($"MBU_CF_CD").alias("MBU_CF_CD"),
							trim($"prodlvl3").alias("prodlvl3"),
							trim($"fundlvl2").alias("fundlvl2"),
							trim($"system_id").alias("system_id"),
							trim($"MCS").alias("MCS"),
							trim($"Inc_Month").alias("Inc_Month"),
							trim($"PROV_NM").alias("PROV_NM"),
							trim($"CLM_LINE_ENCNTR_CD").alias("CLM_LINE_ENCNTR_CD"),
							($"ALWD_AMT"),
							($"BILLD_CHRG_AMT"),
							($"CASES"),
							($"CMAD"),
							($"CMAD_ALLOWED"),
							($"CMAD_CASES"),
							($"PAID_AMT"))

					val inpatientSummaryDf = inpatientSummary_src_hiveDf.select(upper(trim($"MBR_County")).alias("MBR_County"),
							trim($"MBR_State").alias("MBR_State"),
							trim($"brand").alias("brand"),
							trim($"MEDCR_ID").alias("MEDCR_ID"),
							trim($"liccd").alias("liccd"),
							trim($"PROV_ST_NM").alias("PROV_ST_NM"),
							trim($"PROV_ZIP_CD").alias("PROV_ZIP_CD"),
							upper(trim($"prov_county")).alias("prov_county"),
							trim($"ER_Flag").alias("ER_Flag"),
							trim($"EXCHNG_IND_CD").alias("EXCHNG_IND_CD"),
							trim($"FNL_DRG_CD").alias("FNL_DRG_CD"),
							trim($"INN_CD").alias("INN_CD"),
							trim($"Inc_Month").alias("Inc_Month"),
							trim($"cat1").alias("cat1"),
							trim($"cat2").alias("cat2"),
							trim($"MBUlvl2").alias("MBUlvl2"),
							trim($"MBUlvl3").alias("MBUlvl3"),
							trim($"MBUlvl4").alias("MBUlvl4"),
							trim($"MBU_CF_CD").alias("MBU_CF_CD"),
							trim($"fundlvl2").alias("fundlvl2"),
							trim($"prodlvl3").alias("prodlvl3"),
							trim($"system_id").alias("system_id"),
							trim($"MCS").alias("MCS"),
							trim($"PROV_NM").alias("PROV_NM"),
							trim($"MBR_ZIP_CD").alias("MBR_ZIP_CD"),
							trim($"CLM_LINE_ENCNTR_CD").alias("CLM_LINE_ENCNTR_CD"),
							($"ALWD_AMT"),
							($"BILLD_CHRG_AMT"),
							($"CASES"),
							($"CMAD"),
							($"CMAD_ALLOWED"),
							($"CMAD_CASES"),
							($"PAID_AMT"))

					val facilityAttributeProfileDf = facilityAttributeProfile_src_hiveDf
					.select(trim($"MEDCR_ID").alias("MEDCR_ID"),
							trim($"Hospital").alias("Hospital"),
							trim($"factype").alias("factype"),
							trim($"Rating_Area").alias("Rating_Area"))
							
					val mapDF = Map(
							"inpatient_summary" -> inpatientSummaryDf,
							"facility_attribute_profile" -> facilityAttributeProfileDf,
							"outpatient_summary" -> outpatientSummaryDf)

					mapDF
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
