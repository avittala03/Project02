package com.am.ndo.phrmcyCost

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.{ functions, Column, DataFrame, SQLContext }
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.Minutes
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Locale
import org.apache.spark.sql.Row

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

class PhrmcyCostOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

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

    val phrmcyClmLineQuery = config.getString("query_phrmcy_clm_line").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val clmLineQuery = config.getString("query_clm_line").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val clmQuery = config.getString("query_clm").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val clmLinecoaQuery = config.getString("query_clm_line_coa").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val phrmcyClmQuery = config.getString("query_phrmcy_clm").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val glCfMbuHrzntlHrchyQuery = config.getString("query_gl_cf_mbu_hrzntl_hrchy").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val fnclProdCfQuery = config.getString("query_fncl_prod_cf_phrmcy").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val fnclMbuCfQuery = config.getString("query_fncl_mbu_cf_phrmcy").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()

    println(s"[NDO-ETL] Query for reading data from Pharmacy Claim Line table is $phrmcyClmLineQuery")
    println(s"[NDO-ETL] Query for reading data from Claim Line table is $clmLineQuery")
    println(s"[NDO-ETL] Query for reading data from Claim table is $clmQuery")
    println(s"[NDO-ETL] Query for reading data from Claim Line COA table is $clmLinecoaQuery")
    println(s"[NDO-ETL] Query for reading data from Pharmacy Claim table is $phrmcyClmQuery")
    println(s"[NDO-ETL] Query for reading data from gl_cf_mbu_hrzntl_hrchy table is $glCfMbuHrzntlHrchyQuery")
    println(s"[NDO-ETL] Query for reading data from fncl_prod_cf table is $fnclProdCfQuery")
    println(s"[NDO-ETL] Query for reading data from fncl_mbu_cf table is $fnclMbuCfQuery")

    val phrmcyClmLineDf = spark.sql(phrmcyClmLineQuery)
    val clmLineDf = spark.sql(clmLineQuery)
    val clmDf = spark.sql(clmQuery)
    val clmLinecoaDf = spark.sql(clmLinecoaQuery)
    val phrmcyClmDf = spark.sql(phrmcyClmQuery)
    val glCfMbuHrzntlHrchyDf = spark.sql(glCfMbuHrzntlHrchyQuery)
    val fnclProdCfDf = spark.sql(fnclProdCfQuery)
    val fnclMbuCfDf = spark.sql(fnclMbuCfQuery)

    val mapDF = Map(

      "phrmcy_clm_line" -> phrmcyClmLineDf,
      "clm_line" -> clmLineDf,
      "clm" -> clmDf,
      "clm_line_coa" -> clmLinecoaDf,
      "phrmcy_clm" -> phrmcyClmDf,
      "gl_cf_mbu_hrzntl_hrchy" -> glCfMbuHrzntlHrchyDf,
      "fncl_prod_cf" -> fnclProdCfDf,
      "fncl_mbu_cf" -> fnclMbuCfDf)

    println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
    println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    mapDF
  }

  def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
    val startTime = DateTime.now
    println(s"[NDO-ETL] Processing Data Started: $startTime")

    //Reading the data frames as elements from Map
    val phrmcyClmLineDf = inMapDF.getOrElse("phrmcy_clm_line", null)
    val clmLineDf = inMapDF.getOrElse("clm_line", null)
    val clmDf = inMapDF.getOrElse("clm", null)
    val clmLinecoaDf = inMapDF.getOrElse("clm_line_coa", null)
    val phrmcyClmDf = inMapDF.getOrElse("phrmcy_clm", null)
    val glCfMbuHrzntlHrchyDf = inMapDF.getOrElse("gl_cf_mbu_hrzntl_hrchy", null)
    val fnclProdCfDf = inMapDF.getOrElse("fncl_prod_cf", null)
    val fnclMbuCfDf = inMapDF.getOrElse("fncl_mbu_cf", null)

    val tabndoPhrmcyCost = config.getString("tab_ndo_phrmcy_cost")
    val tabndoPhrmcyCostBkp = config.getString("tab_ndo_phrmcy_cost_bkp")

    val maxrun_id = getMAxRunId(tabndoPhrmcyCost)

    val map = getDateRange()
    val rangeStartDate = map.getOrElse("rangeStartDate", null)
    val rangeEndDate = map.getOrElse("rangeEndDate", null)
    
    println(s"[NDO-ETL] Max run id is : $maxrun_id")
    println(s"[NDO-ETL] rangeStartDate: $rangeStartDate")
    println(s"[NDO-ETL] rangeEndDate: $rangeEndDate")

    val phrmcyClmLineFilDf = phrmcyClmLineDf.filter(phrmcyClmLineDf("trnsctn_stts_cd").isin("X", "P")
      && phrmcyClmLineDf("clm_sor_cd").isin("806", "807", "898", "1037", "512"))

    val clmFilDf = clmDf.filter(clmDf("rx_filled_dt").between(rangeStartDate, rangeEndDate))

   

    val phrmcyClmLineFilJoin = phrmcyClmLineFilDf.join(clmLineDf, phrmcyClmLineFilDf("CLM_ADJSTMNT_KEY") === clmLineDf("CLM_ADJSTMNT_KEY")
      && phrmcyClmLineFilDf("CLM_LINE_NBR") === clmLineDf("CLM_LINE_NBR"), "inner")
      .join(clmFilDf, clmLineDf("CLM_ADJSTMNT_KEY") === clmFilDf("CLM_ADJSTMNT_KEY")
        && clmLineDf("CLM_SOR_CD") === clmFilDf("CLM_SOR_CD")
        && clmLineDf("ADJDCTN_DT") === clmFilDf("ADJDCTN_DT"), "inner")
      .join(clmLinecoaDf, phrmcyClmLineFilDf("CLM_ADJSTMNT_KEY") === clmLinecoaDf("CLM_ADJSTMNT_KEY")
        && phrmcyClmLineFilDf("CLM_LINE_NBR") === clmLinecoaDf("CLM_LINE_NBR")
        && phrmcyClmLineFilDf("CLM_SOR_CD") === clmLinecoaDf("CLM_SOR_CD"), "inner")
      .join(phrmcyClmDf, phrmcyClmLineFilDf("CLM_ADJSTMNT_KEY") === phrmcyClmDf("CLM_ADJSTMNT_KEY")
        && phrmcyClmLineFilDf("CLM_SOR_CD") === phrmcyClmDf("CLM_SOR_CD"), "inner")
      .join(glCfMbuHrzntlHrchyDf, clmLinecoaDf("MBU_CF_CD") === glCfMbuHrzntlHrchyDf("MBU_CF_CD"), "inner")
      .join(fnclMbuCfDf, clmLinecoaDf("MBU_CF_CD") === fnclMbuCfDf("MBU_CF_CD"), "inner")
      .join(fnclProdCfDf, clmLinecoaDf("PROD_CF_CD") === fnclProdCfDf("PROD_CF_CD"), "inner")

    val ndoPhrmcyCostOutpt = phrmcyClmLineFilJoin.select(
      (when(trim(fnclMbuCfDf("BRND_DESC")) === ("BCC") && trim(fnclMbuCfDf("MBU_LVL_2_DESC")).isin("INDIVIDUAL", "SMALL GROUP", "LARGE GROUP", "BLUE CARD/NASCO PAR", "HOME") && trim(fnclProdCfDf("PROD_LVL_2_DESC")) === ("HMO"), lit("CA"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCC") && trim(fnclMbuCfDf("MBU_LVL_2_DESC")).isin("INDIVIDUAL", "SMALL GROUP", "LARGE GROUP", "BLUE CARD/NASCO PAR", "HOME") && trim(fnclProdCfDf("PROD_LVL_2_DESC")) === ("PPO"), lit("CA"))
        when (trim(phrmcyClmDf("SRC_CUST_ID")) === ("WLW"), lit("WCIC"))
        when (trim(clmLineDf("BNFT_PKG_ID")).isin("2908", "1X07", "2K4J", "2K4K"), lit("NH"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("PBMEME"), lit("ME"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("PBVAST"), lit("VA"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("PBCTAO", "PBCTZZ", "PBCTNA"), lit("CT"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("SHFLMD", "SHFLCH"), lit("FL"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("AGAZZZ", "SRAZPM", "SRAZMC"), lit("AZ"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("AGCAZZ", "SRCAMP"), lit("CA"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("AGCOZZ"), lit("CO"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("AGDCSS"), lit("DC"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("AGFLMA", "AGFLZZ", "AGAMBR"), lit("FL"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("AGGAMA", "AGGAZZ"), lit("GA"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("AGILZZ"), lit("IL"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("AGIAZZ"), lit("IA"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("AGINZZ", "MUCAWP"), lit("IN"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("SSUNKC", "SSUNKS", "AGKSZZ"), lit("KS"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("AGKYZZ"), lit("KY"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("AGLAZZ"), lit("LA"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("SSUNHS", "SSUNMA"), lit("MA"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("AGMDMA", "AGMDZZ", "AGDCZZ"), lit("MD"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("AGMEZZ"), lit("ME"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("AGMIZZ"), lit("MI"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("AGNJMA", "AGNJZZ"), lit("NJ"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("AGNMMA", "AGNMZZ"), lit("NM"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("AGNVZZ"), lit("NV"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("AGNYDE", "AGNYZZ", "AGNYMA", "AGNYXX"), lit("NY"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("SSHNNY"), lit("NY"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("AGOHZZ"), lit("OH"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("SSSCZZ", "AGSCZZ"), lit("SC"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("AGMEZZ"), lit("ME"))
        when (trim(fnclMbuCfDf("MBU_CF_CD")).isin("SSUNTP", "SSUNTC", "SSUNTS", "SSUNTN"), lit("TX"))
        when (trim(fnclMbuCfDf("MBU_CF_CD")).isin("AGTXSA", "AGTXDE", "AGTXLU", "AGAICW", "AGAICC", "AGTXFW", "AGTXEL", "AGTXMA", "AGTXHU", "AGTXBE", "AGTXAU", "AGAICN", "AGTXDA", "AGTXCC", "AGTXEX"), lit("TX"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("AGVAMA", "AGVAEL", "AGVAZZ"), lit("VA"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("AGWAMA", "AGWAZZ"), lit("WA"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("AGWIZZ", "SSWIZZ"), lit("WI"))
        when (trim(clmLinecoaDf("MBU_CF_CD")).isin("SSUNWV", "SSWVEX"), lit("WV"))
        when (trim(clmLinecoaDf("MBU_CF_CD")) === ("SSARZZ"), lit("AR"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCBSCO"), lit("CO"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCBSCT"), lit("CT"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCBSGA"), lit("GA"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCBSIN"), lit("IN"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCBSKY"), lit("KY"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCBSME"), lit("ME"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCBSMO"), lit("MO"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCBSNH"), lit("NH"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCBSNV"), lit("NV"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCBSNY"), lit("NY"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCBSOH"), lit("OH"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCBSVA"), lit("VA"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCBSWI"), lit("WI"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("BCC"), lit("CA"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("CO/NV"), lit("CO"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("OTHER"), lit("OTHER"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("UNICARE"), lit("UN"))
        when (trim(fnclMbuCfDf("BRND_DESC")) === ("UNKWN"), lit("OTHER"))
        otherwise (lit("OTHER"))).alias("STATE"),
      glCfMbuHrzntlHrchyDf("GL_LVL_4_DESC").alias("LOB"),
      fnclProdCfDf("PROD_LVL_2_DESC").alias("PROD_ID"),
      fnclMbuCfDf("MBU_LVL_2_DESC").alias("BSNS_SGMNT"),
      phrmcyClmLineFilDf("APRVD_INGRED_AMT"),
      phrmcyClmLineFilDf("APRVD_FEE_AMT"),
      phrmcyClmLineFilDf("APRVD_SALES_TAX_AMT")).groupBy(
        $"STATE",
        $"LOB",
        $"PROD_ID",
        $"BSNS_SGMNT").agg(
          sum(phrmcyClmLineFilDf("APRVD_INGRED_AMT") + phrmcyClmLineFilDf("APRVD_FEE_AMT") + phrmcyClmLineFilDf("APRVD_SALES_TAX_AMT")).alias("ALWD_AMT"))
      .drop(phrmcyClmLineFilDf("APRVD_INGRED_AMT"))
      .drop(phrmcyClmLineFilDf("APRVD_FEE_AMT"))
      .drop(phrmcyClmLineFilDf("APRVD_SALES_TAX_AMT"))

 
    val ndoPhrmcyCost = ndoPhrmcyCostOutpt.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
      withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
      withColumn(lastUpdatedDate, lit(current_timestamp())).
      withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser))
      .withColumn("run_id", lit(maxrun_id))

   

    writeTblInsertOverwrite(ndoPhrmcyCost, warehouseHiveDB, tabndoPhrmcyCost)
    writeTblAppendOverwrite(ndoPhrmcyCost, warehouseHiveDB, tabndoPhrmcyCostBkp)



    runIDInsert(tabndoPhrmcyCost, maxrun_id)
   

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
							rangeStartDate = formatter3.format(priorEndNotFormatted) + "-01" + "-01"
							println("rangeStartDate" + rangeStartDate)
							rangeEndDate = formatter3.format(priorEndNotFormatted) + "-12"+ "-31"
							println("rangeStartDate" + rangeEndDate)

						} else {
							calender.add(Calendar.YEAR, -1)
							priorEndNotFormatted = calender.getTime()
							rangeStartDate = formatter3.format(priorEndNotFormatted) + "-01" + "-01"
							println("rangeStartDate" + rangeStartDate)
							rangeEndDate = formatter3.format(priorEndNotFormatted) + "-12"+ "-31"
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
