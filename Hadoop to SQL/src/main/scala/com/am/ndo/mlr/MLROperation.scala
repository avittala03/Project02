package com.am.ndo.mlr

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
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

class MLROperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

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
    val factRsttdMbrshpQuery = config.getString("query_fact_rsttd_mbrshp").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val factRsttdRvnuQuery = config.getString("query_fact_rsttd_rvnu").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val factRsttdExpnsQuery = config.getString("query_fact_rsttd_expns").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val ddimSrvcareaQuery = config.getString("query_ddim_srvcarea").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val ddimBnftPlanQuery = config.getString("query_ddim_bnft_plan").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val ddimProvTaxIdQuery = config.getString("query_ddim_prov_tax_id").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val ddimHccQuery = config.getString("query_ddim_hcc").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()

    println(s"[NDO-ETL] Query for reading data from factRsttdMbrshpQuery table is $factRsttdMbrshpQuery")
    println(s"[NDO-ETL] Query for reading data from factRsttdRvnuQuery table is $factRsttdRvnuQuery")
    println(s"[NDO-ETL] Query for reading data from factRsttdExpnsQuery table is $factRsttdExpnsQuery")
    println(s"[NDO-ETL] Query for reading data from ddimSrvcareaQuery table is $ddimSrvcareaQuery")
    println(s"[NDO-ETL] Query for reading data from ddimBnftPlanQuery table is $ddimBnftPlanQuery")
    println(s"[NDO-ETL] Query for reading data from ddimProvTaxIdQuery table is $ddimProvTaxIdQuery")
    println(s"[NDO-ETL] Query for reading data from ddimHccQuery table is $ddimHccQuery")

    val factRsttdMbrshpDf = spark.sql(factRsttdMbrshpQuery)

    val factRsttdRvnuDf = spark.sql(factRsttdRvnuQuery)

    val factRsttdExpnsDf = spark.sql(factRsttdExpnsQuery)

    val ddimSrvcareaDf = spark.sql(ddimSrvcareaQuery)

    val ddimBnftPlanDf = spark.sql(ddimBnftPlanQuery)

    val ddimProvTaxIdDf = spark.sql(ddimProvTaxIdQuery)

    val ddimHccDf = spark.sql(ddimHccQuery)

    val mapDF = Map("fact_rsttd_mbrshp" -> factRsttdMbrshpDf, "fact_rsttd_rvnu" -> factRsttdRvnuDf, "fact_rsttd_expns" -> factRsttdExpnsDf,
      "ddim_srvcarea" -> ddimSrvcareaDf,
      "ddim_bnft_plan" -> ddimBnftPlanDf, "ddim_prov_tax_id" -> ddimProvTaxIdDf, "ddim_hcc" -> ddimHccDf)

    println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
    println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    mapDF
    

  }

  def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
    val startTime = DateTime.now
    println(s"[NDO-ETL] Processing Data Started: $startTime")
    //Reading the data frames as elements from Map
    val factRsttdMbrshpDf = inMapDF.getOrElse("fact_rsttd_mbrshp", null)
    val factRsttdRvnuDf = inMapDF.getOrElse("fact_rsttd_rvnu", null)
    val factRsttdExpnsDf = inMapDF.getOrElse("fact_rsttd_expns", null)
    val ddimSrvcareaDf = inMapDF.getOrElse("ddim_srvcarea", null)
    val ddimBnftPlanDf = inMapDF.getOrElse("ddim_bnft_plan", null)
    val ddimProvTaxIdDf = inMapDF.getOrElse("ddim_prov_tax_id", null)
    val ddimHccDf = inMapDF.getOrElse("ddim_hcc", null)
    val lastUpdatedDate = config.getString(ConfigKey.auditColumnName)

    //Date params read from config file
    val snapNbr = getSnapNbr(inMapDF)
    var priorBeginYyyymm = ""
    var priorEndYyyymm = ""
    var currentBeginYyyymm = ""
    var currentEndYyyymm = ""
    try {
      priorBeginYyyymm = config.getString("prior_begin_yyyymm")
      priorEndYyyymm = config.getString("prior_end_yyyymm")
      currentBeginYyyymm = config.getString("current_begin_yyyymm")
      currentEndYyyymm = config.getString("current_end_yyyymm")
    } catch {

      case e: ConfigException =>
        println(s"[NDO-ETL] No configuration found for the date params or a parameter is missing in properties file. Deriving the date params from scala code. : $e")

        //					//Date params read from config file
        priorBeginYyyymm = getPriorBegin(snapNbr)
        priorEndYyyymm = getPriorEnd(snapNbr)
        currentBeginYyyymm = getCurrentBegin(snapNbr)
        currentEndYyyymm = getCurrentEnd(snapNbr)
    }
    println(s"[NDO-ETL] SNAP_YEAR_MNTH_NBR is : $snapNbr")
    println(s"[NDO-ETL] prior_begin_yyyymm is : $priorBeginYyyymm")
    println(s"[NDO-ETL] prior_end_yyyymm is : $priorEndYyyymm")
    println(s"[NDO-ETL] current_begin_yyyymm is : $currentBeginYyyymm")
    println(s"[NDO-ETL] current_end_yyyymm is : $currentEndYyyymm")

    //PARAMETERS_TXT
    val priorPeriod = config.getString("priorPeriod")
    val currentPeriod = config.getString("currentPeriod")
    val span = config.getString("span") //# of Months in Study
    val hclvl = config.getString("hclvl") //HIGH COST MEMBER LEVEL

    println(s"[NDO-ETL] priorPeriod from config prop file is : $priorPeriod")
    println(s"[NDO-ETL] currentPeriod from config prop file is : $currentPeriod")
    println(s"[NDO-ETL] span from config prop file is : $span")
    println(s"[NDO-ETL] hclvl from config prop file is : $hclvl")

    //Table names
    val tabMlrSvcArea = config.getString("tab_mlr_svc_area")
    val tabMlrBenPlan = config.getString("tab_mlr_ben_plan")
    val tabMlrHcc = config.getString("tab_mlr_hcc")
    val tabmlrMbr = config.getString("tab_mlr_mbr")
    val tabMlrMbrRank = config.getString("tab_mlr_mbr_rank")
    val tabMlrAllprov = config.getString("tab_mlr_allprov")
    val tabMlrPrvName = config.getString("tab_mlr_prv_name")
    val tabMlrFinPrvName = config.getString("tab_mlr_fin_prv_name")
    val tabMlrRev = config.getString("tab_mlr_rev")
    val tabMlrExp = config.getString("tab_mlr_exp")
    val tabApiMlrDtl = config.getString("tab_api_mlr_dtl")
    val tabMlrMbrEnd = config.getString("tab_mlr_mbr_end")
    val tabApiMlrSmry = config.getString("tab_api_mlr_smry")
    val tabMlrDataSmry = config.getString("tab_mlr_data_smry")
    val tabApiMlrDtlBkp = config.getString("tab_api_mlr_dtl_bkp")
    val tabApiMlrSmryBkp = config.getString("tab_api_mlr_smry_bkp")

    val maxrun_id_dtl = getMAxRunId(tabApiMlrDtl)
    println(s"[NDO-ETL] Max run id is : $maxrun_id_dtl")

    val maxrun_id_smry = getMAxRunId(tabApiMlrSmry)
    println(s"[NDO-ETL] Max run id is : $maxrun_id_smry")

    //Filters
    val ddimSrvcareaFil = ddimSrvcareaDf.filter(ddimSrvcareaDf("SNAP_YEAR_MNTH_NBR") === snapNbr &&
      ddimSrvcareaDf("MDCD_RGN_CD").isin("-2", "SAM0005") === false && !ddimSrvcareaDf("MDCD_HLTH_PLAN_SHRT_DESC").like("%Not Mapped%")) //excludes not mapped, Unknown state

    val ddimBnftPlanFil = ddimBnftPlanDf.filter(ddimBnftPlanDf("SNAP_YEAR_MNTH_NBR") === snapNbr &&
      ddimBnftPlanDf("PROD_LINE_CD").isin("BPM081", "BPM090")) //BPM081 = Medicare Advantage, BPM090 = Medicaid

    val ddimHccFil = ddimHccDf.filter(ddimHccDf("SNAP_YEAR_MNTH_NBR") === snapNbr && ddimHccDf("HCC_CD").isin("HCC00002", "HCC00003", "HCC00004") === false && ddimHccDf("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").isin("HCC Not Mapped") === false)

    val factRsttdMbrshpFil = factRsttdMbrshpDf.filter(factRsttdMbrshpDf("SNAP_YEAR_MNTH_NBR") === snapNbr &&
      (factRsttdMbrshpDf("ANLYTC_INCRD_YEAR_MNTH_NBR").between(priorBeginYyyymm, priorEndYyyymm) || factRsttdMbrshpDf("ANLYTC_INCRD_YEAR_MNTH_NBR").
        between(currentBeginYyyymm, currentEndYyyymm)))

    val factRsttdMbrshpFil2 = factRsttdMbrshpDf.filter(factRsttdMbrshpDf("SNAP_YEAR_MNTH_NBR") === snapNbr &&
      (factRsttdMbrshpDf("ANLYTC_INCRD_YEAR_MNTH_NBR") === priorEndYyyymm || factRsttdMbrshpDf("ANLYTC_INCRD_YEAR_MNTH_NBR") === currentEndYyyymm))

    val factRsttdRvnuFil = factRsttdRvnuDf.filter(factRsttdRvnuDf("SNAP_YEAR_MNTH_NBR") === snapNbr &&
      (factRsttdRvnuDf("ANLYTC_INCRD_YEAR_MNTH_NBR").between(priorBeginYyyymm, priorEndYyyymm) || factRsttdRvnuDf("ANLYTC_INCRD_YEAR_MNTH_NBR").
        between(currentBeginYyyymm, currentEndYyyymm)))

    val factRsttdExpnsFil = factRsttdExpnsDf.filter(factRsttdExpnsDf("SNAP_YEAR_MNTH_NBR") === snapNbr &&
      (factRsttdExpnsDf("ANLYTC_INCRD_YEAR_MNTH_NBR").between(priorBeginYyyymm, priorEndYyyymm) || factRsttdExpnsDf("ANLYTC_INCRD_YEAR_MNTH_NBR").
        between(currentBeginYyyymm, currentEndYyyymm)))

    val ddimProvTaxIdFil = ddimProvTaxIdDf.filter(ddimProvTaxIdDf("SNAP_YEAR_MNTH_NBR") === snapNbr)

    val f = for {

      /* service area for all service areas */
      mlrSvcAreaDf <- mlrSvcArea(ddimSrvcareaFil)
      WriteMlrSvcArea <- WriteManagedTblInsertOverwrite(mlrSvcAreaDf, stagingHiveDB, tabMlrSvcArea)

      /* Create MLR plan table for all medicaid and medicare advantage */
      mlrBenPlanDf <- mlrBenPlan(ddimBnftPlanFil)
      WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(mlrBenPlanDf, stagingHiveDB, tabMlrBenPlan)

      /* create HCC rollup categories table */
      mlrHccDf <- mlrHcc(ddimHccFil)
      WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(mlrHccDf, stagingHiveDB, tabMlrHcc)

      mlrMbrDf <- mlrMbr(factRsttdMbrshpFil, tabMlrSvcArea, tabMlrBenPlan, tabMlrHcc, priorBeginYyyymm, priorEndYyyymm, currentBeginYyyymm, currentEndYyyymm, priorPeriod, currentPeriod, stagingHiveDB)
      WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(mlrMbrDf, stagingHiveDB, tabmlrMbr)

      mlrMbrRankDf <- mlrMbrRank(tabmlrMbr, currentPeriod, stagingHiveDB)
      WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(mlrMbrRankDf, stagingHiveDB, tabMlrMbrRank)

      mlrAllprovDf <- mlrAllprov(tabmlrMbr, mlrMbrRankDf, stagingHiveDB)
      WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(mlrAllprovDf, stagingHiveDB, tabMlrAllprov)

      mlrPrvNameDf <- mlrPrvName(tabMlrAllprov, ddimProvTaxIdFil, stagingHiveDB)
      WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(mlrPrvNameDf, stagingHiveDB, tabMlrPrvName)

      /* note:  looks for most recent 1099 tax name */
      mlrFinPrvNameDf <- mlrFinPrvName(tabMlrPrvName, tabMlrMbrRank, stagingHiveDB)
      WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(mlrFinPrvNameDf, stagingHiveDB, tabMlrFinPrvName)

      mlrRevDf <- mlrRev(factRsttdRvnuFil, tabMlrSvcArea, tabMlrBenPlan, tabMlrHcc, priorBeginYyyymm, priorEndYyyymm, currentBeginYyyymm, currentEndYyyymm, priorPeriod, currentPeriod, stagingHiveDB)
      WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(mlrRevDf, stagingHiveDB, tabMlrRev)

      mlrAllprov2Df <- mlrAllprov2(tabMlrRev, tabMlrMbrRank, stagingHiveDB)
      WriteMlrBenPlan <- WriteExternalTblAppend(mlrAllprov2Df, stagingHiveDB, tabMlrAllprov)

      mlrExpDf <- mlrExp(factRsttdExpnsFil, tabMlrSvcArea, tabMlrBenPlan, tabMlrHcc, priorBeginYyyymm, priorEndYyyymm, currentBeginYyyymm, currentEndYyyymm, priorPeriod, currentPeriod, stagingHiveDB)
      WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(mlrExpDf, stagingHiveDB, tabMlrExp)  

      mlrAllprov3Df <- mlrAllprov3(tabMlrExp, tabMlrMbrRank, stagingHiveDB)
      WriteMlrBenPlan <- WriteExternalTblAppend(mlrAllprov3Df, stagingHiveDB, tabMlrAllprov)

      /* create API MLR detail output table */
      apiMlrDtlDf2 <- apiMlrDtl(tabMlrAllprov, tabMlrFinPrvName, snapNbr, stagingHiveDB)

      val apiMlrDtlDf2WC = apiMlrDtlDf2.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
        withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
        withColumn(lastUpdatedDate, lit(current_timestamp())).
        withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).withColumn("run_id", lit(maxrun_id_dtl))

      WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(apiMlrDtlDf2WC, warehouseHiveDB, tabApiMlrDtl)

      /* table for member months at the end of each time period used as panel count*/

      mlrMbrEndDf <- mlrMbrEnd(factRsttdMbrshpFil2, tabMlrSvcArea, tabMlrBenPlan, tabMlrHcc, priorBeginYyyymm, priorEndYyyymm, currentBeginYyyymm, currentEndYyyymm, priorPeriod, currentPeriod, stagingHiveDB)
      WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(mlrMbrEndDf, stagingHiveDB, tabMlrMbrEnd)

      apiMlrDtl2Df <- apiMlrDtl2(tabMlrMbrEnd, tabMlrMbrRank, tabMlrFinPrvName, snapNbr, stagingHiveDB)

      val apiMlrDtl2WC = apiMlrDtl2Df.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
        withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
        withColumn(lastUpdatedDate, lit(current_timestamp())).
        withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).withColumn("run_id", lit(maxrun_id_dtl))

      WriteMlrBenPlanDf <- WriteExternalTblAppend(apiMlrDtl2WC, warehouseHiveDB, tabApiMlrDtl)
      WriteApiMlrDtlvDf <- WriteExternalTblAppend(apiMlrDtl2WC, warehouseHiveDB, tabApiMlrDtlBkp)

      /* calculations for summary table*/
      mlrDataSmryDf <- mlrDataSmry(tabApiMlrDtl, warehouseHiveDB )
      WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(mlrDataSmryDf, stagingHiveDB, tabMlrDataSmry)

      /* create output MLR summary table, to be used by the NDO API*/
      apiMlrSmryDf <- apiMlrSmry(tabMlrDataSmry, stagingHiveDB)

      val apiMlrSmryWC = apiMlrSmryDf.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
        withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
        withColumn(lastUpdatedDate, lit(current_timestamp())).
        withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).withColumn("run_id", lit(maxrun_id_smry))

      WriteMlrBenPlan <- WriteManagedTblInsertOverwrite(apiMlrSmryWC, warehouseHiveDB, tabApiMlrSmry)
      WriteapiMlrSmryWC <- WriteExternalTblAppend(apiMlrSmryWC, warehouseHiveDB, tabApiMlrSmryBkp)

    } yield {
      println("done")
    }
    f.run(spark)
    runIDInsert(tabApiMlrDtl, maxrun_id_dtl)
    runIDInsert(tabApiMlrSmry, maxrun_id_smry)

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
  def getSnapNbr(inMapDF: Map[String, DataFrame]): Long = {
    val startTime = DateTime.now()
    //Writing the data to a table in Hive
    println(s"[HPIP-ETL] SNAP_YEAR_MNTH_NBR derivation started at : $startTime")

    val factRsttdMbrshpDf = inMapDF.getOrElse("fact_rsttd_mbrshp", null).select($"SNAP_YEAR_MNTH_NBR").distinct
    val factRsttdRvnuDf = inMapDF.getOrElse("fact_rsttd_rvnu", null).select($"SNAP_YEAR_MNTH_NBR").distinct
    val factRsttdExpnsDf = inMapDF.getOrElse("fact_rsttd_expns", null).select($"SNAP_YEAR_MNTH_NBR").distinct
    val ddimSrvcareaDf = inMapDF.getOrElse("ddim_srvcarea", null).select($"SNAP_YEAR_MNTH_NBR").distinct
    val ddimBnftPlanDf = inMapDF.getOrElse("ddim_bnft_plan", null).select($"SNAP_YEAR_MNTH_NBR").distinct
    val ddimProvTaxIdDf = inMapDF.getOrElse("ddim_prov_tax_id", null).select($"SNAP_YEAR_MNTH_NBR").distinct
    val ddimHccDf = inMapDF.getOrElse("ddim_hcc", null).select($"SNAP_YEAR_MNTH_NBR").distinct

    val snapNbr = factRsttdMbrshpDf.join(factRsttdRvnuDf, Seq("SNAP_YEAR_MNTH_NBR"))
      .join(factRsttdExpnsDf, Seq("SNAP_YEAR_MNTH_NBR"))
      .join(ddimSrvcareaDf, Seq("SNAP_YEAR_MNTH_NBR"))
      .join(ddimBnftPlanDf, Seq("SNAP_YEAR_MNTH_NBR"))
      .join(ddimProvTaxIdDf, Seq("SNAP_YEAR_MNTH_NBR"))
      .join(ddimHccDf, Seq("SNAP_YEAR_MNTH_NBR")).orderBy($"SNAP_YEAR_MNTH_NBR".desc).head.getLong(0)

    println(s"[HPIP-ETL] latest SNAP_YEAR_MNTH_NBR available in all the tables is $snapNbr")
    println(s"[HPIP-ETL]  SNAP_YEAR_MNTH_NBR derivation Completed at: " + DateTime.now())
    println(s"[HPIP-ETL] Time Taken for derivation :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")
    snapNbr

  }
  def getMAxRunId(tablename: String): Long = {
    val startTime = DateTime.now()
    //Writing the data to a table in Hive
    println(s"[NDO-ETL] Derivation of MAX_RUN_ID started at: $startTime")

    //***  Query   for Max run_id column value*****//
    val checkMaxRunId = s"SELECT run_id from (SELECT  run_id,rank() over (order by run_id desc) as r from  $warehouseHiveDB.run_id_table where sub_area='$tablename' ) S where S.r=1"
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

  def getPriorBegin(snapNbr: Long): String =
    {

      val tobeDerived = snapNbr.toString
      val formatter = new SimpleDateFormat("yyyyMM", Locale.ENGLISH);
      val snapNbrFormated = formatter.parse(tobeDerived)

      val calender = Calendar.getInstance();
      calender.setTime(snapNbrFormated);
      calender.add(Calendar.YEAR, -2)
      calender.add(Calendar.MONTH, -1)
      val priorBeginNotFormatted = calender.getTime();
      val priorBegin = formatter.format(priorBeginNotFormatted)

      println(s"[NDO-ETL] After formatting priorBegin : $priorBegin")

      priorBegin
    }

  def getPriorEnd(snapNbr: Long): String =
    {

      val tobeDerived = snapNbr.toString
      val formatter = new SimpleDateFormat("yyyyMM", Locale.ENGLISH);
      val snapNbrFormated = formatter.parse(tobeDerived)

      val calender = Calendar.getInstance();
      calender.setTime(snapNbrFormated);
      calender.add(Calendar.YEAR, -1)
      calender.add(Calendar.MONTH, -2)
      val priorEndNotFormatted = calender.getTime();
      val priorEnd = formatter.format(priorEndNotFormatted)

      println(s"[NDO-ETL] After formatting priorEnd $priorEnd")

      priorEnd
    }

  def getCurrentBegin(snapNbr: Long): String =
    {

      val tobeDerived = snapNbr.toString
      val formatter = new SimpleDateFormat("yyyyMM", Locale.ENGLISH);
      val snapNbrFormated = formatter.parse(tobeDerived)

      val calender = Calendar.getInstance();
      calender.setTime(snapNbrFormated);
      calender.add(Calendar.YEAR, -1)
      calender.add(Calendar.MONTH, -1)
      val currentBeginNotFormatted = calender.getTime();
      val currentBegin = formatter.format(currentBeginNotFormatted)

      println(s"[NDO-ETL] After formatting currentBegin $currentBegin")

      currentBegin
    }

  def getCurrentEnd(snapNbr: Long): String =
    {

      val tobeDerived = snapNbr.toString
      val formatter = new SimpleDateFormat("yyyyMM", Locale.ENGLISH);
      val snapNbrFormated = formatter.parse(tobeDerived)

      val calender = Calendar.getInstance();
      calender.setTime(snapNbrFormated);
      calender.add(Calendar.MONTH, -2)
      val currentEndNotFormatted = calender.getTime();
      val currentEnd = formatter.format(currentEndNotFormatted)

      println(s"[NDO-ETL] After formatting currentEnd $currentEnd")

      currentEnd
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
