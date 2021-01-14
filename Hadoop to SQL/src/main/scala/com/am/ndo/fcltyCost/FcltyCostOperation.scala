package com.am.ndo.fcltyCost

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

class FcltyCostOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

  import spark.implicits._

  //Audit
  var program = ""
  var user_id = ""
  var app_id = ""
  var start_time: DateTime = DateTime.now()
  var start = ""

  var listBuffer = ListBuffer[Audit]()
  val tabRenderingFacility = config.getString("tab_ndo_fclty_cost")
  val tabRenderingFacilityBkp = config.getString("tab_ndo_fclty_cost_bkp")
  def loadData(): Map[String, DataFrame] = {

    //Reading the data into Data frames
    val startTime = DateTime.now
    println(s"[NDO-ETL] The loading of Data started with start time at :  + $startTime")

    //Reading the queries from config file
    println("Reading the queries from config file")

    
 
    val pcrEtgClmOutptQuery = config.getString("query_pcr_etg_clm_outpt").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val pcrCostEpsdQuery = config.getString("query_pcr_cost_epsd").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val pcrEtgMbrSmryQuery = config.getString("query_pcr_etg_mbr_smry").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val fnclMbuCfQuery = config.getString("query_fncl_mbu_cf").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val mlsaZipCdQuery = config.getString("query_mlsa_zip_cd").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val medcrProvAnlytcPatchQuery = config.getString("query_medcr_prov_anlytc_patch").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val ndoZipSubmarketXwalkQuery = config.getString("query_ndo_zip_submarket_xwalk_Ehoppa").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()

    println(s"[NDO-ETL] Query for reading data from pcr_etg_clm_outpt table is $pcrEtgClmOutptQuery")
    println(s"[NDO-ETL] Query for reading data from pcr_cost_epsd table is $pcrCostEpsdQuery")
    println(s"[NDO-ETL] Query for reading data from pcr_etg_mbr_smry table is $pcrEtgMbrSmryQuery")
    println(s"[NDO-ETL] Query for reading data from fncl_mbu_cf table is $fnclMbuCfQuery")
    println(s"[NDO-ETL] Query for reading data from mlsa_zip_cd table is $mlsaZipCdQuery")
    println(s"[NDO-ETL] Query for reading data from ndo_zip_submarket_xwalk table is $ndoZipSubmarketXwalkQuery")
    println(s"[NDO-ETL] Query for reading data from medcr_prov_anlytc_patch table is $medcrProvAnlytcPatchQuery")

    val pcrEtgClmOutptDf = spark.sql(pcrEtgClmOutptQuery)
    val pcrCostEpsdDf = spark.sql(pcrCostEpsdQuery)
    val pcrEtgMbrSmryDf = spark.sql(pcrEtgMbrSmryQuery)
    val fnclMbuCfDf = spark.sql(fnclMbuCfQuery)
    val mlsaZipCdDf = spark.sql(mlsaZipCdQuery)
    val ndoZipSubmarketXwalkDf =  spark.sql(ndoZipSubmarketXwalkQuery)
    val medcrProvAnlytcPatchDf = spark.sql(medcrProvAnlytcPatchQuery)

    val mapDF = Map("pcr_etg_clm_outpt" -> pcrEtgClmOutptDf, 
                    "pcr_cost_epsd" -> pcrCostEpsdDf,
                    "pcr_etg_mbr_smry" -> pcrEtgMbrSmryDf,
                    "fncl_mbu_cf" -> fnclMbuCfDf,
                    "medcr_prov_anlytc_patch" -> medcrProvAnlytcPatchDf,
                    "mlsa_zip_cd" -> mlsaZipCdDf,
                    "ndo_zip_submarket_xwalk" -> ndoZipSubmarketXwalkDf)

    println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
    println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    mapDF

  }

  def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
    val startTime = DateTime.now
    println(s"[NDO-ETL] Processing Data Started: $startTime")
    //Reading the data frames as elements from Map

    val pcrEtgClmOutptDf: Dataset[Row] = inMapDF.getOrElse("pcr_etg_clm_outpt", null)
    val pcrCostEpsdDf: Dataset[Row] = inMapDF.getOrElse("pcr_cost_epsd", null)
    val pcrEtgMbrSmryDf: Dataset[Row] = inMapDF.getOrElse("pcr_etg_mbr_smry", null)
    val fnclMbuCfDf: Dataset[Row] = inMapDF.getOrElse("fncl_mbu_cf", null)
    val mlsaZipCdDf: Dataset[Row] = inMapDF.getOrElse("mlsa_zip_cd", null)
    val ndoZipSubmarketXwalkDf: Dataset[Row] = inMapDF.getOrElse("ndo_zip_submarket_xwalk", null)
    val medcrProvAnlytcPatchDf: Dataset[Row] = inMapDF.getOrElse("medcr_prov_anlytc_patch", null)

    val lastUpdatedDate = config.getString(ConfigKey.auditColumnName)
    val maxrun_id_renderingFacility = getMAxRunId(tabRenderingFacility)
    
    println(s"[NDO-ETL] Max run id is : $maxrun_id_renderingFacility")

    val maxEtgRunId = getmaxEtgRunId(inMapDF)
    println(s"[NDO-ETL] Max ETG run id is : $maxEtgRunId")
    val pcrCostEpsdDfFilter = pcrCostEpsdDf.filter((pcrCostEpsdDf("ETG_RUN_ID") === maxEtgRunId) && trim(pcrCostEpsdDf("GRP_AGRGTN_TYPE_CD")) === ("TIN") &&
      trim(pcrCostEpsdDf("BNCHMRK_PROD_DESC")).isin("NP", "NA", "PAR") === false &&
      trim(pcrCostEpsdDf("SPCLTY_PRMRY_CD")).isin("UNK", "NA", "53", "17", "A0", "A5", "69", "32", "B4", "51", "87", "99", "43", "54", "88", "61") === false &&
      trim(pcrCostEpsdDf("PCR_LOB_DESC")).isin("NP") === false &&
      trim(pcrCostEpsdDf("NTWK_ST_CD")).isin("CA", "CO", "CT", "FL", "GA", "IA", "IN", "KS", "KY", "LA", "MD", "ME", "MO", "NH", "NJ", "NM", "NV", "NY", "OH", "SC", "TN", "TX", "VA", "WA", "WI", "WV") &&
      ((trim(pcrCostEpsdDf("NTWK_ST_CD")).isin("CA", "NY", "VA") && trim(pcrCostEpsdDf("PEER_MRKT_CD")) === ("8")) || (trim(pcrCostEpsdDf("NTWK_ST_CD")).isin("CA", "NY", "VA") === false && trim(pcrCostEpsdDf("PEER_MRKT_CD")) === ("4"))))

    val mlsaZipCdDfFil = mlsaZipCdDf.filter(mlsaZipCdDf("ACTV_ZIP_IND") === "Y").select($"ZIP_CD", $"ST_CD",substring($"CNTY_NM",3,3).alias("MS_CNTY_CD")).distinct()

    val pcrEostEtgJoin = pcrCostEpsdDfFilter.join(pcrEtgMbrSmryDf, pcrCostEpsdDfFilter("mcid") === pcrEtgMbrSmryDf("mcid")
      && pcrCostEpsdDfFilter("ETG_RUN_ID") === pcrEtgMbrSmryDf("ETG_RUN_ID"), "inner")
      .join(fnclMbuCfDf, trim(fnclMbuCfDf("MBU_CF_CD")) === trim(pcrEtgMbrSmryDf("MBU_CF_CD")), "inner")
      .join(mlsaZipCdDfFil, trim(mlsaZipCdDfFil("ZIP_CD")) === trim(pcrEtgMbrSmryDf("ZIP_CD")) 
          && trim(mlsaZipCdDfFil("st_cd")) === trim(pcrEtgMbrSmryDf("MBR_RSDNT_ST_CD")) , "left_outer")
      .join(ndoZipSubmarketXwalkDf, trim(ndoZipSubmarketXwalkDf("zip_code")) === trim(pcrEtgMbrSmryDf("ZIP_CD")) 
          && trim(ndoZipSubmarketXwalkDf("st_cd")) === trim(pcrEtgMbrSmryDf("MBR_RSDNT_ST_CD")) 
          && trim(ndoZipSubmarketXwalkDf("cnty_cd")) === trim(mlsaZipCdDfFil("MS_CNTY_CD")), "left_outer")
      .select(
        pcrCostEpsdDfFilter("ETG_RUN_ID"),
        pcrCostEpsdDfFilter("BKBN_SNAP_YEAR_MNTH_NBR"),
        pcrCostEpsdDfFilter("EPSD_NBR"),
        pcrCostEpsdDfFilter("NTWK_ST_CD"),
        pcrCostEpsdDfFilter("GRP_AGRGTN_ID"),
        upper(fnclMbuCfDf("MBU_LVL_2_DESC")).alias("BSNS_SGMNT"),
        (when(trim(pcrEtgMbrSmryDf("MBR_RSDNT_ST_CD")) !== trim(pcrCostEpsdDfFilter("NTWK_ST_CD")), lit("UNK"))
            otherwise (coalesce(ndoZipSubmarketXwalkDf("RATING_AREA_DESC"), lit("UNK"))) ).alias("MBR_RATNG_AREA")
            ).distinct

 

    val pcrEtgClmOutptFil = pcrEtgClmOutptDf.filter(pcrEtgClmOutptDf("ETG_RUN_ID") === maxEtgRunId && trim(pcrEtgClmOutptDf("RCRD_TYPE_CD")).isin("A", "F")
      && trim(pcrEtgClmOutptDf("TOS_LVL_0_CD")).isin("INPT", "OUTPT") && trim(pcrEtgClmOutptDf("SRVC_RNDRG_TYPE_CD")).isin("HOSP", "FANCL"))

    

    val FnclClmOutptJoin = pcrEtgClmOutptFil.join(pcrEostEtgJoin, pcrEtgClmOutptFil("EPSD_NBR") === pcrEostEtgJoin("EPSD_NBR") &&
      pcrEtgClmOutptFil("ETG_RUN_ID") === pcrEostEtgJoin("ETG_RUN_ID"), "inner")
      .join(medcrProvAnlytcPatchDf, trim(medcrProvAnlytcPatchDf("CLM_ADJSTMNT_KEY")) === trim(pcrEtgClmOutptFil("CLM_ADJSTMNT_KEY")),
        "left_outer").select(
          pcrEtgClmOutptFil("ETG_RUN_ID"),
          pcrEostEtgJoin("BKBN_SNAP_YEAR_MNTH_NBR"),
          (when(trim(medcrProvAnlytcPatchDf("RPTG_MEDCR_ID")).isNotNull && trim(medcrProvAnlytcPatchDf("RPTG_MEDCR_ID")).isin("UNK", "UNKNOWN", "") === false, trim(medcrProvAnlytcPatchDf("RPTG_MEDCR_ID")))
            when (trim(pcrEtgClmOutptFil("INPT_EDW_MEDCR_ID")).isin("UNK", "UNKNOWN", "") === false, trim(pcrEtgClmOutptFil("INPT_EDW_MEDCR_ID"))) otherwise (lit("UNK"))).alias("Medcr_ID"),
          pcrEostEtgJoin("GRP_AGRGTN_ID").alias("PROV_TAX_ID"),
          pcrEostEtgJoin("NTWK_ST_CD"),
          pcrEtgClmOutptFil("PCR_LOB_DESC"),
          pcrEtgClmOutptFil("PCR_PROD_DESC").alias("PROD_ID"),
          pcrEostEtgJoin("BSNS_SGMNT"),
          pcrEtgClmOutptFil("RPTG_NTWK_DESC"),
          pcrEostEtgJoin("MBR_RATNG_AREA"),
          pcrEtgClmOutptFil("TOTL_ALWD_AMT"),
          pcrEtgClmOutptFil("TOS_LVL_0_CD"),
          pcrEtgClmOutptFil("TOTL_ALWD_AMT"),
          pcrEtgClmOutptFil("FCLTY_ALWD_AMT"),
          pcrEtgClmOutptFil("ANCLRY_INPAT_ALWD_AMT"),
          pcrEtgClmOutptFil("ANCLRY_OUTPAT_ALWD_AMT"),
          pcrEtgClmOutptFil("EPSD_NBR")).groupBy(
            pcrEtgClmOutptFil("ETG_RUN_ID"),
            pcrEostEtgJoin("BKBN_SNAP_YEAR_MNTH_NBR"),
            $"Medcr_ID",
            $"PROV_TAX_ID",
            pcrEostEtgJoin("NTWK_ST_CD"),
            pcrEtgClmOutptFil("PCR_LOB_DESC"),
            $"PROD_ID",
            pcrEostEtgJoin("BSNS_SGMNT"),
            pcrEtgClmOutptFil("RPTG_NTWK_DESC"),
            pcrEostEtgJoin("MBR_RATNG_AREA")
            ).
            agg(
              sum(pcrEtgClmOutptFil("TOTL_ALWD_AMT")).alias("TOTL_ALWD_AMT"),
              sum(when(pcrEtgClmOutptFil("TOS_LVL_0_CD") === ("INPT"), pcrEtgClmOutptFil("TOTL_ALWD_AMT")) otherwise 0).alias("INPT_TOTL_ALWD_AMT"),
              sum(when(pcrEtgClmOutptFil("TOS_LVL_0_CD") === ("OUTPT"), pcrEtgClmOutptFil("TOTL_ALWD_AMT")) otherwise 0).alias("OUTPT_TOTL_ALWD_AMT"),
              sum(pcrEtgClmOutptFil("FCLTY_ALWD_AMT")).alias("FCLTY_ALWD_AMT"),
              sum(pcrEtgClmOutptFil("ANCLRY_INPAT_ALWD_AMT")).alias("ANCLRY_INPAT_ALWD_AMT"),
              sum(pcrEtgClmOutptFil("ANCLRY_OUTPAT_ALWD_AMT")).alias("ANCLRY_OUTPAT_ALWD_AMT"),
              countDistinct(pcrEtgClmOutptFil("EPSD_NBR")).alias("epsd_cnt"))
      .drop(pcrEtgClmOutptFil("TOS_LVL_0_CD"))
      .drop(pcrEtgClmOutptFil("EPSD_NBR"))
      .withColumn("RCRD_CREATN_DTM", lit(current_timestamp()))
      .withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser))
      .withColumn(lastUpdatedDate, lit(current_timestamp()))
      .withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).withColumn("run_id", lit(maxrun_id_renderingFacility))

 

    runIDInsert(tabRenderingFacility, maxrun_id_renderingFacility)

    val mapDF = Map(tabRenderingFacility -> FnclClmOutptJoin)

    println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
    println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    mapDF
  }

  def writeData(map: Map[String, DataFrame]): Unit = { 
    val startTime = DateTime.now()
    //Writing the data to a table in Hive

    val RenderingFacilityDf = map.getOrElse(tabRenderingFacility, null)

    println(s"[NDO-ETL] Write started for the table RenderingFacility at:" + DateTime.now())
    RenderingFacilityDf.write.mode("overwrite").insertInto(warehouseHiveDB + """.""" + tabRenderingFacility)
    println(s"[NDO-ETL] Write started for the table RenderingFacilityBkp at:" + DateTime.now())
    RenderingFacilityDf.write.mode("append").insertInto(warehouseHiveDB + """.""" + tabRenderingFacilityBkp)
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

  def getmaxEtgRunId(inMapDF: Map[String, DataFrame]): Long = {
    val startTime = DateTime.now()
    //Writing the data to a table in Hive
    info(s"[HPIP-ETL] Writing Dataframes to Hive started at: $startTime")

    val pcrCostEpsdDf = inMapDF.getOrElse("pcr_cost_epsd", null).select($"ETG_RUN_ID").distinct
    val pcrEtgClmOutptDf = inMapDF.getOrElse("pcr_etg_clm_outpt", null).select($"ETG_RUN_ID").distinct
    val pcrEtgMbrSmryDf = inMapDF.getOrElse("pcr_etg_mbr_smry", null).select($"ETG_RUN_ID").distinct

    val maxEtgRunId = pcrCostEpsdDf.join(pcrEtgClmOutptDf, Seq("ETG_RUN_ID")).join(pcrEtgMbrSmryDf, Seq("ETG_RUN_ID"))
      .orderBy($"ETG_RUN_ID".desc).head.getLong(0)

    info(s"[NDO-ETL] writing the data to target tables is Completed at: " + DateTime.now())
    info(s"[NDO-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")
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
