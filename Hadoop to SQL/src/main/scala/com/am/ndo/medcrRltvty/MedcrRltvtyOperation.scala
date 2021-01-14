package com.am.ndo.medcrRltvty

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

class MedcrRltvtyOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

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

    val outpatientsummaryQuery = config.getString("query_outpatient_summary_medcr").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val inpatientsummaryQuery = config.getString("query_inpatient_summary_medcr").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val facilityattributeprofileQuery = config.getString("query_facility_attribute_profile_medcr").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    val ndtRatingAreaAcaZip3Query = config.getString("query_ndt_rating_area_aca_zip3").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()

    			println(s"[NDO-ETL] Query for reading data from outpatient_summary table is $outpatientsummaryQuery")
					println(s"[NDO-ETL] Query for reading data from inpatient_summary_medcr table is $inpatientsummaryQuery")
					println(s"[NDO-ETL] Query for reading data from facility_attribute_profile table is $facilityattributeprofileQuery")
					println(s"[NDO-ETL] Query for reading data from worktbl_ndt_rating_area_aca_zip3 table is $ndtRatingAreaAcaZip3Query")
    
    val outpatientSummaryDf = spark.sql(outpatientsummaryQuery)
    val inpatientSummaryDf = spark.sql(inpatientsummaryQuery)
    val facilityAttributeProfileDf = spark.sql(facilityattributeprofileQuery)
    val ndtRatingAreaAcaZip3Df = spark.sql(ndtRatingAreaAcaZip3Query)

    val mapDF = Map(

      "inpatient_summary" -> inpatientSummaryDf,
      "facility_attribute_profile" -> facilityAttributeProfileDf,
      "outpatient_summary" -> outpatientSummaryDf,
      "ndt_rating_area_aca_zip3" -> ndtRatingAreaAcaZip3Df)

    println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
    println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    mapDF
  }

  def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
    val startTime = DateTime.now
    println(s"[NDO-ETL] Processing Data Started: $startTime")
    //Reading the data frames as elements from Map
    val outpatientSummaryDf = inMapDF.getOrElse("outpatient_summary", null)
    val inpatientSummaryDf = inMapDF.getOrElse("inpatient_summary", null)
    val facilityAttributeProfileDf1 = inMapDF.getOrElse("facility_attribute_profile", null)
    val ndtRatingAreaAcaZip3Df = inMapDF.getOrElse("ndt_rating_area_aca_zip3", null)

  //  val tabndoMedcrRltvtyWrk1 = config.getString("tab_ndo_medcr_rltvty_wrk1")
  //  val tabndoMedcrRltvtyNtwk = config.getString("tab_ndo_medcr_rltvty_ntwk")
    val tabndoMedcrRltvty = config.getString("tab_medcr_rltvty")
    val tabndoMedcrRltvtyBkp = config.getString("tab_medcr_rltvty_bkp")

   //need to remove in next release
   val withOutSpclChar = facilityAttributeProfileDf1.filter(!$"hospital".contains("NYU Langone Hospital"))
   val withSpclChar = facilityAttributeProfileDf1.filter($"hospital".contains("NYU Langone Hospital"))
   val spclCharRpls = withSpclChar.withColumn("hospital", lit("NYU Langone Hospital—Brooklyn"))
   val facilityAttributeProfileDf = withOutSpclChar.union(spclCharRpls)

    val maxrun_id = getMAxRunId(tabndoMedcrRltvty)
    
    val map = getDateRange()
    val rangeStartDate = map.getOrElse("rangeStartDate", null)
    val rangeEndDate = map.getOrElse("rangeEndDate", null)
    println(s"[NDO-ETL] Max run id is : $maxrun_id")
    println(s"[NDO-ETL] rangeStartDate: $rangeStartDate")
    println(s"[NDO-ETL] rangeEndDate: $rangeEndDate")
    
    val inpatientSummaryFilDf = inpatientSummaryDf.filter($"mbr_state".isin("CA", "CO", "CT", "GA", "IN", "KY", "ME", "MO", "NH", "NV", "NY", "OH", "VA", "WI")
      && $"MBUlvl2".isNotNull && $"brand" === concat(lit("Blue "), $"MBR_State") && ($"MCS").notEqual("Y") && $"liccd" === "01"
      && $"MBU_CF_CD".isin("SRLGCT", "SRGALT", "SREGHS", "SRKYNA", "SRWXNA", "SRMENA", "SRLGNA", "SRCANA", "SRCONA", "SRLGKS", "SRLGNH", "SRMOLT", "SRINLG",
        "MCINGP", "SRLGBC", "SRGANA", "SRMZNA", "SRMELG", "SRKYNR", "SRLGNY", "SRLGCA", "SREGOH", "SRWILT", "SRLGW2", "SRLGCO", "SRVZNA",
        "SRLGNV", "MCOHGP", "SRNHLT", "MCCALA", "SRNYDL", "SRCOLT", "SRLGVA", "SRLGGA", "SRWINA", "SRINLT", "SRNYCC", "SRINNA", "SRMELT",
        "SRMYNA", "SRCALT", "SRLGWI", "SRKYLT", "SREGNA", "SREGNH", "SREGIN", "SRMONA", "SRLGME", "SREGCA", "SREGMO", "SROZNA", "SROYNA",
        "SRLGUN", "SRNAHS", "SRGALG", "SRNYLG", "SRWZNA", "SRCTLG", "SRKYRG", "SRCTLT", "SRLGMO", "SRWILG", "SRNACA", "SRLGOH", "SRNYNA",
        "SROHRG", "SRVANA", "SREGVA", "SRGAST", "SRVALT", "SRMOLG", "MCKYGP", "SREGKY", "SROHNA", "SROHLT", "SRINRG", "SRNYUL", "SRCTNA",
        "SREGGA", "SRNVLT") === false && $"CLM_LINE_ENCNTR_CD".isin("N", "NA", "UNK") && $"Inc_Month".between(rangeStartDate, rangeEndDate)
        )
        

    
      
   val outpatientSummaryFilDf = outpatientSummaryDf.filter($"mbr_state".isin("CA", "CO", "CT", "GA", "IN", "KY", "ME", "MO", "NH", "NV", "NY", "OH", "VA", "WI")
      && $"MBUlvl2".isNotNull && $"brand" === concat(lit("Blue "), $"MBR_State") && ($"MCS").notEqual("Y") && $"liccd" === "01"
      && $"MBU_CF_CD".isin("SRLGCT", "SRGALT", "SREGHS", "SRKYNA", "SRWXNA", "SRMENA", "SRLGNA", "SRCANA", "SRCONA", "SRLGKS", "SRLGNH", "SRMOLT", "SRINLG",
        "MCINGP", "SRLGBC", "SRGANA", "SRMZNA", "SRMELG", "SRKYNR", "SRLGNY", "SRLGCA", "SREGOH", "SRWILT", "SRLGW2", "SRLGCO", "SRVZNA",
        "SRLGNV", "MCOHGP", "SRNHLT", "MCCALA", "SRNYDL", "SRCOLT", "SRLGVA", "SRLGGA", "SRWINA", "SRINLT", "SRNYCC", "SRINNA", "SRMELT",
        "SRMYNA", "SRCALT", "SRLGWI", "SRKYLT", "SREGNA", "SREGNH", "SREGIN", "SRMONA", "SRLGME", "SREGCA", "SREGMO", "SROZNA", "SROYNA",
        "SRLGUN", "SRNAHS", "SRGALG", "SRNYLG", "SRWZNA", "SRCTLG", "SRKYRG", "SRCTLT", "SRLGMO", "SRWILG", "SRNACA", "SRLGOH",
        "SRNYNA", "SROHRG", "SRVANA", "SREGVA", "SRGAST", "SRVALT", "SRMOLG", "MCKYGP", "SREGKY", "SROHNA", "SROHLT", "SRINRG",
        "SRNYUL", "SRCTNA", "SREGGA", "SRNVLT") === false && $"CLM_LINE_ENCNTR_CD".isin("N", "NA", "UNK") && $"Inc_Month".between(rangeStartDate, rangeEndDate))
    
    
    val f = for {

      
    
      ndoMedcrRltvtyWrk1Df <- ndoMedcrRltvtyWrk1(facilityAttributeProfileDf, inpatientSummaryFilDf, outpatientSummaryFilDf, ndtRatingAreaAcaZip3Df, stagingHiveDB, rangeStartDate, rangeEndDate)
    //  stag1 <- WriteManagedTblInsertOverwrite(ndoMedcrRltvtyWrk1Df,stagingHiveDB,"ndo_medcr_rltvty_wrk1")
 
      ndoMedcrRltvtyNtwk <- ndoMedcrRltvtyNtwk(ndoMedcrRltvtyWrk1Df)
    //  stag2 <- WriteManagedTblInsertOverwrite(ndoMedcrRltvtyNtwk,stagingHiveDB,"ndo_medcr_rltvty_ntwk")
    
      ndoMedcrRltvtyDf <- ndoMedcrRltvty(ndoMedcrRltvtyWrk1Df,ndoMedcrRltvtyNtwk)
      val ndoMedcrRltvty = ndoMedcrRltvtyDf.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
        withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
        withColumn(lastUpdatedDate, lit(current_timestamp())).
       withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).withColumn("run_id", lit(maxrun_id))
     WritendoMedcrRltvty <- WriteManagedTblInsertOverwrite(ndoMedcrRltvty, warehouseHiveDB, tabndoMedcrRltvty)
    WritendoMedcrRltvty <- WriteExternalTblAppend(ndoMedcrRltvty, warehouseHiveDB, tabndoMedcrRltvtyBkp)

    } yield {
      println("done")
    }
    

        
    f.run(spark)
    

    
    runIDInsert(tabndoMedcrRltvty, maxrun_id)

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
 
  def getDateRange(): Map[String, String] =
		{
				val calender = Calendar.getInstance()
				val formatter = new SimpleDateFormat("yyyyMM", Locale.ENGLISH)
				val DateUF = calender.getTime()
				val CurrentYearMonth = formatter.format(DateUF)
				println("Current Year Month is : " + CurrentYearMonth)
				calender.add(Calendar.MONTH, -1)
				val previousDateUnFormatted = calender.getTime()
				val previousYearMonth = formatter.format(previousDateUnFormatted)
				println("One Month Prior to current Year Month is" + previousYearMonth)
				
				val map = Map(
						"rangeStartDate" -> "201801",
						"rangeEndDate" -> previousYearMonth)
				
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
