package com.am.ndo.ndoRatgWrk

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window
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

class NdoRatgWrkOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

  import spark.implicits._

  //Audit
  var program = ""
  var user_id = ""
  var app_id = ""
  var start_time: DateTime = DateTime.now()
  var start = ""

  var listBuffer = ListBuffer[Audit]()
  val tabndoRatgArea = config.getString("tab_ndo_ratg_area")
  val tabndoRatgAddr1 = config.getString("tab_ndo_ratg_addr1")
  val tabndoRatgBar1 = config.getString("tab_ndo_ratg_bar1")
  val tabndowrkRatgAddr = config.getString("tab_ndowrk_ratg_addr")
  val tabndowrkRatgAddrBkp = config.getString("tab_ndowrk_ratg_addr_bkp")
   
  def loadData(): Map[String, DataFrame] = {

    //Reading the data into Data frames
    val startTime = DateTime.now
    println(s"[NDO-ETL] The loading of Data started with start time at :  + $startTime")

    //Reading the queries from config file
    println("Reading the queries from config file")
    
    val bkbnIpBvQuery=config.getString("query_bkbn_ip_bv").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase() 
    val bkbnAdrsRltnshpQuery=config.getString("query_bkbn_adrs_rltnshp").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    
    val bkbnIpBvDf = spark.sql(bkbnIpBvQuery)
    val bkbnAdrsRltnshpDf = spark.sql(bkbnAdrsRltnshpQuery)
    
    val snapNbr : String= getSnapNbr(bkbnIpBvDf,bkbnAdrsRltnshpDf)
    
    val ndoRatgAreaQuery = config.getString("query_ndo_ratg_area").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB)
    val ndoRatgAddr1Query = config.getString("query_ndo_ratg_addr1").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).replaceAll(ConfigKey.stagingDBPlaceholder, stagingHiveDB)
                                  .replaceAll(ConfigKey.warehouseDBPlaceHolder, warehouseHiveDB).replaceAll(ConfigKey.snapNbrPlaceholder, snapNbr)
    val ndoRatgBar1Query = config.getString("query_ndo_ratg_bar1").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).replaceAll(ConfigKey.stagingDBPlaceholder, stagingHiveDB).replaceAll(ConfigKey.snapNbrPlaceholder, snapNbr)
    val ndowrkRatgAddrQuery = config.getString("query_ndowrk_ratg_addr").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).replaceAll(ConfigKey.stagingDBPlaceholder, stagingHiveDB)
    
    println(s"[NDO-ETL] Query for reading data from ndo_ratg_area table is $ndoRatgAreaQuery")
    println(s"[NDO-ETL] Query for reading data from ndo_ratg_addr1 table is $ndoRatgAddr1Query")
    println(s"[NDO-ETL] Query for reading data from ndo_ratg_bar1 table is $ndoRatgBar1Query")
    println(s"[NDO-ETL] Query for reading data from ndowrk_ratg_addr table is $ndowrkRatgAddrQuery")
  
    val ndoRatgAreaDf = spark.sql(ndoRatgAreaQuery)
    val ndoRatgAddr1Df = spark.sql(ndoRatgAddr1Query)
    val ndoRatgBar1Df = spark.sql(ndoRatgBar1Query)
    val ndowrkRatgAddrDf = spark.sql(ndowrkRatgAddrQuery)

 
    val mapDF = Map("ndo_ratg_area" -> ndoRatgAreaDf, "ndo_ratg_addr1" -> ndoRatgAddr1Df
        ,"ndo_ratg_bar1" -> ndoRatgBar1Df, "ndowrk_ratg_addr" -> ndowrkRatgAddrDf
        )

    println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
    println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    mapDF
    

  }

  def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
    val startTime = DateTime.now
    println(s"[NDO-ETL] Processing Data Started: $startTime")
    //Reading the data frames as elements from Map
    val ndoRatgAreaDf = inMapDF.getOrElse("ndo_ratg_area", null)
    val ndoRatgAddr1Df = inMapDF.getOrElse("ndo_ratg_addr1", null)
    val ndoRatgBar1Df = inMapDF.getOrElse("ndo_ratg_bar1", null)
    val ndowrkRatgAddrDf = inMapDF.getOrElse("ndowrk_ratg_addr", null)
    
    val lastUpdatedDate = config.getString(ConfigKey.auditColumnName)
    
    val maxrun_id_ratg_addr = getMAxRunId(tabndowrkRatgAddr)
    println(s"[NDO-ETL] Max run id is : $maxrun_id_ratg_addr")
    
     writeTblInsertOverwrite(ndoRatgAreaDf, stagingHiveDB, tabndoRatgArea)
     writeTblInsertOverwrite(ndoRatgAddr1Df, stagingHiveDB, tabndoRatgAddr1)
     writeTblInsertOverwrite(ndoRatgBar1Df, stagingHiveDB, tabndoRatgBar1)
     
     val ndowrkRatgAddrWC = ndowrkRatgAddrDf.withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
        withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
        withColumn(lastUpdatedDate, lit(current_timestamp())).
        withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser))
        .withColumn("run_id", lit(maxrun_id_ratg_addr))
        
        
     writeTblInsertOverwrite(ndowrkRatgAddrWC, warehouseHiveDB, tabndowrkRatgAddr)
     writeTblAppendOverwrite(ndowrkRatgAddrWC, warehouseHiveDB, tabndowrkRatgAddrBkp)
      
		 runIDInsert(tabndowrkRatgAddr, maxrun_id_ratg_addr)
    

    println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
    println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    null
  }

  def writeData(map: Map[String, DataFrame]): Unit = {
    
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
	
	def writeTblInsertOverwrite(df: DataFrame, hiveDB: String, tableName: String){
	   println(s"[NDO-ETL] Write started for table $tableName at:" + DateTime.now())
    df.write.mode("overwrite").insertInto(hiveDB + """.""" + tableName)
    println(s"[NDO-ETL] Table created as $hiveDB.$tableName")
	}
	
	def writeTblAppendOverwrite(df: DataFrame, hiveDB: String, tableName: String){
	   println(s"[NDO-ETL] Write started for table $tableName at:" + DateTime.now())
    df.write.mode("append").insertInto(hiveDB + """.""" + tableName)
    println(s"[NDO-ETL] Table created as $hiveDB.$tableName")
	}
	
	def getSnapNbr(bkbnIpBvDf: DataFrame, bkbnAdrsRltnshpDf: DataFrame):String = {
    val startTime = DateTime.now()
    //Writing the data to a table in Hive
    println(s"[HPIP-ETL] SNAP_YEAR_MNTH_NBR derivation started at : $startTime")

    val snapNbr = bkbnIpBvDf.join(bkbnAdrsRltnshpDf,Seq("SNAP_YEAR_MNTH_NBR")).orderBy($"SNAP_YEAR_MNTH_NBR".desc).head.getLong(0)

    println(s"[HPIP-ETL] latest SNAP_YEAR_MNTH_NBR available in all the tables is $snapNbr")
    println(s"[HPIP-ETL]  SNAP_YEAR_MNTH_NBR derivation Completed at: " + DateTime.now())
    println(s"[HPIP-ETL] Time Taken for derivation :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")
    snapNbr.toString

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
