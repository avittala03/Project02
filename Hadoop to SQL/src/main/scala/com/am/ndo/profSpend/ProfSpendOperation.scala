package com.am.ndo.profSpend

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
import org.apache.spark.sql.types._
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
import org.apache.spark.sql.catalyst.expressions.Substring

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.{ when, col, sum, expr, substring }
import org.apache.spark.sql.types.IntegerType
import java.util.Properties
import org.apache.spark.sql.functions.from_unixtime
class ProfSpendOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

  import spark.implicits._

  //Audit
  var program = ""
  var user_id = ""
  var app_id = ""
  var start_time: DateTime = DateTime.now()
  var start = ""

  var listBuffer = ListBuffer[Audit]()

  /*
  def loadData(): Map[String, DataFrame] = {

    //Reading the data into Data frames
    val startTime = DateTime.now
    println(s"[NDO-ETL] The loading of Data started with start time at :  + $startTime")

    //Reading the queries from config file
    println("Reading the queries from config file")
    //val us201712cms1801v180515Query = config.getString("us201712cms1801v180515_query").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
    //println(s"[NDO-ETL] Query for reading data from factRsttdMbrshpQuery table is $us201712cms1801v180515Query")
    val configTeradata = new TeradataConfig(configPath,env,queryFileCategory)

    val dbserverurl2 =configTeradata.dbserverurl2
    println("dbserverurl :"+dbserverurl2)

    val hiveFileFormat = configTeradata.hiveFileFormat
    println("hiveFileFormat :"+hiveFileFormat)

    val jdbcdriver = configTeradata.jdbcdriver
    val dbuserid = configTeradata.dbuserid
    val encriptedPassword = configTeradata.encriptedPassword
    val dbpassword =Encryption.decrypt(encriptedPassword)

    println("queryFilePath:"+queryFilePath)
    //val us201712cms1801v180515Df = spark.sql(us201712cms1801v180515Query)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val queryPath = new Path(queryFilePath)
    val queryInputStream = fs.open(queryPath)
    val predicates =
      Array(
        "('CA','IN','SC','PR','VI','AP','AE','AE')",
        "('OH','NY','CO','NE','SD','AK','GU')",
        "('GA','VA','KY','WI','AL','HI')",
        "('MO','CT','NH','NV','ME','TX','TN','MA','NJ','IL','NC','FL','MD','WA','PA','WV','IA','LA','ZZ','KS','MI','DC','AZ','MN','UT','EM','OK','NM','RI','OR','VT','MS','AR','ID','DE','WY','MT','ND')"
      ).map {
          case (state) =>
            s"TRIM(PROVIDER_STATE) IN  $state"
        }
    val queryProperties = new Properties();
    queryProperties.load(queryInputStream);

    val properties = new java.util.Properties()

    properties.setProperty("driver", jdbcdriver)
    properties.setProperty("user", dbuserid)
    properties.setProperty("password", dbpassword)
    properties.setProperty("numPartitions", "5")



    properties.setProperty("fetchSize", "5000")

    val tbl_qry=config.getString("query_src_tbl_nm")
      //"us201712cms1801v180515"
		val src_tbl_nmDf = spark.read.jdbc(dbserverurl2,tbl_qry ,predicates = predicates, properties)
		println(s"[NDO-ETL] Teradata table read succesfully")
    src_tbl_nmDf.explain()

    val mapDF = Map("src_tbl_nm" -> src_tbl_nmDf)

    println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
    println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    mapDF


  }
*/

  val tab_ui_prfsnl_spnd_smry = config.getString("query_tgt_tbl_nm")

  def loadData(): Map[String, DataFrame] = {

    //Reading the data into Data frames
    val startTime = DateTime.now
    println(s"[NDO-ETL] The loading of Data started with start time at :  + $startTime")

    //Reading the queries from config file
    println("Reading the queries from config file")

    val tbl_qry = config.getString("query_src_tbl_nm")

    val psruscurrentQuery = config.getString("query_psruscurrent").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()

    println(s"[NDO-ETL] Query for reading data from psruscurrent table is $psruscurrentQuery")

    val src_tbl_nmDf = spark.sql(psruscurrentQuery)

    val mapDF = Map("src_tbl_nm" -> src_tbl_nmDf)

    println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
    println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    mapDF
  }

  
  def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
    val startTime = DateTime.now
    println(s"[NDO-ETL] Processing Data Started: $startTime")
    //Reading the data frames as elements from Map
    val src_tbl_nmDf = inMapDF.getOrElse("src_tbl_nm", null)
    val targetTableName = config.getString("query_tgt_tbl_nm")
    val maxrun_id = getMAxRunId("UI_PRFSNL_SPND_SMRY")

    //Filter for states
    val src_tbl_st_nmFilDf = src_tbl_nmDf.filter(trim($"PROVIDER_STATE").isin("CA", "IN", "SC", "PR", "VI", "AP", "AE", "AE", "OH", "NY", "CO", "NE", "SD", "AK", "GU", "GA", "VA", "KY", "WI", "AL", "HI", "MO", "CT", "NH", "NV", "ME", "TX", "TN", "MA", "NJ", "IL", "NC", "FL", "MD", "WA", "PA", "WV", "IA", "LA", "ZZ", "KS", "MI", "DC", "AZ", "MN", "UT", "EM", "OK", "NM", "RI", "OR", "VT", "MS", "AR", "ID", "DE", "WY", "MT", "ND"))

    //Date params read from config file
    val src_tbl_nmFilDf = src_tbl_st_nmFilDf.filter($"TAX_ID" > "0000099" && substring($"TAX_ID", 1, 1).isin("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"))

    //val uiPrfsnlSpndSmryDf = src_tbl_nmFilDf.select((concat(year($"REPORTING_TIME_END"),month($"REPORTING_TIME_END")+(100)%100)).cast(IntegerType).alias("SNAP_YEAR_MNTH_NBR"),

    //     val uiPrfsnlSpndSmryDf = src_tbl_nmFilDf.select((concat(year($"REPORTING_TIME_END"),(substring(month($"REPORTING_TIME_END")+100,2,3)))).cast(LongType).alias("SNAP_YEAR_MNTH_NBR"),
    //     val uiPrfsnlSpndSmryDf = src_tbl_nmFilDf.select(lit(202005).alias("SNAP_YEAR_MNTH_NBR"),
    val uiPrfsnlSpndSmryDf = src_tbl_nmFilDf.select(
      from_unixtime($"REPORTING_TIME_END" / 1000, "yyyyMM").alias("SNAP_YEAR_MNTH_NBR"),
      trim($"TAX_ID").alias("AGRGTN_ID"),
      trim($"PROVIDER_STATE").alias("ST_CD"),
      (when(trim($"PRODUCT4") === "MEDICAID", lit("MEDICAID"))
        when (trim($"PRODUCT4") === "MEDICARE ADVANTAGE", lit("MEDICARE"))
        otherwise (lit("COMMERCIAL"))).alias("LOB_ID"),
      lit("TIN").alias("AGRGTN_TYPE_CD"),
      trim($"FUNDING").alias("FUNDING"),
      trim($"FUNDING3").alias("FUNDING3"),
      trim($"PRODUCT").alias("PRODUCT"),
      (when(trim($"SEGMENT").isNull, lit("NA")) otherwise(trim($"SEGMENT"))).alias("SEGMENT"),
      trim($"NETWORK_NAME").alias("NETWORK_NAME"),
      trim($"VOLUME").alias("VOLUME"),
      $"CHARGE_AMT".alias("CHARGE_AMT"),
      $"ALLOWED_AMT".alias("ALLOWED_AMT"),
      $"PAID_AMT".alias("PAID_AMT"),
      $"LOCAL_MEDICARE".alias("LOCAL_MEDICARE"),
      $"NATIONAL_MEDICARE".alias("NATIONAL_MEDICARE"),
      $"REPRICED_ALLOWED_AMT".alias("REPRICED_ALLOWED_AMT"),
      $"REPRICED_ADJUSTED_AMT".alias("REPRICED_ADJUSTED_AMT")).groupBy(
        $"SNAP_YEAR_MNTH_NBR",
        $"AGRGTN_ID",
        $"ST_CD",
        $"LOB_ID",
        $"AGRGTN_TYPE_CD",
        $"FUNDING",
        $"FUNDING3",
        $"PRODUCT",
        $"SEGMENT",
        $"NETWORK_NAME").agg(
          sum("VOLUME").alias("SRVC_CNT"),
          sum("CHARGE_AMT").alias("BILLD_CHRG_AMT"),
          sum("ALLOWED_AMT").alias("ALWD_AMT"),
          sum("PAID_AMT").alias("PAID_AMT"),
          sum("LOCAL_MEDICARE").alias("MEDCR_LCL_AMT"),
          sum("NATIONAL_MEDICARE").alias("MEDCR_NATL_AMT"),
          sum("REPRICED_ALLOWED_AMT").alias("REPRCD_ALLWD_AMT"),
          sum("REPRICED_ADJUSTED_AMT").alias("REPRCD_ADJ_ALLWD_AMT")).select(
            $"SNAP_YEAR_MNTH_NBR",
            $"AGRGTN_ID",
            $"ST_CD",
            $"LOB_ID",
            $"AGRGTN_TYPE_CD",
            $"FUNDING",
            $"FUNDING3",
            $"PRODUCT",
            $"SEGMENT",
            $"NETWORK_NAME",
            $"SRVC_CNT",
            $"BILLD_CHRG_AMT".cast(DecimalType(18, 2)),
            $"ALWD_AMT".cast(DecimalType(18, 2)),
            $"PAID_AMT".cast(DecimalType(18, 2)),
            $"MEDCR_LCL_AMT".cast(DecimalType(18, 2)),
            $"MEDCR_NATL_AMT".cast(DecimalType(18, 2)),
            $"REPRCD_ALLWD_AMT".cast(DecimalType(18, 2)),
            $"REPRCD_ADJ_ALLWD_AMT".cast(DecimalType(18, 2)),
            (when($"MEDCR_LCL_AMT" === 0, lit(0)) otherwise ((($"REPRCD_ADJ_ALLWD_AMT" / $"MEDCR_LCL_AMT") * 100).cast(DecimalType(18, 2)))).alias("MEDCR_LCL_PCT"),
            (when($"MEDCR_NATL_AMT" === 0, lit(0)) otherwise ((($"REPRCD_ADJ_ALLWD_AMT" / $"MEDCR_NATL_AMT") * 100).cast(DecimalType(18, 2)))).alias("MEDCR_NATL_PCT"),
            ($"BILLD_CHRG_AMT" - $"ALWD_AMT").cast(DecimalType(18, 2)).alias("DISCOUNT_AMT"),
            (when($"BILLD_CHRG_AMT" === 0, lit(0)) otherwise (((lit(1) - (($"ALWD_AMT") / ($"BILLD_CHRG_AMT"))) * 100)).cast(DecimalType(18, 2))).alias("DISCOUNT_PCT")).withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
            withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
            withColumn(lastUpdatedDate, lit(current_timestamp())).
            withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).withColumn("run_id", lit(maxrun_id))

    runIDInsert(targetTableName, maxrun_id)

    println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
    println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    val mapDF = Map(targetTableName -> uiPrfsnlSpndSmryDf)

    mapDF
  }

  def writeData(map: Map[String, DataFrame]): Unit = {
    val startTime = DateTime.now()
    //Writing the data to a table in Hive
    println(s"[NDO-ETL] Writing Dataframes to Hive started at: $startTime")

    map.par.foreach(x => {
      val tableName = x._1
      val startTime = DateTime.now()
      println(s"[NDO-ETL] Writing Dataframes to Hive table $tableName started at: $startTime")

      //Displaying the sample of data
      val df = x._2

      df.write.mode("overwrite").insertInto(warehouseHiveDB + """.""" + tableName)
      println(s"[NDO-ETL] Data load completed for table $warehouseHiveDB.$tableName")

      println(s"[NDO-ETL] Writing the data into back up table $warehouseHiveDB.$tableName" + "_bkp")

      val dfbkp = spark.sql(s"select * from $warehouseHiveDB.$tableName")

      dfbkp.write.mode("append").insertInto(warehouseHiveDB + """.""" + tableName + "_bkp")
      println(s"[NDO-ETL] Data load completed for table $warehouseHiveDB.$tableName" + "_bkp")

      println(s"[NDO-ETL] Before runIDInsert")
      val maxrun_id = dfbkp.first().getLong(26)

      runIDInsert(tableName, maxrun_id)

      println(s"[NDO-ETL] writing the data to target table $tableName is Completed at: " + DateTime.now())
      println(s"[NDO-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

    })
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
