package com.am.ndo.referralPatterns

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.when
import org.joda.time.DateTime
import org.joda.time.Minutes
import org.apache.spark.sql.functions._
import com.am.ndo.config.ConfigKey
import com.am.ndo.helper.Audit
import com.am.ndo.helper.OperationSession
import com.am.ndo.helper.Operator
import com.am.ndo.util.DateUtils

class ReferralPatternsOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

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
					val etgClmOutptQuery = config.getString("query_etg_clm_outpt").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val pcrCostEpsdQuery = config.getString("query_pcr_cost_epsd").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val clmProvQuery = config.getString("query_clm_prov").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val provQuery = config.getString("query_prov").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val etgCnfnmntQuery = config.getString("query_etg_cnfnmnt").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val etgProvSmryQuery = config.getString("query_etg_prov_smry").replaceAll(ConfigKey.sourceDBPlaceHolder, inboundHiveDB).toLowerCase()
					val apiProvQuery = config.getString("query_api_prov").replaceAll(ConfigKey.warehouseDBPlaceHolder, warehouseHiveDB).toLowerCase()
					
					println(s"[NDO-ETL] Query for reading data from etgClmOutptQuery table is $etgClmOutptQuery")
					println(s"[NDO-ETL] Query for reading data from pcrCostEpsdQuery table is $pcrCostEpsdQuery")
					println(s"[NDO-ETL] Query for reading data from pcrCostEpsdQuery table is $clmProvQuery")
					println(s"[NDO-ETL] Query for reading data from pcrCostEpsdQuery table is $provQuery")
					println(s"[NDO-ETL] Query for reading data from pcrCostEpsdQuery table is $etgCnfnmntQuery")
					println(s"[NDO-ETL] Query for reading data from pcrCostEpsdQuery table is $etgProvSmryQuery")
					println(s"[NDO-ETL] Query for reading data from pcrCostEpsdQuery table is $apiProvQuery")

					val etgClmOutptDf = spark.sql(etgClmOutptQuery)
					println(s"[NDO-ETL] Showing sample data for etgClmOutptDf")
					etgClmOutptDf.show

					val pcrCostEpsdDf = spark.sql(pcrCostEpsdQuery)
					println(s"[NDO-ETL] Showing sample data for pcrCostEpsdDf")
					pcrCostEpsdDf.show

					val clmProvDf = spark.sql(clmProvQuery)
					println(s"[NDO-ETL] Showing sample data for clmProvDf")
					clmProvDf.show

					val provDf = spark.sql(provQuery)
					println(s"[NDO-ETL] Showing sample data for provDf")
					provDf.show

					  val etgCnfnmntDf = spark.sql(etgCnfnmntQuery)
					    println(s"[NDO-ETL] Showing sample data for etgCnfnmntDf")
					    etgCnfnmntDf.show

					val etgProvSmryDf = spark.sql(etgProvSmryQuery)
					println(s"[NDO-ETL] Showing sample data for etgProvSmryDf")
					etgProvSmryDf.show

					val apiProvDf = spark.sql(apiProvQuery)
					println(s"[NDO-ETL] Showing sample data for apiProvDf")
					apiProvDf.show

					val mapDF = Map("etg_clm_outpt" -> etgClmOutptDf, "pcr_cost_epsd" -> pcrCostEpsdDf, "clm_prov" -> clmProvDf, "prov" -> provDf, "etg_cnfnmnt" -> etgCnfnmntDf , "etg_prov_smry" -> etgProvSmryDf,
							"api_prov" -> apiProvDf)

					println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					mapDF

	}

	def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {
			val startTime = DateTime.now
					println(s"[NDO-ETL] Processing Data Started: $startTime")
					//Reading the data frames as elements from Map
					val etgClmOutptDf = inMapDF.getOrElse("etg_clm_outpt", null)
					val pcrCostEpsdDf = inMapDF.getOrElse("pcr_cost_epsd", null)
					val clmProvDf = inMapDF.getOrElse("clm_prov", null)
					val provDf = inMapDF.getOrElse("prov", null)
				  val etgCnfnmntDf = inMapDF.getOrElse("etg_cnfnmnt", null)
					val etgProvSmryDf = inMapDF.getOrElse("etg_prov_smry", null)
					val apiProvDf = inMapDF.getOrElse("api_prov", null)
					val lastUpdatedDate = config.getString(ConfigKey.auditColumnName)
					val maxEtgRunId = getmaxEtgRunId(inMapDF)
					println(s"[NDO-ETL] The max etg_run_id is " +maxEtgRunId)
				
						//Filters
					
					val targetTableRfrlPatrn = config.getString("target_table_rfrl_ptrn")
					println(s"[NDO-ETL] the target table name from ")
					
					println(s"[NDO-ETL] Calculating the max run id for the table: $targetTableRfrlPatrn")
					
					val maxrun_id = getMAxRunId(targetTableRfrlPatrn)
					println(s"[NDO-ETL] Max run id is : $maxrun_id")
					
					val pcrCostEpsdFil = pcrCostEpsdDf.filter((pcrCostEpsdDf("NTWK_ST_CD").isin("CA", "NY", "VA") &&
							pcrCostEpsdDf("PEER_MRKT_CD").===("7")) || ((pcrCostEpsdDf("NTWK_ST_CD").isin("CA", "NY", "VA") === false) &&
									pcrCostEpsdDf("PEER_MRKT_CD").===("2")))
				  
					val etgProvSmryFil = etgProvSmryDf.filter($"TAX_ID".!==(" "))

					val pcrCostEpsdFil2 = pcrCostEpsdFil.filter(pcrCostEpsdFil("ETG_RUN_ID") === maxEtgRunId && pcrCostEpsdFil("GRP_AGRGTN_TYPE_CD") === "TIN")
					// 1st Volatile table

					val etgJoinPcrDf = etgClmOutptDf.join(pcrCostEpsdDf, pcrCostEpsdDf("ETG_RUN_ID") === maxEtgRunId && pcrCostEpsdDf("ETG_RUN_ID") === etgClmOutptDf("ETG_RUN_ID") &&
					pcrCostEpsdDf("EPSD_NBR") === etgClmOutptDf("EPSD_NBR") && pcrCostEpsdDf("MCID") === etgClmOutptDf("MCID") && etgClmOutptDf("ETG_RUN_ID") === maxEtgRunId &&
					etgClmOutptDf("CNFNMNT_NBR").!==(" ") && etgClmOutptDf("EPSD_NBR").!==(" "), "inner")

					val etgCnfnmntNbrDf = etgJoinPcrDf.select(
							etgClmOutptDf("ETG_RUN_ID"),
							etgClmOutptDf("EPSD_NBR"),
							etgClmOutptDf("MCID"),
							etgClmOutptDf("CNFNMNT_NBR"),
							etgClmOutptDf("clm_sor_cd"),
							etgClmOutptDf("CLM_ADJSTMNT_KEY"),
							etgClmOutptDf("fclty_ind_cd"),
							etgClmOutptDf("inpat_cd")).distinct
						
					// 2nd Volatile Table
					val etgCnfnJoinPcrDf = etgCnfnmntNbrDf.join(pcrCostEpsdFil, etgCnfnmntNbrDf("ETG_RUN_ID") === pcrCostEpsdFil("ETG_RUN_ID") &&
					etgCnfnmntNbrDf("EPSD_NBR") === pcrCostEpsdFil("EPSD_NBR") && etgCnfnmntNbrDf("MCID") === pcrCostEpsdFil("MCID"), "inner")

					val etgCnJoinClmProvDf = etgCnfnJoinPcrDf.join(clmProvDf, etgCnfnmntNbrDf("CLM_ADJSTMNT_KEY") === clmProvDf("CLM_ADJSTMNT_KEY") &&
					clmProvDf("PROV_NM").!==(" ") &&
					clmProvDf("CLM_PROV_ROLE_CD").isin("04", "09", "10"), "inner")

					val etgCnJoinProvDf = etgCnJoinClmProvDf.join(provDf, provDf("PROV_SOR_CD") === clmProvDf("PROV_SOR_CD") &&
					provDf("PROV_ID") === clmProvDf("PROV_ID") &&
					provDf("RCRD_STTS_CD").!==("DEL"), "inner")

					val rfrlFcltyNmSel = etgCnJoinProvDf.select(
							clmProvDf("src_clm_prov_id"),
							clmProvDf("PROV_NM"),
							clmProvDf("RPTG_PROV_NM"),
							clmProvDf("CLM_PROV_ID_TYPE_CD"),
							etgCnfnmntNbrDf("fclty_ind_cd"),
							etgCnfnmntNbrDf("inpat_cd"),
							clmProvDf("CLM_PROV_ROLE_CD"),
							provDf("PROV_CTGRY_CD"),
							clmProvDf("CLM_PROV_ORDR_NBR"))
							
							val rfrlFcltyNmgroupBy = rfrlFcltyNmSel.groupBy(
							$"src_clm_prov_id",
							$"PROV_NM",
							$"RPTG_PROV_NM",
							$"CLM_PROV_ID_TYPE_CD",
							$"fclty_ind_cd",
							$"inpat_cd",
							$"CLM_PROV_ROLE_CD",
							$"PROV_CTGRY_CD",
							$"CLM_PROV_ORDR_NBR").agg(count("*").alias("Rcrd_Cntr")).select($"src_clm_prov_id",
							$"PROV_NM",
							$"RPTG_PROV_NM",
							$"CLM_PROV_ID_TYPE_CD",
							$"fclty_ind_cd",
							$"inpat_cd",
							$"CLM_PROV_ROLE_CD",
							$"PROV_CTGRY_CD",
							$"CLM_PROV_ORDR_NBR",
							$"Rcrd_Cntr")

					val windowFunc = Window.partitionBy("src_clm_prov_id").orderBy(
							((when($"CLM_PROV_ID_TYPE_CD".===("TAX"), 1) otherwise (2)).asc),
							$"fclty_ind_cd".desc,
							$"inpat_cd".desc,
							(when($"PROV_CTGRY_CD".isin("02", "03"), 1) otherwise (2)).asc,
							$"CLM_PROV_ORDR_NBR".asc, $"CLM_PROV_ROLE_CD".desc)
							
					val rfrlFcltyNm = rfrlFcltyNmgroupBy.select("*")
					.withColumn("COUNT_ROWS", row_number() over (windowFunc)).
					filter($"COUNT_ROWS".===(1)).drop($"COUNT_ROWS")
					
					
				  //3rd Volatile Table
					val subQueryEtg = etgCnfnmntNbrDf.select($"ETG_RUN_ID", $"EPSD_NBR", $"MCID", $"CNFNMNT_NBR").distinct
				  
					val subQJoinPcr = pcrCostEpsdFil2.join(subQueryEtg, pcrCostEpsdFil2("ETG_RUN_ID") === etgCnfnmntNbrDf("ETG_RUN_ID") &&
					pcrCostEpsdFil2("EPSD_NBR") === subQueryEtg("EPSD_NBR") && pcrCostEpsdFil2("MCID") === subQueryEtg("MCID"), "inner")
				  
					val pcrCJoinEtg = subQJoinPcr.join(etgCnfnmntDf, pcrCostEpsdFil2("ETG_RUN_ID") === etgCnfnmntDf("ETG_RUN_ID") &&
					pcrCostEpsdFil2("EPSD_NBR") === etgCnfnmntDf("EPSD_NBR") &&
					etgCnfnmntNbrDf("CNFNMNT_NBR") === etgCnfnmntDf("CNFNMNT_NBR"), "inner")
				  
					val etgCJoinESmry = pcrCJoinEtg.join(etgProvSmryFil, pcrCJoinEtg("ETG_FCLTY_PROV_ID") === etgProvSmryFil("ETG_UNIQ_PROV_ID") &&
					pcrCostEpsdFil2("ETG_RUN_ID") === etgProvSmryFil("ETG_RUN_ID"), "inner")
					
				  
					val ndoRfrlWrkSel = etgCJoinESmry.select(
							pcrCostEpsdFil2("ETG_RUN_ID"),
							etgProvSmryFil("TAX_ID").alias("FCLTY_TAX_ID"),
							pcrCostEpsdFil2("RSPNSBL_EDW_TAX_ID").alias("PROV_TAX_ID"),
							pcrCostEpsdFil2("RSPNSBL_TIN_GRP_NM").alias("PROV_NM"),
							pcrCostEpsdFil2("SPCLTY_PRMRY_CD"),
							pcrCostEpsdFil2("SPCLTY_PRMRY_DESC"),
							pcrCostEpsdFil2("NTWK_ST_CD").alias("ST_CD"),
							pcrCostEpsdFil2("PCR_LOB_DESC").alias("LOB_ID"),
							pcrCostEpsdFil2("PEER_MRKT_CD"),
							$"PEER_MRKT_CD_SHRT_DESC".alias("Peer_Market_Description"),
							(when(pcrCostEpsdFil2("BNCHMRK_PROD_DESC").===("NO GROUPING"), "ALL") otherwise (pcrCostEpsdFil2("BNCHMRK_PROD_DESC"))).alias("PROD_ID"),
							pcrCostEpsdFil2("PRMRY_SUBMRKT_CD").alias("SUBMRKT_CD"),
							etgCnfnmntDf("FCLTY_ALWD_AMT"),
							pcrCostEpsdFil2("EPSD_NBR"))
					
				  
					val ndoRfrlWrk = ndoRfrlWrkSel.groupBy(
							$"ETG_RUN_ID",
							$"FCLTY_TAX_ID",
							$"PROV_TAX_ID",
							$"PROV_NM",
							$"SPCLTY_PRMRY_CD",
							$"SPCLTY_PRMRY_DESC",
							$"ST_CD",
							$"LOB_ID",
							$"PEER_MRKT_CD",
							$"Peer_Market_Description",
							$"PROD_ID",
							$"SUBMRKT_CD").agg(
									sum($"FCLTY_ALWD_AMT").alias("FCLTY_TOTL_ALLWD_AMT"),
									countDistinct($"EPSD_NBR").alias("FCLTY_EPSD_NBR")).select(
											$"ETG_RUN_ID",
											$"FCLTY_TAX_ID",
											$"PROV_TAX_ID",
											$"PROV_NM",
											$"SPCLTY_PRMRY_CD",
											$"SPCLTY_PRMRY_DESC",
											$"ST_CD",
											$"LOB_ID",
											$"PEER_MRKT_CD",
											$"Peer_Market_Description",
											$"PROD_ID",
											$"SUBMRKT_CD",
											$"FCLTY_TOTL_ALLWD_AMT",
											$"FCLTY_EPSD_NBR")
											
					
					/*NDO Provider Facility Referral Pattern Result Table*/
				  val apiProvDf1 = apiProvDf.alias("apiProvDf1")
          val apiProvDf2 = apiProvDf.alias("apiProvDf2")

					val subQApiProv1 = apiProvDf1.filter(col("apiProvDf1.AGRGTN_TYPE_CD") === "TIN").select(col("apiProvDf1.PROV_TAX_ID"), col("apiProvDf1.PROV_TAX_NM")).distinct
				  
					val subQApiProv2 = apiProvDf2.filter(col("apiProvDf2.AGRGTN_TYPE_CD") === "TIN").select(col("apiProvDf2.PROV_TAX_ID")).distinct
				  
					val ndoRfrlPtrnSub = ndoRfrlWrk.join(subQApiProv1, ndoRfrlWrk("FCLTY_TAX_ID") === col("apiProvDf1.PROV_TAX_ID"), "left_outer")
				  
					val ndoRfrlJoinRfrl = ndoRfrlPtrnSub.join(rfrlFcltyNm, ndoRfrlWrk("FCLTY_TAX_ID") === rfrlFcltyNm("src_clm_prov_id"), "left_outer")
				  
					val ndoRfrlJoinSubQ2 = ndoRfrlJoinRfrl.join(subQApiProv2, ndoRfrlWrk("PROV_TAX_ID") === col("apiProvDf2.PROV_TAX_ID"), "left_outer").filter(ndoRfrlWrk("FCLTY_TAX_ID").>=("001000000")) //-- valid TINs only
				  
				  
					val ndoRfrlPtrn = ndoRfrlJoinSubQ2.select(
							ndoRfrlWrk("FCLTY_TAX_ID"),
							ndoRfrlWrk("PROV_TAX_ID"),
							ndoRfrlWrk("SPCLTY_PRMRY_CD"),
							ndoRfrlWrk("ST_CD"),
							ndoRfrlWrk("LOB_ID"),
							ndoRfrlWrk("PROD_ID"),
							(when(rfrlFcltyNm("src_clm_prov_id").isNull, (coalesce(col("apiProvDf1.PROV_TAX_NM"), lit("NOTF")))) otherwise (rfrlFcltyNm("PROV_NM"))).alias("FCLTY_NM"),
							ndoRfrlWrk("PROV_NM"),
							ndoRfrlWrk("SPCLTY_PRMRY_DESC"),
							ndoRfrlWrk("PEER_MRKT_CD"),
							ndoRfrlWrk("SUBMRKT_CD"),
							ndoRfrlWrk("FCLTY_TOTL_ALLWD_AMT"),
							ndoRfrlWrk("FCLTY_EPSD_NBR"),
							(when(col("apiProvDf1.PROV_TAX_ID").isNull, "N") otherwise ("Y")).alias("API_FCLTY_IND_CD"),
							(when(col("apiProvDf2.PROV_TAX_ID").isNull, "N") otherwise ("Y")).alias("API_PROV_IND_CD")).distinct.
							withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
							withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
							withColumn(lastUpdatedDate, lit(current_timestamp())).
							withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser)).
							withColumn("run_id", lit(maxrun_id))
				  
					println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")
					
					
					val mapDF = Map(targetTableRfrlPatrn -> ndoRfrlPtrn)
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
								val maxrun_id = dfbkp.first().getLong(19)
								
								dfbkp.write.mode("append").insertInto(warehouseHiveDB + """.""" + tableName + "_bkp")
								println(s"[NDO-ETL] Data load completed for table $warehouseHiveDB.$tableName" + "_bkp")
								
								runIDInsert(tableName, maxrun_id)
								
								println(s"[NDO-ETL] writing the data to target table $tableName is Completed at: " + DateTime.now())
								println(s"[NDO-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

					})
					println(s"[NDO-ETL] writing the data to target tables is Completed at: " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

	}
	
	def getmaxEtgRunId(inMapDF: Map[String, DataFrame]): Long = {
    val startTime = DateTime.now()
    //Writing the data to a table in Hive
    info(s"[HPIP-ETL] Writing Dataframes to Hive started at: $startTime")

   				val etgClmOutptDf = inMapDF.getOrElse("etg_clm_outpt", null).select($"ETG_RUN_ID").distinct
					val pcrCostEpsdDf = inMapDF.getOrElse("pcr_cost_epsd", null).select($"ETG_RUN_ID").distinct
					  

    val maxEtgRunId = etgClmOutptDf.join(pcrCostEpsdDf, Seq("ETG_RUN_ID")).orderBy($"ETG_RUN_ID".desc).head.getLong(0)

    info(s"[HPIP-ETL] writing the data to target tables is Completed at: " + DateTime.now())
    info(s"[HPIP-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")
    maxEtgRunId

  }

	def getMAxRunId(tablename: String): Long = {
			val startTime = DateTime.now()
					//Writing the data to a table in Hive
					println(s"[NDO-ETL] Writing Dataframes to Hive started at: $startTime")
					val tab = f"$tablename".toUpperCase()
					//***  Query   for Max run_id column value*****//
					val checkMaxRunId = s"SELECT run_id from (SELECT  run_id,rank() over (order by run_id desc) as r from  $warehouseHiveDB.run_id_table where sub_area='$tab' ) S where S.r=1"
					println(s"[NDO-ETL] Max run id for the table $warehouseHiveDB.$tablename is" + checkMaxRunId)

					//***  Getting the max value of run_id *****//
					val checkMaxRunIdDF = spark.sql(checkMaxRunId).collect()
					val maxRunIdVal = checkMaxRunIdDF.head.getLong(0)
					
					//***  Incrementing the value  of max run_id*****//
					val maxrun_id=maxRunIdVal+1
					
					println(s"[NDO-ETL] writing the data to target tables is Completed at: " + DateTime.now())
					println(s"[NDO-ETL] Time Taken for writing the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")
					maxrun_id
	}

	def runIDInsert(tablename: String, maxrun_id: Long) = {
			spark.sql(f"use $warehouseHiveDB")
			val run_id_tbl = f"$tablename".toUpperCase()
			//append run_id and subject area to the run_id table
			val runid_insert_query = "select  " + maxrun_id + " as run_id,'" + run_id_tbl + "' as sub_area ";
			val data = spark.sql(runid_insert_query)
					data.write.mode("append").insertInto(f"$warehouseHiveDB.run_id_table")
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
