package com.am.ndo.auditTable

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import org.joda.time.DateTime
import org.joda.time.Minutes
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Locale
import org.apache.spark.storage.StorageLevel
import com.am.ndo.config.ConfigKey
import com.am.ndo.helper.OperationSession
import com.am.ndo.helper.Operator
import com.am.ndo.util.DateUtils
import com.am.ndo.helper.WriteExternalTblAppend
import com.typesafe.config.ConfigException
import scala.collection.mutable.ListBuffer
import com.am.ndo.helper.AuditSourceTable
import com.am.ndo.helper.AuditTargetTable
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window
import java.lang.ClassCastException

class AuditTableOperation(configPath: String, env: String, queryFileCategory: String) extends OperationSession(configPath, env, queryFileCategory) with Operator {

	import spark.implicits._

	//Audit
	val auditSourceTabl = "ndo_source_audit_table"
	val auditTargetTbl = "ndo_target_audit_table"

	def loadData(): Map[String, DataFrame] = {
			val startTime = DateTime.now
					println(s"[NDO-ETL] The loading of Data started with start time at :  + $startTime")
					val metadataFileName = config.getString("audit_table_metadata")

					val metadata = spark.read.format("csv").option("header", "true").load(configPath + "/" + metadataFileName).select($"table_name", $"table_type").distinct

					metadata.show
					
					val metadataList = metadata.map(a => (a.getString(0),a.getString(1))).collect().toList

//					for (row <- metadata.rdd.collect) {
//
//						val tablename = row.mkString(",").split(",")(0).toString
//								println(s"[NDO-ETL] tablename from the metadata : " + tablename)
//
//								val tableType = row.mkString(",").split(",")(1).toString().toLowerCase()
//								println(s"[NDO-ETL] tableType from the metadata : " + tableType)
//
//								if (tableType.equals("target".toLowerCase()))
//									auditTargetTableLoad(tablename, warehouseHiveDB)
//
//									else if (tableType.equals("source".toLowerCase()))
//										auditSourceTableLoad(tablename, inboundHiveDB)
//
//										else println(s"[NDO-ETL] table type is incorrect")
//					}

			for ( row <- metadataList) {
			  
			  val tablename = row._1
			  println(s"[NDO-ETL] tablename from the metadata : " + tablename)
			  val tableType = row._2
			  println(s"[NDO-ETL] tableType from the metadata : " + tableType)
			  
			  if (tableType.equals("target".toLowerCase()))
			     auditTargetTableLoad(tablename, warehouseHiveDB)
			  else if (tableType.equals("source".toLowerCase()))
			     auditSourceTableLoad(tablename, inboundHiveDB)
			  else 
					 println(s"[NDO-ETL] table type is incorrect")
			}
					
/* Ignore the below duplicate checks in prd as this scenario would never occur */
			removeTargetTblDuplicates(auditTargetTbl, warehouseHiveDB)
//			removeSourceTblDuplicates(auditSourceTabl, warehouseHiveDB)

			println(s"[NDO-ETL] Loading data completed at : " + DateTime.now())
			println(s"[NDO-ETL] Time Taken for loading the data :" + Minutes.minutesBetween(startTime, DateTime.now()).getMinutes + " mins.")

			null
	}

	def processData(inMapDF: Map[String, DataFrame]): Map[String, DataFrame] = {

			null
	}

	def writeData(map: Map[String, DataFrame]): Unit = {
	}

	def auditSourceTableLoad(tableName: String, hiveDB: String): Unit = {

			import spark.implicits._

			var df = spark.emptyDataFrame

			println(s"[NDO-ETL] Reading the source Table")
			try {
				df = spark.table(s"$hiveDB.$tableName")
			} catch {
			case e: NoSuchTableException =>
			println(s"[NDO-ETL] Error Reading the source Table")
			println(s"[NDO-ETL] Source table cannot be found.")

			}
			var snapNbr: Long = 0
					var ndoAuditDF = spark.emptyDataFrame

					try {

						val listBuffer = ListBuffer[AuditSourceTable]()
								snapNbr = df.select($"SNAP_YEAR_MNTH_NBR").orderBy($"SNAP_YEAR_MNTH_NBR".desc).head.getLong(0)
								println(s"[NDO-ETL] SNAP_YEAR_MNTH_NBR : $snapNbr")

								listBuffer += AuditSourceTable(tableName, snapNbr)
								ndoAuditDF = listBuffer.toDS().withColumn("last_rfrsh_dt", to_date(lit(current_timestamp()))).withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
								withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
								withColumn(lastUpdatedDate, lit(current_timestamp())).
								withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser))
								
								val checkQuery = s"""select * from $warehouseHiveDB.$auditSourceTabl where table_name='$tableName' and TO_DATE(last_rfrsh_dt)="""+to_date(lit(current_timestamp()))
								println(s"[NDO-ETL] Query is : " + checkQuery)
								val checkDf = spark.sql(checkQuery)
								
								 if (checkDf.head(1).isEmpty) {
								
								println(s"Write started for appending the data to final  table $warehouseHiveDB.$auditSourceTabl at:" + DateTime.now())
								ndoAuditDF.write.mode("append").insertInto(warehouseHiveDB + """.""" + auditSourceTabl)
								println(s"[NDO-ETL] Table loaded as $warehouseHiveDB.$auditSourceTabl")

								ndoAuditDF.show()
								
								 }
								 else {
								   
								   println(s"[NDO-ETL] Table name already exists in the audit table. Cannot insert a duplicate record.")
								 }

					} catch {

					case e: AnalysisException =>
					println(s"[NDO-ETL] The read operation on the table failed with Analysis exception :")

					}

	}

	def auditTargetTableLoad(tableName: String, hiveDB: String): Unit = {

			import spark.implicits._

			var df = spark.emptyDataFrame
			println(s"[NDO-ETL] This is a target table :"+  tableName)
			println(s"[NDO-ETL] Reading the target Table")
			try {
				df = spark.table(s"$hiveDB.$tableName")
			} catch {
			case e: NoSuchTableException =>
			println(s"[NDO-ETL] Error Reading the target Table")
			println(s"[NDO-ETL] Target table cannot be found.")

			}
			var run_id: Long = 0
					var snapNbr: Long = 0
					var ndoAuditDF = spark.emptyDataFrame
					var checkDfQuery =""
    
					try {

								try 	{
									run_id = df.select($"run_id").head.getLong(0)
									println(s"[NDO-ETL] runId : $run_id")
											}
								catch {
									case e: ClassCastException =>{
										run_id = df.select($"run_id").head.getInt(0)
																								}
											}
								val listBuffer = ListBuffer[AuditTargetTable]()
	
  								if(tableName.equalsIgnoreCase("ndo_ehoppa_base") || tableName.equalsIgnoreCase("ndo_ehoppa_base_mbr") || tableName.equalsIgnoreCase("ndo_ehoppa_base_mbr_cnty"))
  	              {
  	               snapNbr=getDateRange()
  	               println(s"The end date for Ehoppa table - $tableName " + snapNbr )
  					        
  	              }
								  
  								else if (tableName.equalsIgnoreCase("ndo_rfrl_ptrn"))
  								{
  								  snapNbr=0
  								}
  								else
  								{
    								try {
    								snapNbr = df.select($"SNAP_YEAR_MNTH_NBR").orderBy($"SNAP_YEAR_MNTH_NBR".desc).head.getLong(0)
    								println(s"[NDO-ETL] SNAP_YEAR_MNTH_NBR : $snapNbr")
    	              } catch {
    	                case e: org.apache.spark.sql.AnalysisException =>{
    								  try {
    								  snapNbr = df.select($"BKBN_SNAP_YEAR_MNTH_NBR").orderBy($"BKBN_SNAP_YEAR_MNTH_NBR".desc).head.getInt(0)
    								   } catch {
    								     case e: ClassCastException =>{
    								        snapNbr = df.select($"BKBN_SNAP_YEAR_MNTH_NBR").orderBy($"BKBN_SNAP_YEAR_MNTH_NBR".desc).head.getLong(0)
    								     }
    								   }
    								println(s"[NDO-ETL] SNAP_YEAR_MNTH_NBR : $snapNbr")
    								  }
    								case e: ClassCastException =>{
    								  snapNbr = df.select($"SNAP_YEAR_MNTH_NBR").orderBy($"SNAP_YEAR_MNTH_NBR".desc).head.getInt(0)
    								println(s"[NDO-ETL] SNAP_YEAR_MNTH_NBR : $snapNbr")
    								  }
    								
    								}
  								}
								

								
								listBuffer += AuditTargetTable(run_id, tableName, snapNbr)

								try {
								
									val last_rfrsh_dt = df.select(df(lastUpdatedDate)).orderBy(df(lastUpdatedDate).desc).head.getTimestamp(0).toString
											println(s"[NDO-ETL] The $lastUpdatedDate column is a timetsamp in target table ")
											
											ndoAuditDF = listBuffer.toDS().withColumn("last_rfrsh_dt", to_date(lit(last_rfrsh_dt))).withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
											withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
											withColumn(lastUpdatedDate, lit(current_timestamp())).
											withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser))
											
								checkDfQuery = s"""select * from $warehouseHiveDB.$auditTargetTbl where tbl_nm='$tableName' and run_id=$run_id and TO_DATE(last_rfrsh_dt)=TO_DATE('$last_rfrsh_dt')"""
								println(s"[NDO-ETL] Query is : " + checkDfQuery)
											
								} catch {
								case e: ClassCastException =>{
								println(s"[NDO-ETL] The $lastUpdatedDate column is not timetsamp in target table ")
								println(s"[NDO-ETL] Trying to read it as a string.")
								val last_rfrsh_dt = df.select(df(lastUpdatedDate)).orderBy(df(lastUpdatedDate).desc).head.getString(0)
								
								ndoAuditDF = listBuffer.toDS().withColumn("last_rfrsh_dt", to_date(lit(last_rfrsh_dt))).withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
								withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
								withColumn(lastUpdatedDate, lit(current_timestamp())).
								withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser))
								
								checkDfQuery = s"""select * from $warehouseHiveDB.$auditTargetTbl where tbl_nm='$tableName' and run_id=$run_id and TO_DATE(last_rfrsh_dt)=TO_DATE('$last_rfrsh_dt')"""
								println(s"[NDO-ETL] Query is : " + checkDfQuery)}
								
								case e: AnalysisException =>
								  {
								    println(s"[NDO-ETL] The $lastUpdatedDate column is present in target table ")
								  	val last_rfrsh_dt = to_date(lit(current_timestamp))
								  	
								  ndoAuditDF = listBuffer.toDS().withColumn("last_rfrsh_dt", last_rfrsh_dt).withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
								withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
								withColumn(lastUpdatedDate, lit(current_timestamp())).
								withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser))
								
								checkDfQuery = s"""select * from $warehouseHiveDB.$auditTargetTbl where tbl_nm='$tableName' and run_id=$run_id and TO_DATE(last_rfrsh_dt)=TO_DATE($last_rfrsh_dt)"""
								println(s"[NDO-ETL] Query is : " + checkDfQuery)
								
								  }
								}

						ndoAuditDF.show
						
		/* Commenting duplicate check logic */							
								val checkDf = spark.sql(checkDfQuery)
								
								 if (checkDf.head(1).isEmpty) {
    
						println(s"[NDO-ETL] Write started for appending the data to final  table $warehouseHiveDB.$auditTargetTbl at:" + DateTime.now())
						ndoAuditDF.write.mode("append").insertInto(warehouseHiveDB + """.""" + auditTargetTbl)
						println(s"[NDO-ETL] Table loaded as $warehouseHiveDB.$auditTargetTbl")
							 }
						
						 else {
								   
								   println(s"[NDO-ETL] Table name already exists in the audit table with the same run id and last refresh date. Cannot insert a duplicate record.")
								 }
									

					} catch {

					case e: AnalysisException =>
					println(s"[NDO-ETL] The read operation on the table failed with Analysis exception :")
					
					case e: java.util.NoSuchElementException =>{
								println(s"[NDO-ETL] The table has zero records")
								
								val listBuffer = ListBuffer[AuditTargetTable]()
								listBuffer += AuditTargetTable(0, tableName, 0)
								val last_rfrsh_dt = to_date(lit(current_timestamp))
								val ndoAuditDF = listBuffer.toDS().withColumn("last_rfrsh_dt", last_rfrsh_dt).withColumn("RCRD_CREATN_DTM", lit(current_timestamp())).
								withColumn("RCRD_CREATN_USER_ID", lit(sc.sparkUser)).
								withColumn(lastUpdatedDate, lit(current_timestamp())).
								withColumn("LAST_UPDT_USER_ID", lit(sc.sparkUser))
								
								checkDfQuery = s"""select * from $warehouseHiveDB.$auditTargetTbl where tbl_nm='$tableName' and run_id=0 and TO_DATE(last_rfrsh_dt)=TO_DATE($last_rfrsh_dt)"""
								println(s"[NDO-ETL] Query is : " + checkDfQuery)
	
	/* Commenting duplicate check logic */
			
								val checkDf = spark.sql(checkDfQuery)
								
								 if (checkDf.head(1).isEmpty) {
	
						println(s"[NDO-ETL] Write started for appending the data to final  table $warehouseHiveDB.$auditTargetTbl at:" + DateTime.now())
						ndoAuditDF.write.mode("append").insertInto(warehouseHiveDB + """.""" + auditTargetTbl)
						println(s"[NDO-ETL] Table loaded as $warehouseHiveDB.$auditTargetTbl")
							 }
								 else{
								    println(s"[NDO-ETL] Table name already exists in the audit table with the same run id and last refresh date. Cannot insert a duplicate record.")
								 }
							
								  }

					}

	}
	def removeTargetTblDuplicates(tablename: String, hiveDB: String) = {
			println(s"[NDO-ETL] Removing Duplicates for table $hiveDB.$tablename")
			val ndoAuditTblDf = spark.table(s"$hiveDB.$tablename")
			println(s"[NDO-ETL] Reading the table $hiveDB.$tablename")

			ndoAuditTblDf.write.mode("overwrite").saveAsTable(hiveDB + """.""" + tablename + "_temp")

			val ndoAuditTblDfTemp = spark.table(s"$hiveDB.$tablename" + "_temp")

			val windowFunc = Window.partitionBy($"tbl_nm", $"run_id").orderBy($"last_rfrsh_dt".desc)

			val auditFinal = ndoAuditTblDfTemp.withColumn("row_number", row_number().over(windowFunc))
			.filter($"row_number" === 1)
			.drop($"row_number")

			println(s"Write started for overwrite the data to final  table $hiveDB.$tablename at:" + DateTime.now())
			auditFinal.write.mode("overwrite").insertInto(hiveDB + """.""" + tablename)
			println(s"[NDO-ETL] Table loaded as $hiveDB.$tablename")

	}

	def removeSourceTblDuplicates(tablename: String, hiveDB: String) = {

			println(s"[NDO-ETL] Removing Duplicates for table $hiveDB.$tablename")
			val ndoAuditTblDf = spark.table(s"$hiveDB.$tablename").persist(StorageLevel.DISK_ONLY)
			println(s"[NDO-ETL] Reading the table $hiveDB.$tablename")

			ndoAuditTblDf.write.mode("overwrite").saveAsTable(hiveDB + """.""" + tablename + "_temp")

			val ndoAuditTblDfTemp = spark.table(s"$hiveDB.$tablename" + "_temp")

			val windowFunc = Window.partitionBy($"table_name").orderBy($"last_rfrsh_dt".desc)

			val auditFinal = ndoAuditTblDfTemp.withColumn("row_number", row_number().over(windowFunc))
			.filter($"row_number" === 1)
			.drop($"row_number")

			println(s"Write started for overwrite the data to final  table $hiveDB.$tablename at:" + DateTime.now())
			auditFinal.write.mode("overwrite").insertInto(hiveDB + """.""" + tablename)
			println(s"[NDO-ETL] Table loaded as $hiveDB.$tablename")

	}
	
		def getDateRange():  Long =
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

				rangeEndDate.toLong

		}

	@Override
	def beforeLoadData() {

	}

	@Override
	def afterWriteData() {

	}

}
