package com.am.cogx.etl.Ehub

import scala.reflect.runtime.universe

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.current_date
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.storage.StorageLevel

import com.am.cogx.etl.config.CogxConfigKey
import com.am.cogx.etl.helper.CogxOperationSession
import com.am.cogx.etl.helper.CogxOperator
import com.am.cogx.etl.helper.ehubWpidDelete
import com.am.cogx.etl.helper.ehubWpidExtract
import com.am.cogx.etl.helper.ehubWpidHistory
import com.am.cogx.etl.helper.ArraySizeLimitExceededException
import com.am.cogx.etl.util.CogxCommonUtils.asJSONString
import org.apache.spark.SparkException

/**
 * Created by yuntliu on 1/20/2018.
 */

class CogxEhubOperation(confFilePath: String, env: String, queryFileCategory: String) extends CogxOperationSession(confFilePath, env, queryFileCategory) with CogxOperator{

	sc.setLogLevel("info")

	import spark.implicits._
	import spark.sql

	var rowCount: Long = 0l
	var hashKeyGenDf: List[String] = null
	val incrementalTable = config.getString("ehub_incremental_table")
	val deleteTable = config.getString("ehub_delete_table")
	def loadData(): Map[String, DataFrame] = {

			//Extracting data from Text files
			val files = FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(config.getString("ehub_extracts_path")))
					val filesToDelete = FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(config.getString("ehub_delete_path")))

					var extractDf = spark.emptyDataset[ehubWpidExtract].rdd.toDF
					var deleteExtractDf = spark.emptyDataset[ehubWpidDelete].rdd.toDF

					//Extracting date for load_dt column from Extract file 
					//val dateOfExtract = config.getString("load_dt_temp")

					//Loading all the files from Extract path to dataframe
					files.foreach(filename => {
						val a = filename.getPath.toString()
								var df = fileExtract(a).repartition(1000)
								extractDf = extractDf.union(df)
					})

					// Loading all the files from Daily Delete Extracts to dataframe
					filesToDelete.foreach(filename => {
						val a = filename.getPath.toString()
								var df1 = fileDelete(a).repartition(1000)
								deleteExtractDf = deleteExtractDf.union(df1)
					})

					//Formatting the date to yyyy-MM-dd
					//Adding two columns to dataframe for auditing CONTRACT_START_DT,load_dt
					val incrmntlDf = extractDf.toDF().withColumn("START_DT", to_date(unix_timestamp(extractDf("START_DT"), "MMddyyyy").cast("timestamp")))
					.withColumn("END_DT", to_date(unix_timestamp(extractDf("END_DT"), "MMddyyyy").cast("timestamp")))
					.withColumn("CONTRACT_START_DT", lit(concat($"CONTRACT", $"START_DT")))
					.withColumn("load_dt", lit(current_date())).persist(StorageLevel.MEMORY_AND_DISK)
					
					val deleteDf = deleteExtractDf.toDF().withColumn("START_DT", to_date(substring($"START_DT", 0, 10)))
					.withColumn("END_DT", to_date(substring($"END_DT", 0, 10)))
					.withColumn("CONTRACT_START_DT", lit(concat($"CONTRACT", $"START_DT")))
					.withColumn("load_dt", lit(current_date())).persist(StorageLevel.MEMORY_AND_DISK)

					//Load data from history table
					val ehubHistoryQuery = config.getString("ehub_history_query").replaceAll(CogxConfigKey.warehouseDBPlaceHolder, warehouseHiveDB).toLowerCase()
					val historyDf = spark.sql(ehubHistoryQuery).persist(StorageLevel.MEMORY_AND_DISK)

					val dataMap = Map("ehub_extract" -> incrmntlDf,
							"ehub_delete" -> deleteDf,
							"ehub_hisotry" -> historyDf)

					return dataMap

	}

	def processData(inDFs: Map[String, DataFrame]): Map[String, DataFrame] = {
			val incrmntlDf = inDFs.getOrElse("ehub_extract", null)
					val deleteDf = inDFs.getOrElse("ehub_delete", null)
					val historyDf = inDFs.getOrElse("ehub_hisotry", null)

					val ehubHistoryTempTable = config.getString("ehub_history_temp_table")
					val ehubHistoryTable = config.getString("ehub_history_table")

					println(s"COGX : Loading the data into partitioned table $stagingHiveDB.$incrementalTable ")
					incrmntlDf.write.mode("overwrite").insertInto(s"$stagingHiveDB.$incrementalTable")
					println(s"COGX : Data got loaded into table $stagingHiveDB.$incrementalTable")

					println(s"COGX : Loading the data into partitioned table $stagingHiveDB.$deleteTable  ")
					deleteDf.write.mode("overwrite").insertInto(s"$stagingHiveDB.$deleteTable")
					println(s"COGX : Data got loaded into table $stagingHiveDB.$deleteTable")

					//Step 1 - Extracting the contract_strt_dt from incremental data
					val distinctContractIncr = sc.broadcast(incrmntlDf.select($"CONTRACT_START_DT").distinct.map(r => r.getString(0)).collect.toList)
					
					//Extracting the contract start dates from daily delete data
					val hardDelConList = sc.broadcast(deleteDf.select($"CONTRACT_START_DT").distinct.map(r => r.getString(0)).collect.toList)
					
					hashKeyGenDf = deleteDf.select($"CONTRACT_START_DT").distinct.map(r => r.getString(0)).collect.toList

					//Step 2 - Joining the data for incremental and history  
					val joinIncDf = incrmntlDf.join(historyDf, incrmntlDf("CONTRACT_START_DT") === historyDf("CONTRACT_START_DT"), "full_outer").distinct
					
					/*matchedDf - consists of the data common in both history table and daily incremental data.
												Only the updated data from daily incremental extracts is selected here */
					val matchedDf = joinIncDf.filter(incrmntlDf("CONTRACT_START_DT").isNotNull && historyDf("CONTRACT_START_DT").isNotNull).select(incrmntlDf("*")).distinct

					/*Step 3 - Extracting the contracts which not are updated from history.
										 unmatched - consists of data present only in history table and not in daily incremental data */
					val unmatched = joinIncDf.filter(incrmntlDf("CONTRACT_START_DT").isNull).select(historyDf("*")).distinct

					/*Step 4 - Extracting the contracts which are newly added in incremental.
										 newExtDf - consists of the contracts data present in only incremental table.
										 This data is needed in order to update the history table with the incremental data. */
					val newExtDf = joinIncDf.filter(historyDf("CONTRACT_START_DT").isNull).select(incrmntlDf("*")).distinct

					/* Step 5 - Updating the data from history table also adding the newly added and untouched records
					 uniondf - is the union of data  - for contracts which are updated in incremental along with the history data AND 
					 																	contracts which are present in history but not present in daily incremental AND 
					 																	contracts which are present only in daily incremental */
					val uniondf = matchedDf.union(unmatched).union(newExtDf).distinct

					// Step 6 - Deleting the records matching with deleted contracts
					val withdeleteDf = uniondf.filter($"CONTRACT_START_DT".isin(hardDelConList.value: _*) === false)

					//Daily incremental data after deleting the contracts
					val incrSelDf = withdeleteDf.filter($"CONTRACT_START_DT".isin(distinctContractIncr.value: _*))
					
					//count for Auditing 
					rowCount = incrSelDf.count()
					ABC_load_count = rowCount.toLong

					println("COGX :Row Count => " + rowCount)

					val repartitionNum = config.getInt("repartitionNum")
					val ArraySizeLimit = config.getInt("ArraySizeLimitEhub")

					val CogxUmHbaseDataSet = incrSelDf.as[ehubWpidExtract].repartition(repartitionNum)

					val RDDSet = CogxUmHbaseDataSet.rdd.repartition(repartitionNum).map(record => ((record.CONTRACT,record.START_DT), Set(record))).reduceByKey((a, b) => {
						 try{   
					  if (a.size <= ArraySizeLimit) { a ++ b }
								else {
								 throw ArraySizeLimitExceededException(s" COGX : Array size exceeded the limit $ArraySizeLimit. Please check the contract "+ a.last.CONTRACT + " and start_dt " + a.last.START_DT)  
								}}catch{  
               case e : ArraySizeLimitExceededException => e.printStackTrace()
                System.exit(1)
               a
            }  
					}).repartition(repartitionNum)
					var DSSet = RDDSet.map(k => {
						(new StringBuilder((DigestUtils.md5Hex(String.valueOf(k._1._1+k._1._2))).substring(0, 8)).append(k._1._1).append(k._1._2).toString(), asJSONString(new ehubWpidHistory(k._2.toArray)))
					})
					.repartition(repartitionNum)

					var newDF = DSSet.toDF("rowKey", "jsonData").repartition(repartitionNum)

					println("COGX : Total New Count: " + newDF.count())
					
					val dataMap = Map("ehub_extract" -> newDF)
					
						//Using temp table to load the history data
					println(s"COGX : Creating temp table $warehouseHiveDB.$ehubHistoryTempTable")
					spark.sql(s"CREATE TABLE IF NOT EXISTS $warehouseHiveDB.$ehubHistoryTempTable LIKE $warehouseHiveDB.$ehubHistoryTable")
					
					println(s"Loading the data into table $warehouseHiveDB.$ehubHistoryTempTable")
					withdeleteDf.write.mode("overwrite").insertInto(s"$warehouseHiveDB.$ehubHistoryTempTable")
					println(s"Data got loaded into table $warehouseHiveDB.$ehubHistoryTempTable")

					//Renaming the temp table to final table
					println(s"Dropping the table $warehouseHiveDB.$ehubHistoryTable")
					spark.sql(s"DROP TABLE IF EXISTS $warehouseHiveDB.$ehubHistoryTable purge")
				
					println(s"Renaming the table $warehouseHiveDB.$ehubHistoryTempTable to $warehouseHiveDB.$ehubHistoryTable")
					spark.sql(s"ALTER TABLE $warehouseHiveDB.$ehubHistoryTempTable RENAME TO $warehouseHiveDB.$ehubHistoryTable")
					
					spark.catalog.refreshTable(s"$warehouseHiveDB.$ehubHistoryTable")			
					
					return dataMap
	}

	def writeData(outDFs: Map[String, DataFrame]): Unit = {

			//Reading the processed data and persisting the same
			val df1 = outDFs.getOrElse("ehub_extract", null).toDF("rowKey", "jsonData")
					df1.persist(StorageLevel.MEMORY_AND_DISK)
					
					//Need a list with hash function for rowkeys to be deleted from Hbase 
					val HardDelConListCd: List[String] = hashKeyGenDf.map(x => DigestUtils.md5Hex(String.valueOf(x)).substring(0, 8) + x.toString())
					val hbaseTable = config.getString("hbase_schema") + ":" + config.getString("hbase_table_name")
					println("COGX: Hbase table name : "+ hbaseTable)
					
					val conf = HBaseConfiguration.create();
					val table = new HTable(conf, hbaseTable);
					val columnFamily = config.getString("hbase_table_columnfamily")
							val columnName = config.getString("hbase_table_columnname")

							val putRDD = df1.rdd.map(x => {
								val rowKey = x.getAs[String]("rowKey")
										val holder = x.getAs[String]("jsonData")
										//(rowKey, holder)
										val p = new Put(Bytes.toBytes(rowKey))
										p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(holder))
										(new org.apache.hadoop.hbase.io.ImmutableBytesWritable, p)
							})

							if (env.equalsIgnoreCase("local")) {
								//// Run it on local ////
								getMapReduceJobConfiguration(hbaseTable)
							} else {
								/// Run it on Cluster now
								new PairRDDFunctions(putRDD).saveAsNewAPIHadoopDataset(getMapReduceJobConfiguration(hbaseTable))

								//Create a marker with the column family name, column name and timestamp. 
								//Delete the hbase rowkeys for all the contracts present in HardDelConList
								HardDelConListCd.foreach(rowkey => {
									val marker = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn)
											table.delete(new Delete(Bytes.toBytes(rowkey)).addDeleteMarker(marker))
											println(s"COGX : Delete done for $rowkey")
								})
							}
					

					//Purging the data older than 10 days for backup in incremental 
					purgePartition(incrementalTable, stagingHiveDB, "incremental")

					//Purging the data older than 10 days for backup in delete 
					purgePartition(deleteTable, stagingHiveDB, "delete")

	}

	def fileExtract(fileinput: String): DataFrame = {
			val fileread = spark.read.textFile(fileinput)
					val header = fileread.first()
					val ehubExtractDfWH = fileread.filter(row => row != header).map { list => list.toString().replace("|~", "¤").split("¤", -1) }.map(a => ehubWpidExtract(a(0), a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8), a(9), a(10), a(11), a(12), a(13))).toDF()
					ehubExtractDfWH
	}
	def fileDelete(fileinput: String): DataFrame = {
			val fileread = spark.read.textFile(fileinput)
					val header = fileread.first()
					val ehubExtractDfWH = fileread.filter(row => row != header).map { list => list.toString().replace("|~", "¤").split("¤", -1) }.map(a => ehubWpidDelete(a(0), a(1), a(2), a(3), a(4))).toDF()
					ehubExtractDfWH
	}

	def purgePartition(tablename: String, schema: String, tableType: String) {

		//			  Purging the last 10 days of data
		//Select count(*) (Select distint load_dt from tbl)a; - if this count is >10;
		//Select min(distinct load_dt) from tbl;
		//Do alter partition with that min load_dt.

		var partitionsQuery = ""
				var getLoadDtQuery = ""
				if (tableType.equalsIgnoreCase("incremental")) {
					partitionsQuery = config.getString("incr_partition_count_query").replaceAll(CogxConfigKey.stagingDBPlaceHolder, stagingHiveDB).toLowerCase()
							getLoadDtQuery = config.getString("incr_get_min_load_dt_query").replaceAll(CogxConfigKey.stagingDBPlaceHolder, stagingHiveDB).toLowerCase()
				} else {
					partitionsQuery = config.getString("del_partition_count_query").replaceAll(CogxConfigKey.stagingDBPlaceHolder, stagingHiveDB).toLowerCase()
							getLoadDtQuery = config.getString("del_get_min_load_dt_query").replaceAll(CogxConfigKey.stagingDBPlaceHolder, stagingHiveDB).toLowerCase()
				}

		//Deriving the number of partitions from incremental or delete table
		val noOfPartitionDf = spark.sql(partitionsQuery).collect.head.getLong(0)

				//Checking if the number of partitions is greater than 10
				if (noOfPartitionDf > 10) {
					//Extracting the minimum load_dt from the table (partition column)
					val getMinLoadDt = spark.sql(getLoadDtQuery).collect.head.getString(0)
							try {
								println(s"COGX : Dropping the partition for load_dt $getMinLoadDt for table type $tableType")
								//Dropping the table for partition having minimun load_dt
								spark.sql(s"""ALTER TABLE $schema.$tablename DROP PARTITION (load_dt="$getMinLoadDt")""")
								println(s"COGX : Partition droppped for load_dt $getMinLoadDt for table type $tableType")
							} catch {
							case e: Exception => {
								//Exception is handled if the partition is not existing or for any other exceptions while dropping the partition
								println(s"COGX : Partition not found for $getMinLoadDt for table type $tableType")
							}
							}
				}

	}
	//If it is an external table, we could use deleteFile method below to delete the external hdfs location

	def deleteFile(path: String) {
		val filePath = new Path(path)
				val HDFSFilesSystem = filePath.getFileSystem(new Configuration())
				if (HDFSFilesSystem.exists(filePath)) {
					HDFSFilesSystem.delete(filePath, true)
				}
	}

}