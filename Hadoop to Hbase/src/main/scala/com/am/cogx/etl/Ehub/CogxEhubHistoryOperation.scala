package com.am.cogx.etl.Ehub

import scala.reflect.runtime.universe

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import com.am.cogx.etl.config.CogxConfigKey
import com.am.cogx.etl.helper.CogxOperationSession
import com.am.cogx.etl.helper.CogxOperator
import com.am.cogx.etl.helper.ehubWpidExtract
import com.am.cogx.etl.helper.ehubWpidHistory
import com.am.cogx.etl.util.CogxCommonUtils.asJSONString
import com.am.cogx.etl.helper.ArraySizeLimitExceededException

/**
 * Created by yuntliu on 1/20/2018.
 */

class CogxEhubHistoryOperation(confFilePath: String, env: String, queryFileCategory: String) extends CogxOperationSession(confFilePath, env, queryFileCategory) with CogxOperator {

	sc.setLogLevel("info")

	import spark.implicits._
	import spark.sql

	var rowCount: Long = 0l
	def loadData(): Map[String, DataFrame] = {

			//Load data from history table
			val ehubHistoryQuery = config.getString("ehub_history_query").replaceAll(CogxConfigKey.warehouseDBPlaceHolder, warehouseHiveDB).toLowerCase()
					val historyDf = spark.sql(ehubHistoryQuery).persist(StorageLevel.MEMORY_AND_DISK)

					val dataMap = Map("ehub_hisotry" -> historyDf)

					return dataMap

	}

	def processData(inDFs: Map[String, DataFrame]): Map[String, DataFrame] = {
			val historyDf = inDFs.getOrElse("ehub_hisotry", null)

					rowCount = historyDf.count()
					ABC_load_count = rowCount.toLong

					println("COGX :Row Count => " + rowCount)

					val repartitionNum = config.getInt("repartitionNum")
					val ArraySizeLimit = config.getInt("ArraySizeLimitEhub")

					val CogxUmHbaseDataSet = historyDf.as[ehubWpidExtract].repartition(repartitionNum)

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
						(new StringBuilder((DigestUtils.md5Hex(String.valueOf(k._1._1 + k._1._2))).substring(0, 8)).append(k._1._1).append(k._1._2).toString(), asJSONString(new ehubWpidHistory(k._2.toArray)))
					})
					.repartition(repartitionNum)

					var newDF = DSSet.toDF("rowKey", "jsonData").repartition(repartitionNum)

					println("COGX : Total New Count: " + newDF.count())

					val dataMap = Map("ehub_extract" -> newDF)

					return dataMap
	}

	def writeData(outDFs: Map[String, DataFrame]): Unit = {

			//Reading the processed data and persisting the same
			val df1 = outDFs.getOrElse("ehub_extract", null).toDF("rowKey", "jsonData")
					df1.persist(StorageLevel.MEMORY_AND_DISK)

					//Need a list with hash function for rowkeys to be deleted from Hbase 
					val hbaseTable = config.getString("hbase_schema") + ":" + config.getString("hbase_table_name")
					println("COGX: Hbase table name : " + hbaseTable)

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

					}

	}

}