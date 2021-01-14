package com.am.cogx.etl.HiveUM

import scala.reflect.runtime.universe

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

import com.am.cogx.etl.config.CogxConfigKey
import com.am.cogx.etl.helper.CogxOperationSession
import com.am.cogx.etl.helper.CogxOperator
import com.am.cogx.etl.helper.CogxUMRecord
import com.am.cogx.etl.helper.cogxUmHistory
import com.am.cogx.etl.util.CogxCommonUtils.asJSONString

/**
 * Created by yuntliu on 1/20/2018.
 */

class CogxHiveUMOperation(confFilePath: String, env: String, queryFileCategory: String) extends CogxOperationSession(confFilePath, env, queryFileCategory) with CogxOperator {

	sc.setLogLevel("info")

	import spark.implicits._
	import spark.sql

	val teradata_table_name = config.getString("teradata_table_name")
	var rowCount: Long = 0l

	def loadData(): Map[String, DataFrame] = {

			val isIncremental = config.getString("isIncremental")

					var umHiveQuery = config.getString("hive_query").replace(CogxConfigKey.sourceDBPlaceHolder, inboundHiveDB).replace("<<HiveFilter>>", config.getString("hive_filter"))

					if (isIncremental.equalsIgnoreCase("yes")) {
         ////// Modify the code to add data filter parameters in the cogxSQL

       var umAuditQuery = config.getString("cogx_audit_query").replace("<<auditSchema>>", config.getString("audit_schema"))
       
       var last_load_dtm = config.getString("default_incremental_startdt")      
       umAuditQuery = umAuditQuery.replace("<<programName>>",programName)
       info ("Audit Query => :" + umAuditQuery )
       
       if (env.equalsIgnoreCase("local") || config.getString("force_default_incremental").equalsIgnoreCase("yes") )
       {
       }
       else
       {
             /// Run on Cluster - Need to disable on local
             val dataDF = spark.sql(umAuditQuery)
             dataDF.show()
             info ("count: "+dataDF.count)
             if (dataDF.head(1).isEmpty)
             {
               info ("There is no previously completed loading. Set the load time to : "+ last_load_dtm)   
             }
             else
             {
                 val dataDFString = dataDF.head().toString()
                 info ("last_load_dtm: "+dataDFString)
                 if (dataDFString.contains("null"))
                 {}
                 else
                 {
                    last_load_dtm=dataDFString.substring(0,10).replace("-", "").substring(1,9)
                 }
             }
        }
 
        info ("Last Load Date: " + last_load_dtm)
        umHiveQuery = umHiveQuery.replace("<<load_date>>",last_load_dtm)
        info ("new SQL: " + umHiveQuery)
     }
					
			//			Showing the queries read from config file 
			println(s"[COGX] CogX Hive query from properties file is : $umHiveQuery")

			//			Executing the queries
			val umHiveDf = spark.sql(umHiveQuery)

			//			Creating a map of table name and respective dataframes
			val dataMap = Map("hive_query" -> umHiveDf)
			return dataMap

	}

	def processData(inDFs: Map[String, DataFrame]): Map[String, DataFrame] = {
			val df0 = inDFs.getOrElse("hive_query", null)

					val df1 = df0.columns.foldLeft(df0) { (df, colName) =>
					df.schema(colName).dataType match {
					case StringType => { //println(s"%%%%%%% TRIMMING COLUMN ${colName.toLowerCase()} %%%%%% VALUE '${trim(col(colName))}'"); 
						df.withColumn(colName.toLowerCase, trim(col(colName)));
					}
					case _ => { // println("Column " + colName.toLowerCase() + " is not being trimmed"); 
						df.withColumn(colName.toLowerCase, col(colName));
					}
					}
			}

			df1.persist(MEMORY_AND_DISK)
			rowCount = df1.count()
			ABC_load_count = rowCount.toLong

			val repartitionNum = config.getInt("repartitionNum")
			val ArraySizeLimit = config.getInt("ArraySizeLimit")

			val CogxUmHbaseDataSet = df1.as[CogxUMRecord].repartition(repartitionNum)

			val RDDSet = CogxUmHbaseDataSet.rdd.repartition(repartitionNum).map(record => (record.src_sbscrbr_id, Set(record))).reduceByKey((a, b) => {
				if (a.size <= ArraySizeLimit) { a ++ b }
				else { print("======== Oversized subscriber (over size 3000):" + a.last.src_sbscrbr_id); a }
			}).repartition(repartitionNum)
			println("RDDset Count:" + RDDSet.count())
			var DSSet = RDDSet.map(k => { (new StringBuilder((DigestUtils.md5Hex(String.valueOf(k._1))).substring(0, 8)).append(k._1).toString(), asJSONString(new cogxUmHistory(k._2.toArray))) }).repartition(repartitionNum)
			println("DSSet count: " + DSSet.count())
			var newDF = DSSet.toDF("rowKey", "jsonData").repartition(repartitionNum)

			var dataMap = Map(teradata_table_name -> newDF)
			return dataMap

	}

	def writeData(outDFs: Map[String, DataFrame]): Unit = {

			val df1 = outDFs.getOrElse(teradata_table_name, null).toDF("rowKey", "jsonData")

					// df1.persist(StorageLevel.MEMORY_AND_DISK).count
					df1.show()
					val hbaseCount = df1.count()
					println(s"[COGX]Loading to HBase count: " + hbaseCount)

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

					val hbaseTable = config.getString("hbase_schema") + ":" + config.getString("hbase_table_name")

					if (env.equalsIgnoreCase("local")) {
						//// Run it on local ////
						getMapReduceJobConfiguration(hbaseTable)
					} else {
						/// Run it on Cluster now
						new PairRDDFunctions(putRDD).saveAsNewAPIHadoopDataset(getMapReduceJobConfiguration(hbaseTable))
						println("Loadde to HBase count: " + hbaseCount)
					}

	}

}