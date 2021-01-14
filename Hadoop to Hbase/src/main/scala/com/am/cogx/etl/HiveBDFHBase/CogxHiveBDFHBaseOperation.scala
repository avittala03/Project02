package com.am.cogx.etl.HiveBDFHBase



import java.io.File

import com.am.cogx.etl.config.CogxConfigKey
import com.am.cogx.etl.helper.{ CogxOperationSession, CogxOperator }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ current_timestamp, lit }
import org.apache.spark.sql.functions._
import org.apache.hadoop.security.alias.CredentialProviderFactory
import com.am.cogx.etl.helper.CogxOperator
import org.apache.spark.sql.types.StringType
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.io.PrintWriter
import java.io.StringWriter
import org.apache.commons.codec.digest.DigestUtils
import com.am.cogx.etl.helper.CogxClaimRecord
import com.am.cogx.etl.helper.cogxClaimInfo
import com.am.cogx.etl.helper.cogxClaimHistory
import collection.JavaConverters._
import java.io.PrintWriter
import java.io.StringWriter
import com.am.cogx.etl.util.CogxCommonUtils.asJSONString
import com.am.cogx.etl.util.CogxCommonUtils.Json_function
//import com.am.cogx.etl.util.CogxCommonUtils.getMapReduceJobConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.fs.{ FSDataInputStream, Path }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import java.io.InputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.am.cogx.etl.util.CogxDateUtils
import com.sun.org.apache.xalan.internal.xsltc.compiler.ValueOf




class CogxHiveBDFHBaseOperation (confFilePath: String, env: String, queryFileCategory: String) extends CogxOperationSession(confFilePath, env, queryFileCategory) with CogxOperator  {

	sc.setLogLevel("info")

	import spark.sql
	import spark.implicits._
	val repartitionNum = config.getInt("repartitionNum")
	val teradata_table_name = config.getString("teradata_table_name")
	var rowCount: Long = 0l
	val columnFamily = config.getString("hbase_table_columnfamily")
	val columnName = config.getString("hbase_table_columnname")
	

	def loadData(): Map[String, DataFrame] = {	info(s"[COGX] Reading the queries from config file")


		//	val clmwgsgncnatpea2SQL =   "select rowKey, jsonData from ts_ccpcogxph_nogbd_r000_sg.COGX_BDF_STAGING"
	    val clmwgsgncnatpea2SQL =  config.getString("stageSQL").replace("<<sourceDB>>", config.getString("warehouse-hive-db"))
			val clmgncnatpea2DF = spark.sql(clmwgsgncnatpea2SQL).repartition(repartitionNum)
			
			val dataMap = Map("clm_wgs_gncclmp" -> clmgncnatpea2DF)


			return dataMap
	}


	def processData(inDFs: Map[String, DataFrame]): Map[String, DataFrame] = {

			    val newDF = inDFs.getOrElse("clm_wgs_gncclmp", null)

	  			   
          
          var dataMap = Map(teradata_table_name->newDF)
          
          return dataMap

	}


	def writeData(outDFs: Map[String, DataFrame]): Unit = {

			val df1 = outDFs.getOrElse(teradata_table_name, null).toDF("rowKey", "jsonData")

					 df1.persist(StorageLevel.MEMORY_AND_DISK).count
					val hbaseCount = df1.count()
					info("Loading to HBase count: "+ hbaseCount)
					ABC_load_count=hbaseCount

					val columnFamily = config.getString("hbase_table_columnfamily")
					val columnName = config.getString("hbase_table_columnname")
					val putRDD = df1.rdd.map(x => {
						val rowKey = x.getAs[String]("rowKey")
								val holder = x.getAs[String]("jsonData")
								//(rowKey, holder)
								val p = new Put(Bytes.toBytes(rowKey))
								p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(holder))
								(new org.apache.hadoop.hbase.io.ImmutableBytesWritable, p)
					}
							)

					val hbaseTable = config.getString("hbase_schema") + ":" + config.getString("hbase_table_name")

					if (env.equalsIgnoreCase("local"))
					{
						//// Run it on local ////
						getMapReduceJobConfiguration(hbaseTable)
					}
					else
					{
						/// Run it on Cluster now
						new PairRDDFunctions(putRDD).saveAsNewAPIHadoopDataset(getMapReduceJobConfiguration(hbaseTable))
						//info("Loadde to HBase count: "+ hbaseCount)
					}


	}
	
}