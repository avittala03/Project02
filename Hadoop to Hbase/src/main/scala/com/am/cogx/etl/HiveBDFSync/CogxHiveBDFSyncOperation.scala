package com.am.cogx.etl.HiveBDFSync

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
import org.apache.spark.sql.SaveMode



class CogxHiveBDFSyncOperation (confFilePath: String, env: String, queryFileCategory: String) extends CogxOperationSession(confFilePath, env, queryFileCategory) with CogxOperator  {

	sc.setLogLevel("info")

	import spark.sql
	import spark.implicits._
	val repartitionNum = config.getInt("repartitionNum")
	val teradata_table_name = config.getString("teradata_table_name")
	var rowCount: Long = 0l
	val columnFamily = config.getString("hbase_table_columnfamily")
	val columnName = config.getString("hbase_table_columnname")
	

	def loadData(): Map[String, DataFrame] = {	info(s"[COGX] Reading the queries from config file")

			/* Read queryies from Configuration file */  
	     val backout_months = config.getInt("backout_months")
		   val purge_months = config.getInt("purge_months")
       val histDate:String = CogxDateUtils.addMonthToDateWithFormat(CogxDateUtils.getCurrentDateWithFormat("yyyyMMdd"),"yyyyMMdd", -(backout_months))
	     val purgeDate:Int = CogxDateUtils.addMonthToDateWithFormat(CogxDateUtils.getCurrentDateWithFormat("yyyyMMdd"),"yyyyMMdd", -(purge_months)).toInt
	     println ("HistDate " + histDate)
	     println ("PurgeDate " + purgeDate)
	      
	      /* Purge logic for Wide tables 
	       * Remove partitions less than 48 months 
	       */
	     
	     purgePartitionBDF(config.getString("inbound-hive-db"),"clm_wgs_gncclmp",purgeDate)
	     purgePartitionBDF(config.getString("inbound-hive-db"),"clm_wgs_gncdtlp",purgeDate)
	     purgePartitionBDF(config.getString("inbound-hive-db"),"clm_wgs_gncnatp_ea2",purgeDate)
	     
	     purgePartitionBDF(config.getString("stage-hive-db"),"clm_wgs_gncclmp_narrow",purgeDate)
	     purgePartitionBDF(config.getString("stage-hive-db"),"clm_wgs_gncdtlp_narrow",purgeDate)
	     purgePartitionBDF(config.getString("stage-hive-db"),"clm_wgs_gncnatp_ea2_narrow",purgeDate)
	     
        
	     val isIncremental = config.getString("isIncremental")
   
       println ("isIncremental: "+ isIncremental )
    
       var headerMsckSQL =  config.getString("query_msck_header").replace("<<sourceDB>>", config.getString("inbound-hive-db"))
       var detailMsckSQL =  config.getString("query_msck_detail").replace("<<sourceDB>>", config.getString("inbound-hive-db"))
       var ea2MsckSQL =  config.getString("query_msck_ea2").replace("<<sourceDB>>", config.getString("inbound-hive-db"))
	      
       var headerMsckSQLResult = spark.sql(headerMsckSQL)
       var detailMsckSQLResult = spark.sql(detailMsckSQL)
       var ea2MsckSQLResult = spark.sql(ea2MsckSQL)
	      
			/* 1. Claim Header */
			var clmwgsgncclmpSQL =  config.getString("query_clm_wgs_gncclmp").replace("<<sourceDB>>", config.getString("inbound-hive-db")).
			replace("<<Header_Additional_Filter>>",  config.getString("Header_Additional_Filter")).
			replace("<<histDate>>", histDate)
			
			
			var clmwgsgncdtlpSQL =   config.getString("query_clm_wgs_gncdtlp").replace("<<sourceDB>>", config.getString("inbound-hive-db")).replace("<<Detail_Additional_Filter>>",  config.getString("Detail_Additional_Filter"))
			var clmwgsgncnatpea2SQL =   config.getString("query_clm_wgs_gncnatp_ea2").replace("<<sourceDB>>", config.getString("inbound-hive-db")).replace("<<ea_Additional_Filter>>",  config.getString("ea_Additional_Filter"))

			
			    if ( isIncremental.equalsIgnoreCase("yes"))
     {
         ////// Modify the code to add data filter parameters in the cogxSQL

       var umAuditQuery = config.getString("cogx_audit_query").replace("<<auditSchema>>", config.getString("audit_schema"))
       
       var last_load_dtm = config.getString("default_incremental_startdt")      
       umAuditQuery = umAuditQuery.replace("<<programName>>",programName)
       println ("Audit Query => :" + umAuditQuery )
       
       if (env.equalsIgnoreCase("local") || config.getString("force_default_incremental").equalsIgnoreCase("yes") )
       {
       }
       else
       {
             /// Run on Cluster - Need to disable on local
             val dataDF = spark.sql(umAuditQuery)
             dataDF.show()
             println("count: "+dataDF.count)
             if (dataDF.head(1).isEmpty)
             {
               println("There is no previously completed loading. Set the load time to : "+ last_load_dtm)   
             }
             else
             {
                 val dataDFString = dataDF.head().toString()
                 println("last_load_dtm: "+dataDFString)
                 if (dataDFString.contains("null"))
                 {}
                 else
                 {
                    last_load_dtm=dataDFString.substring(0,10).replace("-", "").substring(1,9)
                 }
             }
        }
 
        println ("Last Load Date: " + last_load_dtm)
        clmwgsgncclmpSQL = clmwgsgncclmpSQL.replace("<<load_date>>",last_load_dtm)

        clmwgsgncdtlpSQL = clmwgsgncdtlpSQL.replace("<<load_date>>",last_load_dtm)
       
        clmwgsgncnatpea2SQL = clmwgsgncnatpea2SQL.replace("<<load_date>>",last_load_dtm)
        println ("new clmwgsgncclmpSQL: " + clmwgsgncclmpSQL)
     }
    
		
			
			info("clmwgsgncclmpSQL =>: "+ clmwgsgncclmpSQL)
			val clmHeaderDF = spark.sql(clmwgsgncclmpSQL).repartition(repartitionNum)
      val clmHeaderDFTrim = clmHeaderDF.columns.foldLeft(clmHeaderDF){(memoDF,colName)=> memoDF.withColumn(colName, trim(col(colName)))}
			
	
			 //2. Claim Detail 
			val clmDetailDF = spark.sql(clmwgsgncdtlpSQL).repartition(repartitionNum)
			val  clmDetailDFTrim = clmDetailDF.columns.foldLeft(clmDetailDF){(memoDF,colName)=>memoDF.withColumn(colName, trim(col(colName)))}


			// 3. EA2 
			val clmgncnatpea2DF = spark.sql(clmwgsgncnatpea2SQL).repartition(repartitionNum)
			val clmgncnatpea2DFTrim = clmgncnatpea2DF.columns.foldLeft(clmgncnatpea2DF){(memoDF,colName)=>memoDF.withColumn(colName, trim(col(colName)))}

			
			/*Creating a map of table name and respective dataframes */

			val dataMap = Map("clm_wgs_gncclmp" -> clmHeaderDFTrim   ,
					"clm_wgs_gncdtlp" -> clmDetailDFTrim,
					"clm_wgs_gncnatp_ea2" -> clmgncnatpea2DFTrim)


			return dataMap
	}


	def processData(inDFs: Map[String, DataFrame]): Map[String, DataFrame] = {


			    val clmHeaderDF = inDFs.getOrElse("clm_wgs_gncclmp", null)
			    
				  val clmDetailDF = inDFs.getOrElse("clm_wgs_gncdtlp", null)
					val clmgncnatpea2DF = inDFs.getOrElse("clm_wgs_gncnatp_ea2", null)



          var dataMap = Map("clm_wgs_gncclmp"->clmHeaderDF,  "clm_wgs_gncdtlp" -> clmDetailDF,
              "clm_wgs_gncnatp_ea2" -> clmgncnatpea2DF
          )
          
          return dataMap

	}


	def writeData(outDFs: Map[String, DataFrame]): Unit = {

			val dfHeader = outDFs.getOrElse("clm_wgs_gncclmp", null).repartition(repartitionNum)
      val dfDetail = outDFs.getOrElse("clm_wgs_gncdtlp", null).repartition(repartitionNum)
      val dfEA2 = outDFs.getOrElse("clm_wgs_gncnatp_ea2", null).repartition(repartitionNum)
      dfHeader.persist(StorageLevel.MEMORY_AND_DISK)
			
      dfHeader.show()
			val headerCount = dfHeader.count()
			ABC_load_count = headerCount
      
/*					dfHeader.persist(StorageLevel.MEMORY_AND_DISK)
					dfHeader.show()
					dfDetail.persist(StorageLevel.MEMORY_AND_DISK)
					dfDetail.show()				
					
					dfEA2.persist(StorageLevel.MEMORY_AND_DISK)
					dfEA2.show()			
					
					val headerCount = dfHeader.count()
					val detailCount = dfDetail.count()
					val EA2Count = dfEA2.count()
					
					info("cogx clm_wgs_gncclmp loadding count: " + headerCount)
					info("cogx clm_wgs_gncdtlp loadding count: " + detailCount)
					info("cogx clm_wgs_gncnatp_ea2 loadding count: " + EA2Count)
					*/
					dfHeader.write.mode(SaveMode.Overwrite).option("truncate", "true").insertInto(config.getString("NarrowHeaderDB").replace("<<sourceDB>>", config.getString("stage-hive-db")))
					dfDetail.write.mode(SaveMode.Overwrite).option("truncate", "true").insertInto(config.getString("NarrowDetailDB").replace("<<sourceDB>>", config.getString("stage-hive-db")))
					dfEA2.write.mode(SaveMode.Overwrite).option("truncate", "true").insertInto(config.getString("NarrowEA2DB").replace("<<sourceDB>>", config.getString("stage-hive-db")))
					


	}
	
	
	def purgePartitionBDF(db: String,table: String, rolloffdate: Int) {
	  
	  val minPrtnSql = config.getString("min_prtn_query").replace("<<sourceDB>>", db).replace("<<tblname>>", table)
	  val minPrtnDate = spark.sql(minPrtnSql).collect.head.getString(0).toInt
	  println ("Least partition date present in " +table + " is " + minPrtnDate)
	  if ( minPrtnDate < rolloffdate )
	  {
	    println("Dropping Parition " +minPrtnDate + " of table " + table ) 
	    spark.sql(s"""ALTER TABLE $db.$table DROP PARTITION (load_ingstn_id="$minPrtnDate")""")
	  }
		
	}

	
}