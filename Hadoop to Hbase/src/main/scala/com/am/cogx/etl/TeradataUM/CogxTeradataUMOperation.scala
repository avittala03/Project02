package com.am.cogx.etl.TeradataUM
import java.io.File

import com.am.cogx.etl.config.CogxConfigKey
import com.am.cogx.etl.helper.{CogxOperationSession, CogxOperator}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit}
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
import com.am.cogx.etl.helper.CogxUMRecord
import com.am.cogx.etl.helper.cogxUmInfo
import com.am.cogx.etl.helper.cogxUmHistory
import com.am.cogx.etl.helper.cogxRecord
import collection.JavaConverters._
import java.io.PrintWriter
import java.io.StringWriter
import com.am.cogx.etl.util.CogxCommonUtils.asJSONString
//import com.am.cogx.etl.util.CogxCommonUtils.getMapReduceJobConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import java.io.InputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.storage.StorageLevel

/**
  * Created by yuntliu on 1/20/2018.
  */


class CogxTeradataUMOperation (confFilePath: String, env: String, queryFileCategory: String) extends CogxOperationSession(confFilePath, env,queryFileCategory) with CogxOperator {

  sc.setLogLevel("info")
  
  import spark.sql
  import spark.implicits._
  
  
  
  val teradata_table_name = config.getString("teradata_table_name")
  var rowCount: Long = 0l
  
  
   def loadData(): Map[String, DataFrame] = {
    
    //print ("(something)".replaceAll("\\(", "").replaceAll("\\)", ""))
 
    info ("Teradata is loaded by: " + config.getString("username"))
    
    val isIncremental = config.getString("isIncremental")
   
    info ("isIncremental: "+ isIncremental )
    
     var cogxSQL=config.getString("teradata_query").replace("<<TeraDataSchema>>", config.getString("teradata_schema")).replace("<<TeraFilter>>",  config.getString("teradata_filter"))

     if ( isIncremental.equalsIgnoreCase("yes"))
     {
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
        cogxSQL = cogxSQL.replace("<<load_date>>",last_load_dtm)
        info ("new SQL: " + cogxSQL)
     }
    
     val dbTable: String = "(".concat(cogxSQL).concat(") T")

     
     val conf = spark.sparkContext.hadoopConfiguration
     conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,  config.getString("credentialproviderurl"))
     var cred = "******"
     if (env.equalsIgnoreCase("local"))
      {
        //// Test on Local - Action 2 //////////////   
           cred = "*******"
      }
     else
     {
        ////// Run on Server cluster
       cred = String.valueOf(conf.getPassword( config.getString("password")))
     }

     // info(cred)
     
      val dboptions = scala.collection.mutable.Map[String, String]()
      dboptions += ("url" -> config.getString("dbserverurl"))
      dboptions += ("user" -> config.getString("username"))
      dboptions += ("password" -> cred)
      dboptions += ("driver" -> config.getString("jdbcdriver"))
      dboptions += ("dbtable" -> dbTable)
      dboptions += ("fetchSize" -> String.valueOf(config.getString("fetch_size")))

    
    var cogxUMDF = spark.read.format("jdbc").options(dboptions).load()
    info("INFO: COGX SQL => " + cogxSQL)

    var dataMap = Map(teradata_table_name->cogxUMDF)
    
    return dataMap
    
  }
  

    
    def processData(inDFs: Map[String, DataFrame]): Map[String, DataFrame] = {
      val df0 = inDFs.getOrElse(teradata_table_name, null)
  
      val   df1 = df0.columns.foldLeft(df0) { (df, colName) =>
            df.schema(colName).dataType match {
              case StringType => { //info(s"%%%%%%% TRIMMING COLUMN ${colName.toLowerCase()} %%%%%% VALUE '${trim(col(colName))}'"); 
              df.withColumn(colName.toLowerCase, trim(col(colName))); }
              case _ => {// info("Column " + colName.toLowerCase() + " is not being trimmed"); 
              df.withColumn(colName.toLowerCase, col(colName)); }
            }
          }
  
      rowCount = df1.count()
      ABC_load_count = rowCount.toLong
      info("INFO: CogX Row Count => " + rowCount)
      val repartitionNum = config.getInt("repartitionNum")
      val ArraySizeLimit = config.getInt("ArraySizeLimit")
      
      val CogxUmHbaseDataSet = df1.as[CogxUMRecord].repartition(repartitionNum)
      
     //println("Original From Teradata: ")
     //println("===========================================")
     //CogxUmHbaseDataSet.foreach(x=>{println( x.toString() ) })
     
     //// New code with reducebykey  ////

     val RDDSet = CogxUmHbaseDataSet.rdd.repartition(repartitionNum).map(record => (record.src_sbscrbr_id, Set(record))).reduceByKey((a,b)=>{
       if (a.size <=ArraySizeLimit)  
       {a ++ b}
       else{print("======== Oversized subscriber (over size 3000):" + a.last.src_sbscrbr_id);a}
       }).repartition(repartitionNum)
     info ("RDDset Count:" + RDDSet.count())
     var DSSet = RDDSet.map(k=>{( new StringBuilder((DigestUtils.md5Hex(String.valueOf(k._1))).substring(0, 8)).append(k._1).toString(),asJSONString(new cogxUmHistory(k._2.toArray)))}).repartition(repartitionNum)
     info ("DSSet count: " + DSSet.count())
     var newDF= DSSet.toDF("rowKey", "jsonData").repartition(repartitionNum)
 
       println("New DataSet: ")
       println("===========================================")
       //newDF.foreach(x=>{println("rowKey:"+x.getString(0) + "=> jsonData: " +  x.getString(1)  ) })
       println("Total New Count: "+newDF.count())


      
     //////////////////
      
      ////////////// Original code with Groupbykey //////////////////////
   /*  
      var groupCogxUmHbaseDataSet = CogxUmHbaseDataSet.groupByKey { key =>
          ((key.src_sbscrbr_id))
        }
     print ("groupCogxUmHbaseDataSet count: " + groupCogxUmHbaseDataSet.count())
      
      val groupedUMHbaseDataSet1 = groupCogxUmHbaseDataSet.mapGroups((k, iter) => {
          var cogxUmSet = Set[CogxUMRecord]()
          val cogxUmInfo = iter.map(cogx => new cogxUmInfo(
            new CogxUMRecord(
              cogx.rfrnc_nbr,
              cogx.srvc_line_nbr,
              cogx.clncl_sor_cd,
              cogx.mbrshp_sor_cd,
              cogx.um_srvc_stts_cd,
              cogx.src_um_srvc_stts_cd,
              cogx.src_sbscrbr_id,
              cogx.src_mbr_cd,
              cogx.prmry_diag_cd,
              cogx.rqstd_place_of_srvc_cd,
              cogx.src_rqstd_place_of_srvc_cd,
              cogx.authrzd_place_of_srvc_cd,
              cogx.src_authrzd_place_of_srvc_cd,
              cogx.rqstd_srvc_from_dt,
              cogx.authrzd_srvc_from_dt,
              cogx.rqstd_srvc_to_dt,
              cogx.authrzd_srvc_to_dt,
              cogx.rqstd_proc_srvc_cd,
              cogx.authrzd_proc_srvc_cd,
              cogx.rqstd_qty,
              cogx.authrzd_qty,
              cogx.src_um_prov_id,
              cogx.prov_id,
              cogx.src_um_prov_id_rp,
              cogx.prov_id_rp,
              cogx.src_prov_frst_nm_rp,
              cogx.src_prov_last_nm_rp,
              cogx.src_prov_frst_nm,
              cogx.src_prov_last_nm))).toArray
      
       for (um <- cogxUmInfo) {  cogxUmSet += um.cogxUMdata   }
       val cogxUM = new cogxUmHistory(cogxUmSet.toArray)
          (String.valueOf(k), cogxUM) // getString(0) i.e. hbase_key is the column for the rowkey 
        }).repartition(2000)
      
    info("groupedUMHbaseDataSet1")
  //groupedUMHbaseDataSet1.show(1000,false)
    print ("groupedUMHbaseDataSet1 count: " + groupedUMHbaseDataSet1.count())
        
        
        val DF3 = groupedUMHbaseDataSet1.map{
          case (key, value) => {
            val digest = DigestUtils.md5Hex(String.valueOf(key))
            val rowKey = new StringBuilder(digest.substring(0, 8)).append(key).toString()
            val holder = asJSONString(value)
            val p = new Put(Bytes.toBytes(rowKey))
            ((rowKey),(holder))
          }
        }
      
       val newDF =  DF3.toDF("rowKey", "jsonData")
       */

       
       println("Original DataSet: ")
       println("===========================================")
       //newDF.foreach(x=>{println("rowKey:"+x.getString(0) + "=> jsonData: " +  x.getString(1)  ) })
       println("Total original Count: "+newDF.count())
       
        ////////////////////////////////////// End of original Groupbykey ////////////////////////////      
       

    
       var dataMap = Map(teradata_table_name->newDF)
       return dataMap
    
  }

  def writeData(outDFs: Map[String, DataFrame]): Unit = {

    val df1 = outDFs.getOrElse(teradata_table_name, null).toDF("rowKey", "jsonData")

    // df1.persist(StorageLevel.MEMORY_AND_DISK).count
    df1.show()
    val hbaseCount = df1.count()
    info("Loading to HBase count: "+ hbaseCount)
    
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
       info("Loadde to HBase count: "+ hbaseCount)
      }


  }

  
}