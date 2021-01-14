package com.am.cogx.etl.Driver

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
class CogxTemplateOperation(confFilePath: String, env: String, queryFileCategory: String) extends CogxOperationSession(confFilePath, env,queryFileCategory) with CogxOperator {

  sc.setLogLevel("info")
  
  import spark.sql
  import spark.implicits._
  
  
  
  val teradata_table_name = config.getString("teradata_table_name")
  var rowCount: Long = 0
  
   def loadData(): Map[String, DataFrame] = {
 
    print ("Teradata is loaded by: " + config.getString("username"))
    
     var cogxSQL=config.getString("teradata_query")
    
    //var cogxSQL="""SELECT DISTINCT sum(1) over( rows unbounded preceding ) as  ROW_ID,   TRIM(R.RFRNC_NBR) AS  RFRNC_NBR, TRIM(S.SRVC_LINE_NBR) AS SRVC_LINE_NBR, TRIM(R.CLNCL_SOR_CD) AS CLNCL_SOR_CD, TRIM(R.MBRSHP_SOR_CD) AS MBRSHP_SOR_CD, TRIM(STTS.UM_SRVC_STTS_CD) AS UM_SRVC_STTS_CD, TRIM(STTS.SRC_UM_SRVC_STTS_CD) AS SRC_UM_SRVC_STTS_CD, TRIM(R.SRC_SBSCRBR_ID) AS SRC_SBSCRBR_ID, TRIM(R.SRC_MBR_CD) AS SRC_MBR_CD, TRIM(R.PRMRY_DIAG_CD) AS PRMRY_DIAG_CD, TRIM(S.RQSTD_PLACE_OF_SRVC_CD) AS RQSTD_PLACE_OF_SRVC_CD, TRIM(S.SRC_RQSTD_PLACE_OF_SRVC_CD) AS SRC_RQSTD_PLACE_OF_SRVC_CD, TRIM(S.AUTHRZD_PLACE_OF_SRVC_CD) AS AUTHRZD_PLACE_OF_SRVC_CD, TRIM(S.SRC_AUTHRZD_PLACE_OF_SRVC_CD) AS SRC_AUTHRZD_PLACE_OF_SRVC_CD, TRIM(S.RQSTD_SRVC_FROM_DT) AS RQSTD_SRVC_FROM_DT, TRIM(S.AUTHRZD_SRVC_FROM_DT) AS AUTHRZD_SRVC_FROM_DT, TRIM(S.RQSTD_SRVC_TO_DT) AS RQSTD_SRVC_TO_DT, TRIM(S.AUTHRZD_SRVC_TO_DT) AS AUTHRZD_SRVC_TO_DT, TRIM(S.RQSTD_PROC_SRVC_CD) AS RQSTD_PROC_SRVC_CD, TRIM(S.AUTHRZD_PROC_SRVC_CD) AS AUTHRZD_PROC_SRVC_CD, TRIM(S.RQSTD_QTY) AS RQSTD_QTY, TRIM(S.AUTHRZD_QTY) AS AUTHRZD_QTY, TRIM(SP.SRC_UM_PROV_ID) AS SRC_UM_PROV_ID, TRIM(SP.PROV_ID) AS PROV_ID, TRIM(RP.SRC_UM_PROV_ID) AS SRC_UM_PROV_ID_RP, TRIM(RP.PROV_ID) AS PROV_ID_RP, TRIM(RP.SRC_PROV_FRST_NM) AS SRC_PROV_FRST_NM_RP, TRIM(RP.SRC_PROV_LAST_NM) AS SRC_PROV_LAST_NM_RP, TRIM(SP.SRC_PROV_FRST_NM) AS SRC_PROV_FRST_NM, TRIM(SP.SRC_PROV_LAST_NM) AS SRC_PROV_LAST_NM FROM T89_ETL_VIEWS_ENT_XM.UM_RQST R INNER JOIN T89_ETL_VIEWS_ENT_XM.UM_SRVC S ON  R.RFRNC_NBR = S.RFRNC_NBR   AND R.CLNCL_SOR_CD = S.CLNCL_SOR_CD and R.CLNCL_SOR_CD like '11%' LEFT JOIN T89_ETL_VIEWS_ENT_XM.UM_RQST_PROV RP ON R.RFRNC_NBR = RP.RFRNC_NBR  AND R.CLNCL_SOR_CD = RP.CLNCL_SOR_CD LEFT JOIN T89_ETL_VIEWS_ENT_XM.UM_SRVC_PROV SP ON R.RFRNC_NBR = SP.RFRNC_NBR AND S.SRVC_LINE_NBR = SP.SRVC_LINE_NBR  AND R.CLNCL_SOR_CD = SP.CLNCL_SOR_CD  AND S.CLNCL_SOR_CD = SP.CLNCL_SOR_CD LEFT JOIN T89_ETL_VIEWS_ENT_XM.UM_SRVC_STTS STTS ON R.RFRNC_NBR = STTS.RFRNC_NBR AND S.SRVC_LINE_NBR = STTS.SRVC_LINE_NBR AND R.CLNCL_SOR_CD = STTS.CLNCL_SOR_CD AND S.CLNCL_SOR_CD = STTS.CLNCL_SOR_CD WHERE R.MBRSHP_SOR_CD in ('808', '815') SAMPLE 100"""
    //var cogxSQL="""SELECT DISTINCT sum(1) over( rows unbounded preceding ) as  ROW_ID,   TRIM(R.RFRNC_NBR) AS  RFRNC_NBR, TRIM(S.SRVC_LINE_NBR) AS SRVC_LINE_NBR, TRIM(R.CLNCL_SOR_CD) AS CLNCL_SOR_CD, TRIM(R.MBRSHP_SOR_CD) AS MBRSHP_SOR_CD, TRIM(STTS.UM_SRVC_STTS_CD) AS UM_SRVC_STTS_CD, TRIM(STTS.SRC_UM_SRVC_STTS_CD) AS SRC_UM_SRVC_STTS_CD, TRIM(R.SRC_SBSCRBR_ID) AS SRC_SBSCRBR_ID, TRIM(R.SRC_MBR_CD) AS SRC_MBR_CD, TRIM(R.PRMRY_DIAG_CD) AS PRMRY_DIAG_CD, TRIM(S.RQSTD_PLACE_OF_SRVC_CD) AS RQSTD_PLACE_OF_SRVC_CD, TRIM(S.SRC_RQSTD_PLACE_OF_SRVC_CD) AS SRC_RQSTD_PLACE_OF_SRVC_CD, TRIM(S.AUTHRZD_PLACE_OF_SRVC_CD) AS AUTHRZD_PLACE_OF_SRVC_CD, TRIM(S.SRC_AUTHRZD_PLACE_OF_SRVC_CD) AS SRC_AUTHRZD_PLACE_OF_SRVC_CD, TRIM(S.RQSTD_SRVC_FROM_DT) AS RQSTD_SRVC_FROM_DT, TRIM(S.AUTHRZD_SRVC_FROM_DT) AS AUTHRZD_SRVC_FROM_DT, TRIM(S.RQSTD_SRVC_TO_DT) AS RQSTD_SRVC_TO_DT, TRIM(S.AUTHRZD_SRVC_TO_DT) AS AUTHRZD_SRVC_TO_DT, TRIM(S.RQSTD_PROC_SRVC_CD) AS RQSTD_PROC_SRVC_CD, TRIM(S.AUTHRZD_PROC_SRVC_CD) AS AUTHRZD_PROC_SRVC_CD, TRIM(S.RQSTD_QTY) AS RQSTD_QTY, TRIM(S.AUTHRZD_QTY) AS AUTHRZD_QTY, TRIM(SP.SRC_UM_PROV_ID) AS SRC_UM_PROV_ID, TRIM(SP.PROV_ID) AS PROV_ID, TRIM(RP.SRC_UM_PROV_ID) AS SRC_UM_PROV_ID_RP, TRIM(RP.PROV_ID) AS PROV_ID_RP, TRIM(RP.SRC_PROV_FRST_NM) AS SRC_PROV_FRST_NM_RP, TRIM(RP.SRC_PROV_LAST_NM) AS SRC_PROV_LAST_NM_RP, TRIM(SP.SRC_PROV_FRST_NM) AS SRC_PROV_FRST_NM, TRIM(SP.SRC_PROV_LAST_NM) AS SRC_PROV_LAST_NM FROM T89_ETL_VIEWS_ENT_XM.UM_RQST R INNER JOIN T89_ETL_VIEWS_ENT_XM.UM_SRVC S ON  R.RFRNC_NBR = S.RFRNC_NBR   AND R.CLNCL_SOR_CD = S.CLNCL_SOR_CD and R.CLNCL_SOR_CD like '11%' LEFT JOIN T89_ETL_VIEWS_ENT_XM.UM_RQST_PROV RP ON R.RFRNC_NBR = RP.RFRNC_NBR  AND R.CLNCL_SOR_CD = RP.CLNCL_SOR_CD LEFT JOIN T89_ETL_VIEWS_ENT_XM.UM_SRVC_PROV SP ON R.RFRNC_NBR = SP.RFRNC_NBR AND S.SRVC_LINE_NBR = SP.SRVC_LINE_NBR  AND R.CLNCL_SOR_CD = SP.CLNCL_SOR_CD  AND S.CLNCL_SOR_CD = SP.CLNCL_SOR_CD LEFT JOIN T89_ETL_VIEWS_ENT_XM.UM_SRVC_STTS STTS ON R.RFRNC_NBR = STTS.RFRNC_NBR AND S.SRVC_LINE_NBR = STTS.SRVC_LINE_NBR AND R.CLNCL_SOR_CD = STTS.CLNCL_SOR_CD AND S.CLNCL_SOR_CD = STTS.CLNCL_SOR_CD WHERE R.MBRSHP_SOR_CD in ('808', '815','886', '823') and LENGTH(TRIM(R.SRC_SBSCRBR_ID))=9 and  R.RCRD_STTS_CD<> 'DUP' GROUP BY r.RFRNC_NBR, s.SRVC_LINE_NBR, r.CLNCL_SOR_CD, r.MBRSHP_SOR_CD, stts.UM_SRVC_STTS_CD, stts.SRC_UM_SRVC_STTS_CD, r.src_sbscrbr_id, r.src_mbr_cd, r.prmry_diag_cd, s.RQSTD_PLACE_OF_SRVC_CD, s.SRC_RQSTD_PLACE_OF_SRVC_CD, s.AUTHRZD_PLACE_OF_SRVC_CD, s.SRC_AUTHRZD_PLACE_OF_SRVC_CD, s.RQSTD_SRVC_FROM_DT, s.AUTHRZD_SRVC_FROM_DT, s.RQSTD_SRVC_TO_DT, s.AUTHRZD_SRVC_TO_DT, s.RQSTD_PROC_SRVC_CD, s.AUTHRZD_PROC_SRVC_CD, s.RQSTD_QTY, s.AUTHRZD_QTY, sp.src_um_prov_id, sp.prov_id, rp.src_um_prov_id, rp.prov_id, rp.SRC_PROV_FRST_NM, rp.SRC_PROV_LAST_NM, sp.SRC_PROV_FRST_NM, sp.SRC_PROV_LAST_NM"""  
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

     // println(cred)
     
      val dboptions = scala.collection.mutable.Map[String, String]()
      dboptions += ("url" -> config.getString("dbserverurl"))
      dboptions += ("user" -> config.getString("username"))
      dboptions += ("password" -> cred)
      dboptions += ("driver" -> config.getString("jdbcdriver"))
      dboptions += ("dbtable" -> dbTable)
      dboptions += ("fetchSize" -> String.valueOf(config.getString("fetch_size")))
      

      // Print out the Map for dboptions
    //  dboptions.map(x=>{println(x._1 + " ; " + x._2)})
      
    
    var cogxUMDF = spark.read.format("jdbc").options(dboptions).load()
    println("INFO: COGX SQL => " + cogxSQL)
    
    
    var dataMap = Map(teradata_table_name->cogxUMDF)
    
    return dataMap
    
  }
  
    def processData(inDFs: Map[String, DataFrame]): Map[String, DataFrame] = {
    val df0 = inDFs.getOrElse(teradata_table_name, null)

    val   df1 = df0.columns.foldLeft(df0) { (df, colName) =>
          df.schema(colName).dataType match {
            case StringType => { println(s"%%%%%%% TRIMMING COLUMN ${colName.toLowerCase()} %%%%%% VALUE '${trim(col(colName))}'"); df.withColumn(colName.toLowerCase, trim(col(colName))); }
            case _ => { println("Column " + colName.toLowerCase() + " is not being trimmed"); df.withColumn(colName.toLowerCase, col(colName)); }
          }
        }
    

    rowCount = df1.count()
    println("INFO: CogX Row Count => " + rowCount)
    
    
    
    val CogxUmHbaseDataSet = df1.as[CogxUMRecord]
    
    var groupCogxUmHbaseDataSet = CogxUmHbaseDataSet.groupByKey { key =>
        ((key.src_sbscrbr_id))
      }
    
    
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
    
     for (um <- cogxUmInfo) {
          cogxUmSet += um.cogxUMdata
        }

     val cogxUM = new cogxUmHistory(cogxUmSet.toArray)
     

        (String.valueOf(k), cogxUM) // getString(0) i.e. hbase_key is the column for the rowkey 
      }).repartition(2000)
    
  println("groupedUMHbaseDataSet1")
//groupedUMHbaseDataSet1.show(1000,false)
      
      
      val DF3 = groupedUMHbaseDataSet1.map{
        case (key, value) => {
          val digest = DigestUtils.md5Hex(String.valueOf(key))
          val rowKey = new StringBuilder(digest.substring(0, 8)).append(key).toString()
          val holder = asJSONString(value)
          val p = new Put(Bytes.toBytes(rowKey))
          ((rowKey),(holder))
        }
    }
    
   val df1WithAuditColumn =  DF3.toDF("rowKey", "jsonData")

   

    var dataMap = Map(teradata_table_name->df1WithAuditColumn)
    return dataMap
    
  }

  def writeData(outDFs: Map[String, DataFrame]): Unit = {

/*     var confIT = spark.sparkContext.hadoopConfiguration.iterator()
     println ("*********************************************************")
     while (confIT.hasNext())
     {
       var it = confIT.next()
       if (it.getKey.toString().contains("hbase"))
       {
       
       println("Key: "+ it.getKey())
       println("Value: " + it.getValue())
       }
     }
     println ("*********************************************************")*/
    
    val df1 = outDFs.getOrElse(teradata_table_name, null).toDF("rowKey", "jsonData")
    Thread.sleep(10000)
   // df1.persist(StorageLevel.MEMORY_AND_DISK).count
    //df1.show()
    
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
     //// Empty this when run on local
     ////////////////////////////////////
        getMapReduceJobConfiguration(hbaseTable)
      }
      else
      {
     /// Run it on Cluster now
       new PairRDDFunctions(putRDD).saveAsNewAPIHadoopDataset(getMapReduceJobConfiguration(hbaseTable))
      }


  }


  
  
}