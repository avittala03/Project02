package com.am.cogx.etl.HiveBDFTestLocal

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
import com.am.cogx.etl.helper.CogxClaimRecord
import com.am.cogx.etl.helper.cogxClaimInfo
import com.am.cogx.etl.helper.cogxClaimHistory
import com.am.cogx.etl.helper.header
import com.am.cogx.etl.helper.detail
import com.am.cogx.etl.helper.headerHistory
import com.am.cogx.etl.helper.detailHistory
import collection.JavaConverters._
import java.io.PrintWriter
import java.io.StringWriter
import com.am.cogx.etl.util.CogxCommonUtils.asJSONString
import com.am.cogx.etl.util.CogxCommonUtils.Json_function
//import com.am.cogx.etl.util.CogxCommonUtils.getMapReduceJobConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import java.io.InputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.storage.StorageLevel
import collection.mutable

/**
  * Created by yuntliu on 1/20/2018.
  */


class TestBDFOperation (confFilePath: String, env: String, queryFileCategory: String) extends CogxOperationSession(confFilePath, env,queryFileCategory) with CogxOperator {

  sc.setLogLevel("info")
  val firstTableName="firstTableName"
  
  	import spark.sql
	import spark.implicits._
val repartitionNum = config.getInt("repartitionNum")
  def loadData(): Map[String, DataFrame] = {


    val path = "C:\\Users\\AG29035\\Documents\\bitbucket-code\\UM\\ds-cogx-etl-padp\\data\\bdf_data.csv"
    println("Path:" + path)
    var inputFieldsList:String =  "KEY_CHK_DCN_NBR,KEY_CHK_DCN_ITEM_CD,CLM_CMPLTN_DT,CLM_PAYMNT_ACTN_1_CD,CLM_PAYMNT_ACTN_2_6_CD,MEMBER_SSN,PAT_MBR_CD,GRP_NBR,SRVC_FROM_DT,SRVC_THRU_DT,PROV_TAX_ID,PROV_NM,PROV_SCNDRY_NM,PROV_SPCLTY_CD,PROV_LCNS_CD,TOTL_CHRG_AMT,TYPE_OF_BILL_CD,MDCL_RCRD_2_NBR,MRN_NBR,ICD_A_CD,ICD_B_CD,ICD_C_CD,ICD_D_CD,ICD_E_CD,PRVDR_STATUS,CLAIM_TYPE,DTL_LINE_NBR,ICD_9_1_CD,PROC_CD,TOS_TYPE_CD,PROC_SRVC_CLS_1_CD,PROC_SRVC_CLS_2_CD,PROC_SRVC_CLS_3_CD,HCPCS_CD,BILLD_CHRGD_AMT,BILLD_SRVC_UNIT_QTY,UNITS_OCR_NBR,PROC_MDFR_CD,HCPCS_MDFR_CD,MDFR_1_CD,MDFR_2_CD,MDFR_3_CD,HCFA_PT_CD,POT_CD,ELGBL_EXPNS_AMT,SRVC_FROM_DT_DTL,SRVC_TO_DT_DTL,LOAD_INGSTN_ID,LOAD_DTM,rank"
    var inputFieldsListArray:Array[String]=inputFieldsList.split(",")
    val testDF = spark
      .read.format( "com.databricks.spark.csv" )
      .option( "header", "true" )
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option( "inferSchema", "true" )
      .load( path )

    testDF.show(5)

    val newTestDF = testDF.toDF(inputFieldsListArray: _*)



    var dataMap = Map(firstTableName->newTestDF)

    return dataMap
  }


  def processData(inDFs: Map[String, DataFrame]): Map[String, DataFrame] = {
    val bdfCombineClaimsDF = inDFs.getOrElse(firstTableName, null)

    
    
    var CogaClaimDataset = bdfCombineClaimsDF.as[CogxClaimRecord]
    
    bdfCombineClaimsDF.as[CogxClaimRecord].createOrReplaceTempView("bdfCombineClaimsDF")
    
    val dates_logic = spark.sql("select CLM_CMPLTN_DT,cast(to_date(cast(unix_timestamp(cast(CLM_CMPLTN_DT as String),'yyyyMMdd') as timestamp)) as String) as CLM_CMPLTN_DT_NEW,SRVC_FROM_DT,cast(to_date(cast(unix_timestamp(cast(SRVC_FROM_DT as String),'yyyyMMdd') as timestamp)) as String) as SRVC_FROM_DT_NEW,SRVC_THRU_DT ,cast(to_date(cast(unix_timestamp(cast(SRVC_THRU_DT as String),'yyyyMMdd') as timestamp)) as String) as SRVC_THRU_DT_NEW,SRVC_FROM_DT_DTL,cast(to_date(cast(unix_timestamp(cast(SRVC_FROM_DT_DTL as String),'yyyyMMdd') as timestamp)) as String) as SRVC_FROM_DT_DTL_NEW,SRVC_TO_DT_DTL,cast(to_date(cast(unix_timestamp(cast(SRVC_TO_DT_DTL as String),'yyyyMMdd') as timestamp)) as String) as SRVC_TO_DT_DTL_NEW from bdfCombineClaimsDF")
    
    dates_logic.show(false)
    
    val ArraySizeLimit = config.getInt("ArraySizeLimit")
			    val reduce_function = (a: Set[CogxClaimRecord],b: Set[CogxClaimRecord])=>{
                                         if (a.size <=ArraySizeLimit)  
                                           {a ++ b}
                                         else{print("======== Oversized member (over size 3000):" + a.last.MEMBER_SSN);a}
                                         
			    }
			    
			     val reduce_function_new = (a: String,b: String)=>{
                                         if (a.size <=ArraySizeLimit)  
                                           {a ++ "," ++ b}
                                         else{print("======== Oversized member (over size 3000):" + a.last);a}
                                         }
			    
			    
   			  //Reduce by 7 columns   
          val RDDSet = CogaClaimDataset.rdd.repartition(repartitionNum).map(record => ((record.MEMBER_SSN, record.PAT_MBR_CD, record.SRVC_FROM_DT, record.SRVC_THRU_DT, record.KEY_CHK_DCN_NBR,record.KEY_CHK_DCN_ITEM_CD,record.CLM_CMPLTN_DT),Set(record)))
          .reduceByKey(reduce_function).
          repartition(repartitionNum)
           
          
         //Change the data structure of the RDD 
          val RDDSet1 = RDDSet.map(k=>((k._1._1,k._1._2,k._1._3,k._1._4),Json_function(k._1._5,k._1._6,k._1._7,k._2)))
    
          
          //Reduce by 4 columns 
          val heading = "{\"CogxClaim\": ["
          val ending = "]}"
          val RDDSet2 = RDDSet1.reduceByKey(reduce_function_new).repartition(repartitionNum).map(k=>(String.valueOf(k._1).replaceAll("\\(", "").replaceAll("\\)", "").replaceAll(",", ""), k._2 ))
          
          var DSSet = RDDSet2.map(k=>{( new StringBuilder((DigestUtils.md5Hex(String.valueOf(k._1))).substring(0, 8)).append(String.valueOf(k._1)).toString(),f"$heading" ++ k._2 ++ f"$ending")}).repartition(repartitionNum)
          
          var newDF= DSSet.toDF("rowKey", "jsonData").repartition(repartitionNum)
          
          var dataMap = Map(firstTableName->newDF)
          
          return dataMap

  }

  def writeData(outDFs: Map[String, DataFrame]): Unit = {

    val df1 = outDFs.getOrElse(firstTableName, null)
    Thread.sleep(10000)
    df1.show(false)
   // df1.coalesce(1).write.format( "com.databricks.spark.csv").save( "C:\\java\\git\\repos\\coe_2.0\\target\\test.csv")
  //  df1.coalesce(1).write.mode("overwrite").format( "com.databricks.spark.csv" ).option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
    //  .option( "header", "true" ).save("C:\\java\\git\\repos\\coe_2.0\\target\\Test.csv" )

  }



}
