package com.am.cogx.etl.HiveBDFHive


import java.io.File

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
import org.apache.spark.sql.SaveMode






class CogxHiveBDFHiveOperation (confFilePath: String, env: String, queryFileCategory: String) extends CogxOperationSession(confFilePath, env, queryFileCategory) with CogxOperator  {

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
       val histDate:String = CogxDateUtils.addMonthToDateWithFormat(CogxDateUtils.getCurrentDateWithFormat("yyyyMMdd"),"yyyyMMdd", -(backout_months))
	      println ("HistDate " + histDate)

	      
	     val isIncremental = config.getString("isIncremental")
   
       println ("isIncremental: "+ isIncremental )
    
	      
	      
			/* 1. Claim Header */
			var clmwgsgncclmpSQL =  config.getString("query_clm_wgs_gncclmp").replace("<<sourceDB>>", config.getString("stage-hive-db")).
			replace("<<Header_Additional_Filter>>",  config.getString("Header_Additional_Filter")).
			replace("<<histDate>>", histDate)
			
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
        println ("new clmwgsgncclmpSQL: " + clmwgsgncclmpSQL)
     }
    
			
			
			info("clmwgsgncclmpSQL =>: "+ clmwgsgncclmpSQL)
			val clmHeaderDF = spark.sql(clmwgsgncclmpSQL).repartition(repartitionNum)
      val clmHeaderDFTrim = clmHeaderDF.columns.foldLeft(clmHeaderDF){(memoDF,colName)=> memoDF.withColumn(colName, trim(col(colName)))}
			

			/* 2. Claim Detail */
			val clmwgsgncdtlpSQL =   config.getString("query_clm_wgs_gncdtlp").replace("<<sourceDB>>", config.getString("stage-hive-db")).replace("<<Detail_Additional_Filter>>",  config.getString("Detail_Additional_Filter"))
			val clmDetailDF = spark.sql(clmwgsgncdtlpSQL).repartition(repartitionNum)
			val  clmDetailDFTrim = clmDetailDF.columns.foldLeft(clmDetailDF){(memoDF,colName)=>memoDF.withColumn(colName, trim(col(colName)))}


			/* 3. EA2 */
			val clmwgsgncnatpea2SQL =   config.getString("query_clm_wgs_gncnatp_ea2").replace("<<sourceDB>>", config.getString("stage-hive-db")).replace("<<ea_Additional_Filter>>",  config.getString("ea_Additional_Filter"))
			val clmgncnatpea2DF = spark.sql(clmwgsgncnatpea2SQL).repartition(repartitionNum)
			val clmgncnatpea2DFTrim = clmgncnatpea2DF.columns.foldLeft(clmgncnatpea2DF){(memoDF,colName)=>memoDF.withColumn(colName, trim(col(colName)))}

			
			/*Creating a map of table name and respective dataframes */

			val dataMap = Map("clm_wgs_gncclmp" -> clmHeaderDFTrim,
					"clm_wgs_gncdtlp" -> clmDetailDFTrim,
					"clm_wgs_gncnatp_ea2" -> clmgncnatpea2DFTrim)


			return dataMap
	}


	def processData(inDFs: Map[String, DataFrame]): Map[String, DataFrame] = {
/*
			    val clmHeaderDFData = inDFs.getOrElse("clm_wgs_gncclmp", null)
					val clmDetailDFData = inDFs.getOrElse("clm_wgs_gncdtlp", null)
					val clmgncnatpea2DFData = inDFs.getOrElse("clm_wgs_gncnatp_ea2", null)
	  			    
			    val HeaderrowCount = clmHeaderDFData.count()
					info(s"[COGX]INFO: CogX Row Count clmHeaderDFData=> " + HeaderrowCount)
					
				  val DetailrowCount = clmDetailDFData.count()
					info(s"[COGX]INFO: CogX Row Count clmDetailDFData=> " + DetailrowCount)			
					
					val EA2rowCount = clmgncnatpea2DFData.count()
					info(s"[COGX]INFO: CogX Row Count clmgncnatpea2DFData=> " + EA2rowCount)			
						*/	
	  
			    val clmHeaderDF = inDFs.getOrElse("clm_wgs_gncclmp", null).createOrReplaceTempView("clmHeaderDF")
					val clmDetailDF = inDFs.getOrElse("clm_wgs_gncdtlp", null).createOrReplaceTempView("clmDetailDF")
					val clmgncnatpea2DF = inDFs.getOrElse("clm_wgs_gncnatp_ea2", null).createOrReplaceTempView("clmgncnatpea2DF")

					/*var bdfCombineClaimsDF = clmHeaderDF.as("header").join(clmDetailDF.as("detail"), $"header.ddc_cd_dcn" === $"gnchiios_hclm_dcn", "inner")
					.join(clmgncnatpea2DF.as("ea2"), $"header.ddc_cd_dcn" === $"ea2.gnchiios_hclm_dcn" and $"header.ddc_cd_dcn_cc" === $"ea2.gnchiios_hclm_dcn_cc", "inner")
	        .select("header.ddc_cd_dcn",
					    "header.ddc_cd_itm_cde", "header.ddc_cd_clm_compl_dte", "header.ddc_cd_clm_pay_act_1", "header.ddc_cd_clm_pay_act_2_6", "header.member_id",
							"header.ddc_cd_pat_mbr_cde", "header.ddc_cd_grp_nbr", "header.ddc_cd_svc_from_dte", "header.ddc_cd_svc_thru_dte", "header.ddc_cd_prvdr_tax_id",
							"header.ddc_cd_prvdr_nme", "header.ddc_cd_prvdr_sec_nme", "header.ddc_cd_prvdr_spclty_cde", "header.prov_lcns_cd", "header.ddc_cd_tot_chrg_amt",
							"ea2.ddc_nat_ea2_type_of_bill", "header.ddc_cd_med_rec_nbr_2", "header.ddc_cd_med_rec_nbr", "header.ddc_cd_icda_cde_1", "header.ddc_cd_icda_cde_2",
							"header.ddc_cd_icda_cde_3", "header.ddc_cd_icda_cde_4", "header.ddc_cd_icda_cde_5", "header.prvdr_status", "header.claim_type",
							"detail.ddc_dtl_lne_nbr", "detail.ddc_dtl_icda_pntr_1", "detail.ddc_dtl_prcdr_cde", "detail.ddc_dtl_svc_cde_1_3", "detail.ddc_dtl_proc_svc_cls_1",
							"detail.ddc_dtl_proc_svc_cls_2", "detail.ddc_dtl_proc_svc_cls_3", "detail.ddc_dtl_pcodec_hcpcs_cde", "detail.ddc_dtl_blld_amt", "detail.ddc_dtl_unts_occur",
							"detail.ddc_dtl_units_occur", "detail.ddc_dtl_prcdr_modfr_cde", "detail.ddc_dtl_pcodec_hcpcs_mod", "detail.ddc_dtl_mod_cde_1", "detail.ddc_dtl_mod_cde_2",
							"detail.ddc_dtl_mod_cde_3", "detail.ddc_dtl_hcfa_pt_cde", "detail.ddc_dtl_pt_cde", "detail.ddc_dtl_elig_expsn_amt", "detail.srvc_from_dt_dtl",
							"detail.srvc_to_dt_dtl", "header.load_ingstn_id", "header.load_dtm").repartition(repartitionNum)*/
         import org.apache.spark.sql.functions.broadcast
				 var bdfCombineClaimsDF = broadcast (spark.sql("""
				   select 
				   header.ddc_cd_dcn as KEY_CHK_DCN_NBR,
				   header.ddc_cd_itm_cde as KEY_CHK_DCN_ITEM_CD,
				   --header.ddc_cd_clm_compl_dte as CLM_CMPLTN_DT,
				   cast(to_date(cast(unix_timestamp(cast(header.ddc_cd_clm_compl_dte as String),'yyyyMMdd') as timestamp)) as String) as CLM_CMPLTN_DT,
				   header.ddc_cd_clm_pay_act_1 as CLM_PAYMNT_ACTN_1_CD,
				   header.ddc_cd_clm_pay_act_2_6 as CLM_PAYMNT_ACTN_2_6_CD,
				   header.member_id as MEMBER_SSN,
				   header.ddc_cd_pat_mbr_cde as PAT_MBR_CD,
				   header.ddc_cd_grp_nbr as GRP_NBR,
				   --header.ddc_cd_svc_from_dte as SRVC_FROM_DT,
				   cast(to_date(cast(unix_timestamp(cast(header.ddc_cd_svc_from_dte as String),'yyyyMMdd') as timestamp)) as String) as SRVC_FROM_DT,
				   --header.ddc_cd_svc_thru_dte as SRVC_THRU_DT,
				   cast(to_date(cast(unix_timestamp(cast(header.ddc_cd_svc_thru_dte as String),'yyyyMMdd') as timestamp)) as String) as SRVC_THRU_DT,
				   header.ddc_cd_prvdr_tax_id as PROV_TAX_ID,
				   header.ddc_cd_prvdr_nme as PROV_NM,
				   header.ddc_cd_prvdr_sec_nme as PROV_SCNDRY_NM,
				   header.ddc_cd_prvdr_spclty_cde as PROV_SPCLTY_CD,
				   header.prov_lcns_cd as PROV_LCNS_CD,
				   header.ddc_cd_tot_chrg_amt as TOTL_CHRG_AMT,
				   ea2.ddc_nat_ea2_type_of_bill as TYPE_OF_BILL_CD,
				   header.ddc_cd_med_rec_nbr_2 as MDCL_RCRD_2_NBR,
				   header.ddc_cd_med_rec_nbr as MRN_NBR,
				   header.ddc_cd_icda_cde_1 as ICD_A_CD,
				   header.ddc_cd_icda_cde_2 as ICD_B_CD,
				   header.ddc_cd_icda_cde_3 as ICD_C_CD,
				   header.ddc_cd_icda_cde_4 as ICD_D_CD,
				   header.ddc_cd_icda_cde_5 as ICD_E_CD,
				   header.prvdr_status as PRVDR_STATUS,
				   header.claim_type as CLAIM_TYPE,
				   detail.ddc_dtl_lne_nbr as DTL_LINE_NBR,
				   detail.ddc_dtl_icda_pntr_1 as ICD_9_1_CD,
				   detail.ddc_dtl_prcdr_cde as PROC_CD,
				   detail.ddc_dtl_svc_cde_1_3 as TOS_TYPE_CD,
				   detail.ddc_dtl_proc_svc_cls_1 as PROC_SRVC_CLS_1_CD,
				   detail.ddc_dtl_proc_svc_cls_2 as PROC_SRVC_CLS_2_CD,
				   detail.ddc_dtl_proc_svc_cls_3 as PROC_SRVC_CLS_3_CD,
				   detail.ddc_dtl_pcodec_hcpcs_cde as HCPCS_CD,
				   detail.ddc_dtl_blld_amt as BILLD_CHRGD_AMT,
				   detail.ddc_dtl_unts_occur as BILLD_SRVC_UNIT_QTY,
				   detail.ddc_dtl_units_occur as UNITS_OCR_NBR,
				   detail.ddc_dtl_prcdr_modfr_cde as PROC_MDFR_CD,
				   detail.ddc_dtl_pcodec_hcpcs_mod as HCPCS_MDFR_CD,
				   detail.ddc_dtl_mod_cde_1 as MDFR_1_CD,
				   detail.ddc_dtl_mod_cde_2 as MDFR_2_CD,
				   detail.ddc_dtl_mod_cde_3 as MDFR_3_CD,
				   detail.ddc_dtl_hcfa_pt_cde as HCFA_PT_CD,
				   detail.ddc_dtl_pt_cde as POT_CD,
				   detail.ddc_dtl_elig_expsn_amt as ELGBL_EXPNS_AMT,
				   --detail.srvc_from_dt_dtl as SRVC_FROM_DT_DTL,
				   cast(to_date(cast(unix_timestamp(cast(detail.srvc_from_dt_dtl as String),'yyyyMMdd') as timestamp)) as String) as SRVC_FROM_DT_DTL,
				   --detail.srvc_to_dt_dtl as SRVC_TO_DT_DTL,
				   cast(to_date(cast(unix_timestamp(cast(detail.srvc_to_dt_dtl as String),'yyyyMMdd') as timestamp)) as String) as SRVC_TO_DT_DTL,
				   header.load_ingstn_id as LOAD_INGSTN_ID,
				   header.load_dtm as LOAD_DTM
				   from clmHeaderDF header 
				   inner join 
				   clmDetailDF detail 
				   on header.ddc_cd_dcn = detail.gnchiios_hclm_dcn
				   inner join 
				   clmgncnatpea2DF ea2
				   on header.ddc_cd_dcn = ea2.gnchiios_hclm_dcn
				   and header.ddc_cd_dcn_cc = ea2.gnchiios_hclm_dcn_cc
				   """)).repartition(repartitionNum)
						
         							
					

					var bdfClaimsPartition = Window.partitionBy("KEY_CHK_DCN_NBR", "KEY_CHK_DCN_ITEM_CD", "DTL_LINE_NBR").orderBy(col("LOAD_DTM").desc)
					bdfCombineClaimsDF = bdfCombineClaimsDF.withColumn("rank", rank().over(bdfClaimsPartition)).filter("rank=1")

/*					////////////// Test write to single HDFS file = Need to be disabled all the time other than running with test///////////////////////
			    bdfCombineClaimsDF.persist(StorageLevel.MEMORY_AND_DISK)
			    //bdfCombineClaimsDF.write.format( "com.databricks.spark.csv").save( "/dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/", header="true")
					bdfCombineClaimsDF.coalesce(1).write.mode("overwrite").format( "com.databricks.spark.csv" ).option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option( "header", "true" ).save( "/dv/hdfsapp/ve2/ccp/cogx/phi/gbd/r000/control/af35352Test/BDFTestData.csv")
          ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
*/					
/*					rowCount = bdfCombineClaimsDF.count()
					info(s"[COGX]INFO: CogX Row Count bdfCombineClaimsDF=> " + rowCount)
*/
					var CogaClaimDataset = bdfCombineClaimsDF.as[CogxClaimRecord]

       
			    val ArraySizeLimit = config.getInt("ArraySizeLimit")
			    val reduce_function = (a: Set[CogxClaimRecord],b: Set[CogxClaimRecord])=>{
                                         if (a.size <=ArraySizeLimit)  
                                           {a ++ b}
                                         else{print("======== Oversized member (over size 3000):" + a.last.MEMBER_SSN);a}
                                         }
			    
			    /*
          val RDDSet = CogaClaimDataset.rdd.repartition(repartitionNum).map(record => ((record.MEMBER_SSN, record.PAT_MBR_CD, record.SRVC_FROM_DT, record.SRVC_THRU_DT),Set(record)))
          .reduceByKey(reduce_function).
          repartition(repartitionNum).map(k=>(String.valueOf(k._1).replaceAll("\\(", "").replaceAll("\\)", "").replaceAll(",", ""), k._2) )
          
          
          //println ("RDDset Count:" + RDDSet.count())
          //.replaceAll("\\(", "").replaceAll("\\)", "")
          var DSSet = RDDSet.map(k=>{( new StringBuilder((DigestUtils.md5Hex(String.valueOf(k._1))).substring(0, 8)).append(String.valueOf(k._1)).toString(),asJSONString(new cogxClaimHistory(k._2.toArray)))}).repartition(repartitionNum)
      
          var newDF= DSSet.toDF("rowKey", "jsonData").repartition(repartitionNum)
          
          var dataMap = Map(teradata_table_name->newDF)
          
          return dataMap
          * */
         
			    
			    
			    /* New Logic starting here */
			    
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
          val RDDSet1 = RDDSet.map(k=>((k._1._1,k._1._2,k._1._3,k._1._4),Json_function(k._1._5,k._1._6,k._1._7,k._2))).repartition(repartitionNum)
    
          
          //Reduce by 4 columns 
          val heading = "{\"CogxClaim\": ["
          val ending = "]}"
          val RDDSet2 = RDDSet1.reduceByKey(reduce_function_new).repartition(repartitionNum).map(k=>(String.valueOf(k._1).replaceAll("\\(", "").replaceAll("\\)", "").replaceAll(",", ""), k._2 ))
          
          var DSSet = RDDSet2.map(k=>{( new StringBuilder((DigestUtils.md5Hex(String.valueOf(k._1))).substring(0, 8)).append(String.valueOf(k._1)).toString(),f"$heading" ++ k._2 ++ f"$ending")}).repartition(repartitionNum)
          
          var newDF= DSSet.toDF("rowKey", "jsonData").repartition(repartitionNum)
          
          var dataMap = Map(teradata_table_name->newDF)
          
          return dataMap

	}


	def writeData(outDFs: Map[String, DataFrame]): Unit = {

			val df1 = outDFs.getOrElse(teradata_table_name, null).toDF("rowKey", "jsonData")

					df1.persist(StorageLevel.MEMORY_AND_DISK).count
					val hbaseCount = df1.count()
					info("Loading to HBase count: "+ hbaseCount)
					ABC_load_count=hbaseCount

					val truncateDB = spark.sql(config.getString("truncateSQL").replace("<<sourceDB>>", config.getString("warehouse-hive-db")))

				//	df1.write.mode(SaveMode.Overwrite).option("truncate", "true").insertInto(config.getString("NarrowStageDB").replace("<<sourceDB>>", config.getString("warehouse-hive-db")))
          df1.write.mode(SaveMode.Overwrite).option("truncate", "true").insertInto(config.getString("NarrowStageDB").replace("<<sourceDB>>", config.getString("warehouse-hive-db")))

	}
	
}