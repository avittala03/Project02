package com.am.cogx.etl.util

import com.google.gson.GsonBuilder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectData
import org.apache.avro.reflect.ReflectDatumWriter
import java.io.ByteArrayOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableMapReduceUtil, TableOutputFormat}

import com.am.cogx.etl.helper.cogxClaimHistory
import com.am.cogx.etl.helper.header
import com.am.cogx.etl.helper.detail
import com.am.cogx.etl.helper.headerHistory
import com.am.cogx.etl.helper.detailHistory
import com.am.cogx.etl.helper.CogxClaimRecord

object CogxCommonUtils {

  def argsErrorMsg(): String = {
    """Rendering Spend Driver program needs exactly 3 arguments.
       | 1. Configuration file path
       | 2. Environment
       | 3. Query File Category""".stripMargin
  }
  
  
    def asJSONString(aType: java.lang.Object): String = {
    val gson = new GsonBuilder().serializeNulls().create();
    gson.toJson(aType);
  }
    
     def Json_function(a: String, b: String, c: String, d: Set[CogxClaimRecord]): String ={
			       
			       var detail_accumalated:String = ""
			       var header: String = ""
			       var count = 1
             
			       for(data <- d)
			       {
			         if (count == 1 )
			         {
			       var header_df = asJSONString(new header(data.KEY_CHK_DCN_NBR,data.KEY_CHK_DCN_ITEM_CD,data.CLM_CMPLTN_DT,data.CLM_PAYMNT_ACTN_1_CD,data.CLM_PAYMNT_ACTN_2_6_CD,data.MEMBER_SSN,data.PAT_MBR_CD,data.GRP_NBR,data.SRVC_FROM_DT,data.SRVC_THRU_DT,data.PROV_TAX_ID,data.PROV_NM,data.PROV_SCNDRY_NM,data.PROV_SPCLTY_CD,data.PROV_LCNS_CD,data.TOTL_CHRG_AMT,data.TYPE_OF_BILL_CD,data.MDCL_RCRD_2_NBR,data.MRN_NBR,data.ICD_A_CD,data.ICD_B_CD,data.ICD_C_CD,data.ICD_D_CD,data.ICD_E_CD,data.PRVDR_STATUS,data.CLAIM_TYPE,data.LOAD_INGSTN_ID))
			       header = header_df
			         }
			       var detail_df = asJSONString(new detail(data.KEY_CHK_DCN_NBR,data.KEY_CHK_DCN_ITEM_CD,data.CLM_CMPLTN_DT,data.DTL_LINE_NBR,data.ICD_9_1_CD,data.PROC_CD,data.TOS_TYPE_CD,data.PROC_SRVC_CLS_1_CD,data.PROC_SRVC_CLS_2_CD,data. PROC_SRVC_CLS_3_CD,data.HCPCS_CD,data. BILLD_CHRGD_AMT,data.BILLD_SRVC_UNIT_QTY,data.UNITS_OCR_NBR,data.PROC_MDFR_CD,data.HCPCS_MDFR_CD,data.MDFR_1_CD,data.MDFR_2_CD,data.MDFR_3_CD,data.HCFA_PT_CD,data.POT_CD,data.ELGBL_EXPNS_AMT,data.SRVC_FROM_DT_DTL,data.SRVC_TO_DT_DTL)) 
			       detail_accumalated= detail_accumalated  + detail_df + ","
			       
			       count+=1
			       }
			       detail_accumalated = detail_accumalated.dropRight(1)
			       var final_header = "\"header\": " + header + ","
			       var final_detail = "\"details\": [" + detail_accumalated + "]"
			       val full_data = "{" + final_header + final_detail + "}"
			       
			       return full_data 
			       
                                         }
    

}