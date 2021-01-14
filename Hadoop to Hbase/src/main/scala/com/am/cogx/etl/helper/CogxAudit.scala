package com.am.cogx.etl.helper

import org.apache.spark.sql.types.StructType

@SerialVersionUID(2017L)
case class CogxAudit(program: String, user_id: String, app_id: String, start_time: String, app_duration: String, status: String, loaded_row_count: Long )

case class CogxUMRecord( rfrnc_nbr: String, srvc_line_nbr: String, clncl_sor_cd: String, mbrshp_sor_cd: String, um_srvc_stts_cd: String, src_um_srvc_stts_cd: String, src_sbscrbr_id: String, src_mbr_cd: String, prmry_diag_cd: String, rqstd_place_of_srvc_cd: String, src_rqstd_place_of_srvc_cd: String, authrzd_place_of_srvc_cd: String, src_authrzd_place_of_srvc_cd: String, rqstd_srvc_from_dt: String, authrzd_srvc_from_dt: String, rqstd_srvc_to_dt: String, authrzd_srvc_to_dt: String, rqstd_proc_srvc_cd: String, authrzd_proc_srvc_cd: String, rqstd_qty: String, authrzd_qty: String, src_um_prov_id: String, prov_id: String, src_um_prov_id_rp: String, prov_id_rp: String, src_prov_frst_nm_rp: String, src_prov_last_nm_rp: String, src_prov_frst_nm: String, src_prov_last_nm: String)

final case class ArraySizeLimitExceededException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause) 
case class cogxUmInfo(cogxUMdata: CogxUMRecord)

case class cogxUmHistory(cogxUM: Array[CogxUMRecord])

case class cogxRecord( key: String,  cogxUM: Array[CogxUMRecord])

/*case class CogxClaimRecord(
  ddc_cd_dcn: String, ddc_cd_itm_cde: String, ddc_cd_clm_compl_dte: String, ddc_cd_clm_pay_act_1: String, ddc_cd_clm_pay_act_2_6: String, member_id: String,
  ddc_cd_pat_mbr_cde: String, ddc_cd_grp_nbr: String, ddc_cd_svc_from_dte: String, ddc_cd_svc_thru_dte: String, ddc_cd_prvdr_tax_id: String,
  ddc_cd_prvdr_nme: String, ddc_cd_prvdr_sec_nme: String, ddc_cd_prvdr_spclty_cde: String, prov_lcns_cd: String, ddc_cd_tot_chrg_amt: String,
  ddc_nat_ea2_type_of_bill: String, ddc_cd_med_rec_nbr_2: String, ddc_cd_med_rec_nbr: String, ddc_cd_icda_cde_1: String, ddc_cd_icda_cde_2: String,
  ddc_cd_icda_cde_3: String, ddc_cd_icda_cde_4: String, ddc_cd_icda_cde_5: String, prvdr_status: String, claim_type: String,
  ddc_dtl_lne_nbr: String, ddc_dtl_icda_pntr_1: String, ddc_dtl_prcdr_cde: String, ddc_dtl_svc_cde_1_3: String, ddc_dtl_proc_svc_cls_1: String,
  ddc_dtl_proc_svc_cls_2: String, ddc_dtl_proc_svc_cls_3: String, ddc_dtl_pcodec_hcpcs_cde: String, ddc_dtl_blld_amt: String, ddc_dtl_unts_occur: String,
  ddc_dtl_units_occur: String, ddc_dtl_prcdr_modfr_cde: String, ddc_dtl_pcodec_hcpcs_mod: String, ddc_dtl_mod_cde_1: String, ddc_dtl_mod_cde_2: String,
  ddc_dtl_mod_cde_3: String, ddc_dtl_hcfa_pt_cde: String, ddc_dtl_pt_cde: String, ddc_dtl_elig_expsn_amt: String, srvc_from_dt_dtl: String,
  srvc_to_dt_dtl: String, load_ingstn_id: String)*/
  
case class CogxClaimRecord(KEY_CHK_DCN_NBR: String,KEY_CHK_DCN_ITEM_CD: String,CLM_CMPLTN_DT: String,
CLM_PAYMNT_ACTN_1_CD: String,CLM_PAYMNT_ACTN_2_6_CD: String,MEMBER_SSN: String,PAT_MBR_CD: String,GRP_NBR: String,
SRVC_FROM_DT: String,SRVC_THRU_DT: String,PROV_TAX_ID: String,PROV_NM: String,PROV_SCNDRY_NM: String,PROV_SPCLTY_CD: String,
PROV_LCNS_CD: String,TOTL_CHRG_AMT: String,TYPE_OF_BILL_CD: String,MDCL_RCRD_2_NBR: String,MRN_NBR: String,ICD_A_CD: String,
ICD_B_CD: String,ICD_C_CD: String,ICD_D_CD: String,ICD_E_CD: String,PRVDR_STATUS: String,CLAIM_TYPE: String,
DTL_LINE_NBR: String,ICD_9_1_CD: String,PROC_CD: String,TOS_TYPE_CD: String,PROC_SRVC_CLS_1_CD: String,
PROC_SRVC_CLS_2_CD: String, PROC_SRVC_CLS_3_CD: String,HCPCS_CD: String, BILLD_CHRGD_AMT: String,BILLD_SRVC_UNIT_QTY: String,
UNITS_OCR_NBR: String,PROC_MDFR_CD: String,HCPCS_MDFR_CD: String,MDFR_1_CD: String,MDFR_2_CD: String,MDFR_3_CD: String,
HCFA_PT_CD: String,POT_CD: String,ELGBL_EXPNS_AMT: String,SRVC_FROM_DT_DTL: String,SRVC_TO_DT_DTL: String,LOAD_INGSTN_ID: String)  


case class header(KEY_CHK_DCN_NBR: String,KEY_CHK_DCN_ITEM_CD: String,CLM_CMPLTN_DT: String,
CLM_PAYMNT_ACTN_1_CD: String,CLM_PAYMNT_ACTN_2_6_CD: String,MEMBER_SSN: String,PAT_MBR_CD: String,GRP_NBR: String,
SRVC_FROM_DT: String,SRVC_THRU_DT: String,PROV_TAX_ID: String,PROV_NM: String,PROV_SCNDRY_NM: String,PROV_SPCLTY_CD: String,
PROV_LCNS_CD: String,TOTL_CHRG_AMT: String,TYPE_OF_BILL_CD: String,MDCL_RCRD_2_NBR: String,MRN_NBR: String,ICD_A_CD: String,
ICD_B_CD: String,ICD_C_CD: String,ICD_D_CD: String,ICD_E_CD: String,PRVDR_STATUS: String,CLAIM_TYPE: String,LOAD_INGSTN_ID: String)		  

case class detail(KEY_CHK_DCN_NBR: String,KEY_CHK_DCN_ITEM_CD: String,CLM_CMPLTN_DT: String,DTL_LINE_NBR: String,ICD_9_1_CD: String,PROC_CD: String,TOS_TYPE_CD: String,PROC_SRVC_CLS_1_CD: String,
PROC_SRVC_CLS_2_CD: String, PROC_SRVC_CLS_3_CD: String,HCPCS_CD: String, BILLD_CHRGD_AMT: String,BILLD_SRVC_UNIT_QTY: String,
UNITS_OCR_NBR: String,PROC_MDFR_CD: String,HCPCS_MDFR_CD: String,MDFR_1_CD: String,MDFR_2_CD: String,MDFR_3_CD: String,
HCFA_PT_CD: String,POT_CD: String,ELGBL_EXPNS_AMT: String,SRVC_FROM_DT_DTL: String,SRVC_TO_DT_DTL: String)


//case class CogxClaimRecordTest(Header:header,Detail:Array[detail])

case class CogxClaimRecordTest(KEY_CHK_DCN_NBR: String,KEY_CHK_DCN_ITEM_CD: String,CLM_CMPLTN_DT: String,
CLM_PAYMNT_ACTN_1_CD: String,CLM_PAYMNT_ACTN_2_6_CD: String,MEMBER_SSN: String,PAT_MBR_CD: String,GRP_NBR: String,
SRVC_FROM_DT: String,SRVC_THRU_DT: String,PROV_TAX_ID: String,PROV_NM: String,PROV_SCNDRY_NM: String,PROV_SPCLTY_CD: String,
PROV_LCNS_CD: String,TOTL_CHRG_AMT: String,TYPE_OF_BILL_CD: String,MDCL_RCRD_2_NBR: String,MRN_NBR: String,ICD_A_CD: String,
ICD_B_CD: String,ICD_C_CD: String,ICD_D_CD: String,ICD_E_CD: String,PRVDR_STATUS: String,CLAIM_TYPE: String,SRVC_FROM_DT_DTL: String,SRVC_TO_DT_DTL: String,LOAD_INGSTN_ID: String,
detail : StructType)

  
case class cogxClaimHistoryTest(cogxClaim: Array[CogxClaimRecordTest])  
  
case class cogxClaimInfo(cogxClaimdata: CogxClaimRecord)

case class cogxClaimHistory(cogxClaim: Array[CogxClaimRecord])

case class headerHistory(Header: Array[header])

case class detailHistory(Detail: Array[detail])

case class ehubWpidExtract(SOURCESYSTEMCODE: String, CONTRACT: String, START_DT: String, END_DT: String, REVISION_DT: String, MAJOR_HEADING: String, MINOR_HEADING: String, VARIABLE: String, VARIABLE_DESC: String, VARIABLE_FORMAT: String, VARIABLE_NETWORK: String, VARIABLE_VALUE: String, ACCUM_CODE : String,NOTES : String)

case class ehubWpidInfo(cogxbenefitdata: ehubWpidExtract)

case class ehubWpidHistory(cogxbenefit: Array[ehubWpidExtract])

case class ehubWpidRecord( key: String,  cogxUM: Array[ehubWpidExtract])

case class ehubWpidDelete(CONTRACT: String, START_DT: String, END_DT: String, STATUS: String, REVISION_DT: String)


