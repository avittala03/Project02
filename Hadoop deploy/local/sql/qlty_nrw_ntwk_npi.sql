QLTY_NRW_NTWK_NPI_QUERY=(select trim(c.SNAP_YEAR_MNTH_NBR ) as SNAP_YEAR_MNTH_NBR,trim(c.PGM_NM ) as PGM_NM,trim(c.NPI ) as NPI,trim(c.CMS_SPCLTY_CD ) as CMS_SPCLTY_CD,trim(c.TOTL_MBR_CNT ) as TOTL_MBR_CNT,trim(c.CMPLYNT_MBR_CNT ) as CMPLYNT_MBR_CNT,trim(c.SCRBL_IND_CD ) as SCRBL_IND_CD,trim(c.CNFDNC_INTRVL_LOWR_NBR ) as CNFDNC_INTRVL_LOWR_NBR,trim(c.CNFDNC_INTRVL_UPR_NBR ) as CNFDNC_INTRVL_UPR_NBR,trim(c.CMPLNC_RT ) as CMPLNC_RT,trim(c.MSR_ID_CNT ) as MSR_ID_CNT,c.ENTRPR_CMPLNC_RT  as ENTRPR_CMPLNC_RT,c.OE_QLTY_RT  as OE_QLTY_RT,c.AGRGTN_TYPE_CD  as AGRGTN_TYPE_CD,c.PRD_END_DT as PRD_END_DT,c.PRD_STRT_DT as PRD_START_DT,c.LOAD_LOG_KEY as LOAD_LOG_KEY,c.LOAD_LOG_KEY as  CDH_LOAD_LOG_KEY,current_timestamp as CDH_SOR_DTM ,0 as CDH_CRCTD_LOAD_LOG_KEY ,0 as CDH_UPDTD_LOAD_LOG_KEY,'ACT' as  CDH_RCRD_STTS_CD from  QLTY_NRW_NTWK_NPI  c INNER JOIN PADL_LOAD_LOG driver ON driver.LOAD_LOG_KEY = c.LOAD_LOG_KEY where cast(driver.load_strt_dtm as date)>
QLTY_NRW_NTWK_NPI_COUNT=(select count(1) as count1 from  QLTY_NRW_NTWK_NPI  c INNER JOIN PADL_LOAD_LOG driver  ON driver.LOAD_LOG_KEY = c.LOAD_LOG_KEY where cast(driver.load_strt_dtm as date)>