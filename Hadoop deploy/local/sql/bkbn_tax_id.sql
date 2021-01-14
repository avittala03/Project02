BKBN_TAX_ID_QUERY=(select c.SNAP_YEAR_MNTH_NBR  as SNAP_YEAR_MNTH_NBR,trim(c.EP_TAX_ID ) as EP_TAX_ID,trim(c.EP_TAX_ID_1099_NM ) as EP_TAX_ID_1099_NM,c.UNIQ_NPI_CNT as UNIQ_NPI_CNT,c.PRMRY_SPCLTY_CNT as PRMRY_SPCLTY_CNT,c.UNIQ_ST_CNT as UNIQ_ST_CNT,c.LOAD_LOG_KEY  as LOAD_LOG_KEY,c.UNIQ_CNTRCT_ST_CNT as UNIQ_CNTRCT_ST_CNT,c.LOAD_LOG_KEY as  CDH_LOAD_LOG_KEY,current_timestamp as CDH_SOR_DTM ,0 as CDH_CRCTD_LOAD_LOG_KEY ,0 as CDH_UPDTD_LOAD_LOG_KEY,'ACT' as  CDH_RCRD_STTS_CD from  BKBN_TAX_ID  c INNER JOIN PADL_LOAD_LOG driver ON driver.LOAD_LOG_KEY = c.LOAD_LOG_KEY where cast(driver.load_strt_dtm as date)>
BKBN_TAX_ID_COUNT=(select count(1) as count1 from  BKBN_TAX_ID  c INNER JOIN PADL_LOAD_LOG driver  ON driver.LOAD_LOG_KEY = c.LOAD_LOG_KEY where cast(driver.load_strt_dtm as date)>