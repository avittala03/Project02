BKBN_IP_QUERY=(select trim(c.IP_EP_ID ) as IP_EP_ID,trim(c.IP_SOR_CD  )as IP_SOR_CD,trim(c.IP_GNRTNL_SFX_CD ) as IP_GNRTNL_SFX_CD,trim(c.IP_LGL_FRST_NM  )as IP_LGL_FRST_NM,trim(c.IP_LGL_LAST_NM ) as IP_LGL_LAST_NM,trim(c.IP_LGL_MID_NM ) as IP_LGL_MID_NM,trim(c.IP_PRFRD_FRST_NM ) as IP_PRFRD_FRST_NM,trim(c.IP_PRFSNL_TTL_CD ) as IP_PRFSNL_TTL_CD,trim(c.IP_PRFSNL_TTL_DESC ) as IP_PRFSNL_TTL_DESC,trim(c.TXNMY_PRMRY_CD ) as TXNMY_PRMRY_CD,trim(c.TXNMY_PRMRY_DESC  )as TXNMY_PRMRY_DESC,trim(c.SPCLTY_PRMRY_CD  )as SPCLTY_PRMRY_CD,trim(c.SPCLTY_PRMRY_DESC ) as SPCLTY_PRMRY_DESC,trim(c.NPI ) as NPI,trim(c.PRACTNR_TYPE_CD ) as PRACTNR_TYPE_CD,trim(c.ENCLRTY_PROV_ID ) as ENCLRTY_PROV_ID,trim(c.RHI_UNIQ_PROV_ID ) as RHI_UNIQ_PROV_ID,trim(c.SRC_SYS_SOR_CD ) as SRC_SYS_SOR_CD,trim(c.SCRTY_LVL_CD ) as SCRTY_LVL_CD,c.LOAD_LOG_KEY  as LOAD_LOG_KEY,trim(c.PARG_IND_CD ) as PARG_IND_CD,trim(c.IP_GNRTNL_SFX_DESC ) as IP_GNRTNL_SFX_DESC,trim(c.PRACTNR_TYPE_DESC ) as PRACTNR_TYPE_DESC,c.LOAD_LOG_KEY as  CDH_LOAD_LOG_KEY,current_timestamp as CDH_SOR_DTM ,0 as CDH_CRCTD_LOAD_LOG_KEY ,0 as CDH_UPDTD_LOAD_LOG_KEY,'ACT' as  CDH_RCRD_STTS_CD, c.SNAP_YEAR_MNTH_NBR  as SNAP_YEAR_MNTH_NBR  from BKBN_IP c INNER JOIN PADL_LOAD_LOG driver ON driver.LOAD_LOG_KEY = c.LOAD_LOG_KEY where cast(driver.load_strt_dtm as date)>
BKBN_IP_COUNT=(select count(1) as count1 from BKBN_IP c INNER JOIN PADL_LOAD_LOG driver  ON driver.LOAD_LOG_KEY = c.LOAD_LOG_KEY where cast(driver.load_strt_dtm as date)>