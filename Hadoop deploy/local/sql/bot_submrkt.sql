BOT_SUBMRKT_QUERY=(select trim(c.ADRS_POSTL_CD ) as ADRS_POSTL_CD,trim(c.SUBMRKT_CD ) as SUBMRKT_CD,trim(c.SUBMRKT_DESC ) as SUBMRKT_DESC,trim(c.SUBMRKT_ST_CD ) as SUBMRKT_ST_CD,c.CREATD_CLNDR_DT  as CREATD_CLNDR_DT,trim(c.CREATD_USER_ID ) as CREATD_USER_ID,c.LAST_UPDT_DT  as LAST_UPDT_DT,trim(c.LAST_UPDT_USER_ID ) as LAST_UPDT_USER_ID,0 as  CDH_LOAD_LOG_KEY,current_timestamp as CDH_SOR_DTM ,0 as CDH_CRCTD_LOAD_LOG_KEY ,0 as CDH_UPDTD_LOAD_LOG_KEY,'ACT' as  CDH_RCRD_STTS_CD from BOT_SUBMRKT c 
BOT_SUBMRKT_COUNT=(select count(1) as count1 from BOT_SUBMRKT c