API_ETG_GROUP_QUERY=select v.BKBN_SNAP_YEAR_MNTH_NBR,v.ST_CD,v.LOB_ID,v.PROD_ID,v.AGRGTN_ID,v.AGRGTN_TYPE_CD,v.SPCLTY_ID,v.SUBMRKT_CD,v.GRP_EPSD_VOL_NBR,v.GRP_VOL_WGTD_MEAN_ETG_INDX_NBR,v.GRP_90_PCT_LOWR_CL_NBR,v.GRP_90_PCT_UPR_CL_NBR,v.GRP_NRMLZD_VOL_WGTD_MEAN_ETG_INDX_NBR,v.GRP_NRMLZD_90_PCT_LOWR_CL_NBR,v.GRP_NRMLZD_90_PCT_UPR_CL_NBR  from(SELECT a.BKBN_SNAP_YEAR_MNTH_NBR , a.NTWK_ST_CD as ST_CD , case  when a.PCR_LOB_DESC = 'MEDICARE' THEN  'MEDICARE ADVANTAGE'   else a.PCR_LOB_DESC  end as LOB_ID              , ap.PROD_ID as PROD_ID, a.GRP_AGRGTN_ID as AGRGTN_ID, a.GRP_AGRGTN_TYPE_CD as AGRGTN_TYPE_CD , ap.SPCLTY_ID as SPCLTY_ID,  a.PRMRY_SUBMRKT_CD as SUBMRKT_CD, GRP_DSTNCT_EPSD_CNT as GRP_EPSD_VOL_NBR, GRP_SCRBL_AVG_EPSD_OE_VOL_WGTD_NBR as GRP_VOL_WGTD_MEAN_ETG_INDX_NBR, GRP_OE_LOWR_CNFDNC_LVL_VOL_WGTD_NBR as GRP_90_PCT_LOWR_CL_NBR,GRP_OE_UPR_CNFDNC_LVL_VOL_WGTD_NBR as GRP_90_PCT_UPR_CL_NBR  , GRP_SCRBL_AVG_EPSD_NRMLZD_OE_VOL_WGTD_NBR as GRP_NRMLZD_VOL_WGTD_MEAN_ETG_INDX_NBR,GRP_OE_LOWR_CNFDNC_LVL_NRMLZD_VOL_WGTD_NBR as GRP_NRMLZD_90_PCT_LOWR_CL_NBR, GRP_OE_UPR_CNFDNC_LVL_NRMLZD_VOL_WGTD_NBR as GRP_NRMLZD_90_PCT_UPR_CL_NBR  FROM	 PCR_GRP_EFCNCY_RATIO_MULTI_SPLT a inner join (select    b.ST_CD  ,  b.LOB_ID ,  b.PROD_ID ,  b.PROV_TAX_ID ,  b.SPCLTY_ID ,  b.SUBMRKT_CD ,  b.AGRGTN_TYPE_CD,  b. PEER_MRKT_CD from API_PROV_WRK b group by  b.ST_CD  ,  b.LOB_ID ,  b.PROD_ID ,  b.PROV_TAX_ID ,  b.SPCLTY_ID ,  b.SUBMRKT_CD ,  b.AGRGTN_TYPE_CD,  b. PEER_MRKT_CD) as AP on  a.GRP_AGRGTN_TYPE_CD = ap.AGRGTN_TYPE_CD and a.GRP_AGRGTN_ID = ap.PROV_TAX_ID and  a.NTWK_ST_CD = ap.ST_CD and a. PEER_MRKT_CD = ap. PEER_MRKT_CD  and ap.LOB_ID = (case when a.PCR_LOB_DESC = 'MEDICARE' THEN  'MEDICARE ADVANTAGE'                                else a.PCR_LOB_DESC end)and  a.PRMRY_SUBMRKT_CD =  ap.SUBMRKT_CD and ap.prod_id = (case when a.BNCHMRK_PROD_DESC = 'NO GROUPING' then 'ALL'                                else a.BNCHMRK_PROD_DESC end)join (select max( a.BKBN_SNAP_YEAR_MNTH_NBR) as BKBN_SNAP_YEAR_MNTH_NBR from PCR_ELGBL_PROV a  inner join(select  b.BKBN_SNAP_YEAR_MNTH_NBR as BKBN_SNAP_YEAR_MNTH_NBR from PCR_GRP_EFCNCY_RATIO_MULTI_SPLT b  group by BKBN_SNAP_YEAR_MNTH_NBR  ) as BB on BB.BKBN_SNAP_YEAR_MNTH_NBR = a.BKBN_SNAP_YEAR_MNTH_NBR  inner join (select  c.BKBN_SNAP_YEAR_MNTH_NBR as BKBN_SNAP_YEAR_MNTH_NBR from PCR_GRP_EFCNCY_RATIO c  group by BKBN_SNAP_YEAR_MNTH_NBR  ) as cc on cc.BKBN_SNAP_YEAR_MNTH_NBR = a.BKBN_SNAP_YEAR_MNTH_NBR  inner join (select  d.SNAP_YEAR_MNTH_NBR as SNAP_YEAR_MNTH_NBR from QLTY_NRW_NTWK_TAX_SPCLTY  d group by SNAP_YEAR_MNTH_NBR ) as dd on dd.SNAP_YEAR_MNTH_NBR = a.BKBN_SNAP_YEAR_MNTH_NBR) xy where a.BKBN_SNAP_YEAR_MNTH_NBR=xy.BKBN_SNAP_YEAR_MNTH_NBR and a.GRP_AGRGTN_TYPE_CD = 'TIN' and a.NTWK_ST_CD not in ('UNK' , 'NA' )and a.GRP_DSTNCT_EPSD_CNT >= 20 and a.PHYSN_TYPE_CD = 'All Physicians' and (                                                               (a.NTWK_ST_CD = 'CA' and a.PEER_MRKT_CD in  ('5','7','8') )or (a.NTWK_ST_CD in ('NY','VA') and  a.PEER_MRKT_CD in  ('7','8') )or (a.NTWK_ST_CD not in ( 'CA', 'NY', 'VA') and  a.PEER_MRKT_CD in  ('2' , '4' ) ))group by a.BKBN_SNAP_YEAR_MNTH_NBR,a.NTWK_ST_CD, case  when a.PCR_LOB_DESC = 'MEDICARE' THEN  'MEDICARE ADVANTAGE'   else a.PCR_LOB_DESC   end,ap.PROD_ID,a.GRP_AGRGTN_ID,a.GRP_AGRGTN_TYPE_CD,ap.SPCLTY_ID ,a.PRMRY_SUBMRKT_CD,GRP_DSTNCT_EPSD_CNT , GRP_SCRBL_AVG_EPSD_OE_VOL_WGTD_NBR , GRP_OE_LOWR_CNFDNC_LVL_VOL_WGTD_NBR , GRP_OE_UPR_CNFDNC_LVL_VOL_WGTD_NBR   , GRP_SCRBL_AVG_EPSD_NRMLZD_OE_VOL_WGTD_NBR , GRP_OE_LOWR_CNFDNC_LVL_NRMLZD_VOL_WGTD_NBR , GRP_OE_UPR_CNFDNC_LVL_NRMLZD_VOL_WGTD_NBR union all SELECT a.BKBN_SNAP_YEAR_MNTH_NBR, a.NTWK_ST_CD as ST_CD, case  when a.PCR_LOB_DESC = 'MEDICARE' THEN  'MEDICARE ADVANTAGE'   else a.PCR_LOB_DESC  end as LOB_ID              , ap.PROD_ID as PROD_ID , a.GRP_AGRGTN_ID as AGRGTN_ID, a.GRP_AGRGTN_TYPE_CD as AGRGTN_TYPE_CD, ap.SPCLTY_ID as SPCLTY_ID, a.PRMRY_SUBMRKT_CD as SUBMRKT_CD, GRP_DSTNCT_EPSD_CNT as GRP_EPSD_VOL_NBR, GRP_SCRBL_AVG_EPSD_OE_VOL_WGTD_NBR as GRP_VOL_WGTD_MEAN_ETG_INDX_NBR, GRP_OE_LOWR_CNFDNC_LVL_VOL_WGTD_NBR as GRP_90_PCT_LOWR_CL_NBR, GRP_OE_UPR_CNFDNC_LVL_VOL_WGTD_NBR as GRP_90_PCT_UPR_CL_NBR  , GRP_SCRBL_AVG_EPSD_NRMLZD_OE_VOL_WGTD_NBR as GRP_NRMLZD_VOL_WGTD_MEAN_ETG_INDX_NBR,GRP_OE_LOWR_CNFDNC_LVL_NRMLZD_VOL_WGTD_NBR as GRP_NRMLZD_90_PCT_LOWR_CL_NBR, GRP_OE_UPR_CNFDNC_LVL_NRMLZD_VOL_WGTD_NBR as GRP_NRMLZD_90_PCT_UPR_CL_NBR  FROM	 PCR_GRP_EFCNCY_RATIO_MULTI_SPLT a inner join (select    b.ST_CD  ,  b.LOB_ID ,  b.PROD_ID ,  b.NPI ,  b.SPCLTY_ID ,  b.SUBMRKT_CD ,  b.AGRGTN_TYPE_CD,   b.PEER_MRKT_CD from API_PROV_WRK b group by b.ST_CD  ,  b.LOB_ID ,  b.PROD_ID ,  b.NPI ,  b.SPCLTY_ID ,  b.SUBMRKT_CD ,  b.AGRGTN_TYPE_CD,   b.PEER_MRKT_CD) as AP on a.GRP_AGRGTN_ID = ap.NPI and a.GRP_AGRGTN_TYPE_CD = ap.AGRGTN_TYPE_CD and  a.NTWK_ST_CD = ap.ST_CD and a. PEER_MRKT_CD = ap. PEER_MRKT_CD and ap.LOB_ID = (case when a.PCR_LOB_DESC = 'MEDICARE' THEN  'MEDICARE ADVANTAGE'                                else a.PCR_LOB_DESC end)and  a.PRMRY_SUBMRKT_CD =  ap.SUBMRKT_CD and ap.prod_id = (case when a.BNCHMRK_PROD_DESC = 'NO GROUPING' then  'ALL'                                else a.BNCHMRK_PROD_DESC end)join (select max( a.BKBN_SNAP_YEAR_MNTH_NBR) as BKBN_SNAP_YEAR_MNTH_NBR from PCR_ELGBL_PROV a  inner join(select  b.BKBN_SNAP_YEAR_MNTH_NBR as BKBN_SNAP_YEAR_MNTH_NBR from PCR_GRP_EFCNCY_RATIO_MULTI_SPLT b  group by BKBN_SNAP_YEAR_MNTH_NBR  ) as BB on BB.BKBN_SNAP_YEAR_MNTH_NBR = a.BKBN_SNAP_YEAR_MNTH_NBR  inner join (select  c.BKBN_SNAP_YEAR_MNTH_NBR as BKBN_SNAP_YEAR_MNTH_NBR from PCR_GRP_EFCNCY_RATIO c  group by BKBN_SNAP_YEAR_MNTH_NBR  ) as cc on cc.BKBN_SNAP_YEAR_MNTH_NBR = a.BKBN_SNAP_YEAR_MNTH_NBR  inner join (select  d.SNAP_YEAR_MNTH_NBR as SNAP_YEAR_MNTH_NBR from QLTY_NRW_NTWK_TAX_SPCLTY  d group by SNAP_YEAR_MNTH_NBR ) as dd on dd.SNAP_YEAR_MNTH_NBR = a.BKBN_SNAP_YEAR_MNTH_NBR) xy where a.BKBN_SNAP_YEAR_MNTH_NBR =xy.BKBN_SNAP_YEAR_MNTH_NBR and a.GRP_AGRGTN_TYPE_CD = 'NPI' and a.GRP_DSTNCT_EPSD_CNT >= 20 and a.PHYSN_TYPE_CD = 'All Physicians'  and a.NTWK_ST_CD = 'NY' and a.PEER_MRKT_CD in  ('1' , '3' )group by a.BKBN_SNAP_YEAR_MNTH_NBR , a.NTWK_ST_CD, case  when a.PCR_LOB_DESC = 'MEDICARE' THEN  'MEDICARE ADVANTAGE'   else a.PCR_LOB_DESC  end              , ap.PROD_ID , a.GRP_AGRGTN_ID , a.GRP_AGRGTN_TYPE_CD , ap.SPCLTY_ID , a.PRMRY_SUBMRKT_CD ,GRP_DSTNCT_EPSD_CNT , GRP_SCRBL_AVG_EPSD_OE_VOL_WGTD_NBR , GRP_OE_LOWR_CNFDNC_LVL_VOL_WGTD_NBR , GRP_OE_UPR_CNFDNC_LVL_VOL_WGTD_NBR  , GRP_SCRBL_AVG_EPSD_NRMLZD_OE_VOL_WGTD_NBR, GRP_OE_LOWR_CNFDNC_LVL_NRMLZD_VOL_WGTD_NBR, GRP_OE_UPR_CNFDNC_LVL_NRMLZD_VOL_WGTD_NBR)v group by v.BKBN_SNAP_YEAR_MNTH_NBR,v.ST_CD,v.LOB_ID,v.PROD_ID,v.AGRGTN_ID,v.AGRGTN_TYPE_CD,v.SPCLTY_ID,v.SUBMRKT_CD,v.GRP_EPSD_VOL_NBR,v.GRP_VOL_WGTD_MEAN_ETG_INDX_NBR,v.GRP_90_PCT_LOWR_CL_NBR,v.GRP_90_PCT_UPR_CL_NBR,v.GRP_NRMLZD_VOL_WGTD_MEAN_ETG_INDX_NBR,v.GRP_NRMLZD_90_PCT_LOWR_CL_NBR,v.GRP_NRMLZD_90_PCT_UPR_CL_NBR
API_ETG_GROUP_INSERTQUERY=select bkbn_snap_year_mnth_nbr,st_cd,lob_id,prod_id,agrgtn_id,agrgtn_type_cd,spclty_id,submrkt_cd,grp_epsd_vol_nbr,grp_vol_wgtd_mean_etg_indx_nbr,grp_90_pct_lowr_cl_nbr,grp_90_pct_upr_cl_nbr,grp_nrmlzd_vol_wgtd_mean_etg_indx_nbr,grp_nrmlzd_90_pct_lowr_cl_nbr,grp_nrmlzd_90_pct_upr_cl_nbr,run_id from API_ETG_GROUP