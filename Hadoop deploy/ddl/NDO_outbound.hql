USE pr_pdppndoph_gbd_r000_ou;

DROP TABLE IF EXISTS ndo_phrmcy_cost; 
CREATE TABLE IF NOT EXISTS ndo_phrmcy_cost(
State char(5),
LOB varchar(100), 
Prod_ID varchar(100), 
bsns_sgmnt varchar(100), 
allwd_amt decimal(18,2),
rcrd_creatn_dtm timestamp, 
rcrd_creatn_user_id varchar(50), 
last_updt_dtm timestamp, 
last_updt_user_id varchar(50), 
run_id bigint)
STORED as PARQUET;


DROP TABLE IF EXISTS ndo_phrmcy_cost_bkp;
CREATE TABLE IF NOT EXISTS ndo_phrmcy_cost_bkp(
State char(5),
LOB varchar(100), 
Prod_ID varchar(100), 
bsns_sgmnt varchar(100), 
allwd_amt decimal(18,2),
rcrd_creatn_dtm timestamp, 
rcrd_creatn_user_id varchar(50), 
last_updt_dtm timestamp, 
last_updt_user_id varchar(50))
PARTITIONED BY(run_id bigint)
STORED as PARQUET;



DROP TABLE IF EXISTS api_peg_group;
CREATE TABLE IF NOT EXISTS api_peg_group(
bkbn_snap_year_mnth_nbr int,
st_cd char(3),
lob_id varchar(100),
prod_id varchar(100),
agrgtn_id varchar(100),
agrgtn_type_cd varchar(10),
spclty_id varchar(100),
submrkt_cd char(10),
prov_cnty_nm varchar(100),
rptg_ntwk_desc varchar(100),
grp_epsd_vol_nbr int,
grp_vol_wgtd_mean_etg_indx_nbr decimal(18,8),
grp_90_pct_lowr_cl_nbr decimal(18,8),
grp_90_pct_upr_cl_nbr decimal(18,8),
grp_nrmlzd_vol_wgtd_mean_etg_indx_nbr decimal(18,8),
grp_nrmlzd_90_pct_lowr_cl_nbr decimal(18,8),
grp_nrmlzd_90_pct_upr_cl_nbr decimal(18,8),
rcrd_creatn_dtm timestamp,
rcrd_creatn_user_id varchar(50),
last_updt_dtm timestamp,
last_updt_user_id varchar(50),
run_id int)
STORED as PARQUET;


DROP TABLE IF EXISTS api_peg_group_bkp;
CREATE TABLE IF NOT EXISTS api_peg_group_bkp(
bkbn_snap_year_mnth_nbr int,
st_cd char(3),
lob_id varchar(100),
prod_id varchar(100),
agrgtn_id varchar(100),
agrgtn_type_cd varchar(10),
spclty_id varchar(100),
submrkt_cd char(10),
prov_cnty_nm varchar(100),
rptg_ntwk_desc varchar(100),
grp_epsd_vol_nbr int,
grp_vol_wgtd_mean_etg_indx_nbr decimal(18,8),
grp_90_pct_lowr_cl_nbr decimal(18,8),
grp_90_pct_upr_cl_nbr decimal(18,8),
grp_nrmlzd_vol_wgtd_mean_etg_indx_nbr decimal(18,8),
grp_nrmlzd_90_pct_lowr_cl_nbr decimal(18,8),
grp_nrmlzd_90_pct_upr_cl_nbr decimal(18,8),
rcrd_creatn_dtm timestamp,
rcrd_creatn_user_id varchar(50),
last_updt_dtm timestamp,
last_updt_user_id varchar(50))
PARTITIONED BY (
run_id int)
STORED as PARQUET;


DROP TABLE IF EXISTS api_peg_group_spclty;
CREATE TABLE IF NOT EXISTS api_peg_group_spclty(
bkbn_snap_year_mnth_nbr int,
st_cd char(3),
lob_id varchar(100),
prod_id varchar(100),
agrgtn_id varchar(100),
agrgtn_type_cd varchar(10),
spclty_id varchar(100),
submrkt_cd char(10),
prov_cnty_nm varchar(100),
rptg_ntwk_desc varchar(100),
grp_epsd_vol_nbr int,
grp_vol_wgtd_mean_etg_indx_nbr decimal(18,8),
grp_90_pct_lowr_cl_nbr decimal(18,8),
grp_90_pct_upr_cl_nbr decimal(18,8),
grp_nrmlzd_vol_wgtd_mean_etg_indx_nbr decimal(18,8),
grp_nrmlzd_90_pct_lowr_cl_nbr decimal(18,8),
grp_nrmlzd_90_pct_upr_cl_nbr decimal(18,8),
rcrd_creatn_dtm timestamp,
rcrd_creatn_user_id varchar(50),
last_updt_dtm timestamp,
last_updt_user_id varchar(50),
run_id int)
STORED as PARQUET;


DROP TABLE IF EXISTS api_peg_group_spclty_bkp;
CREATE TABLE IF NOT EXISTS api_peg_group_spclty_bkp(
bkbn_snap_year_mnth_nbr int,
st_cd char(3),
lob_id varchar(100),
prod_id varchar(100),
agrgtn_id varchar(100),
agrgtn_type_cd varchar(10),
spclty_id varchar(100),
submrkt_cd char(10),
prov_cnty_nm varchar(100),
rptg_ntwk_desc varchar(100),
grp_epsd_vol_nbr int,
grp_vol_wgtd_mean_etg_indx_nbr decimal(18,8),
grp_90_pct_lowr_cl_nbr decimal(18,8),
grp_90_pct_upr_cl_nbr decimal(18,8),
grp_nrmlzd_vol_wgtd_mean_etg_indx_nbr decimal(18,8),
grp_nrmlzd_90_pct_lowr_cl_nbr decimal(18,8),
grp_nrmlzd_90_pct_upr_cl_nbr decimal(18,8),
rcrd_creatn_dtm timestamp,
rcrd_creatn_user_id varchar(50),
last_updt_dtm timestamp,
last_updt_user_id varchar(50))
PARTITIONED BY (
run_id int)
STORED as PARQUET;


DROP TABLE IF EXISTS api_peg_epsd_cost;
CREATE TABLE IF NOT EXISTS api_peg_epsd_cost(
bkbn_snap_year_mnth_nbr int,
ntwk_st_cd char(3),
pcr_lob_desc varchar(100),
prod_id varchar(100),
spclty_prmry_id varchar(100),
bsns_sgmnt_nm varchar(100),
mbr_ratg_area_desc varchar(60),
grp_agrgtn_id varchar(100),
grp_agrgtn_type_cd char(10),
etg_base_cls_cd char(6),
etg_base_cls_desc varchar(100),
pdl_svrty_lvl_cd char(10),
scrbl_etg_ind_cd char(10),
prov_ratng_area varchar(60),
rptg_ntwk_desc varchar(100),
epsd_totls_anlzd_alwd_amt decimal(18,2),
peer_grp_expctd_anlzd_alwd_amt decimal(18,2),
epsd_no_fi_totls_anlzd_alwd_amt decimal(18,2),
peer_no_fi_grp_expctd_anlzd_amt decimal(18,2),
totl_epsd_cnt int,
rcrd_creatn_dtm timestamp,
rcrd_creatn_user_id varchar(50),
last_updt_dtm timestamp,
last_updt_user_id varchar(50),
run_id int)
STORED as PARQUET;


DROP TABLE IF EXISTS api_peg_epsd_cost_bkp;
CREATE TABLE IF NOT EXISTS api_peg_epsd_cost_bkp(
bkbn_snap_year_mnth_nbr int,
ntwk_st_cd char(3),
pcr_lob_desc varchar(100),
prod_id varchar(100),
spclty_prmry_id varchar(100),
bsns_sgmnt_nm varchar(100),
mbr_ratg_area_desc varchar(60),
grp_agrgtn_id varchar(100),
grp_agrgtn_type_cd char(10),
etg_base_cls_cd char(6),
etg_base_cls_desc varchar(100),
pdl_svrty_lvl_cd char(10),
scrbl_etg_ind_cd char(10),
prov_ratng_area varchar(60),
rptg_ntwk_desc varchar(100),
epsd_totls_anlzd_alwd_amt decimal(18,2),
peer_grp_expctd_anlzd_alwd_amt decimal(18,2),
epsd_no_fi_totls_anlzd_alwd_amt decimal(18,2),
peer_no_fi_grp_expctd_anlzd_amt decimal(18,2),
totl_epsd_cnt int,
rcrd_creatn_dtm timestamp,
rcrd_creatn_user_id varchar(50),
last_updt_dtm timestamp,
last_updt_user_id varchar(50))
PARTITIONED BY (
run_id int)
STORED as PARQUET;

insert into run_id_table values(0,'API_PEG_GROUP'),(0,'API_PEG_GROUP_SPCLTY'),(0,'API_PEG_EPSD_COST'),(0,'NDO_PHRMCY_COST');