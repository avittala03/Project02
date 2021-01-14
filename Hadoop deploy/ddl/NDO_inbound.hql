USE pr_pdppndoph_gbd_r000_in;

create view IF NOT EXISTS pr_pdppndoph_gbd_r000_in.PHRMCY_CLM_LINE as select * from pr_cdledwdph_r000_wh_allphi.PHRMCY_CLM_LINE;

create view IF NOT EXISTS pr_pdppndoph_gbd_r000_in.PHRMCY_CLM as select * from pr_cdledwdph_r000_wh_allphi.PHRMCY_CLM;

create view IF NOT EXISTS pr_pdppndoph_gbd_r000_in.GL_CF_MBU_HRZNTL_HRCHY as select * from pr_cdledwdph_r000_wh_allphi.GL_CF_MBU_HRZNTL_HRCHY;

CREATE TABLE IF NOT EXISTS ndo_peg_spclty 
(SPCLTY_PRMRY_ID VARCHAR(100))
STORED AS PARQUET;

INSERT INTO ndo_peg_spclty VALUES('Urology'),('Orthopedic Surgery'),('Plastic & Recnstrctv Surgery'),('General Surgery'),('Colorectal Surgery'),('Surgical Oncology'),('Ophthalmology'),('Gastroenterology'),('Gynecological/Oncology'),('Thoracic Surgery'),('Neurosurgery'),('Podiatry'),('Interventional Cardiology'),('Cardiology'),('Cardiac Electrophysiology'),('Hand Surgery'),('Obstetrics Gynecology');

CREATE VIEW IF NOT EXISTS pr_pdppndoph_gbd_r000_in.pcr_sub_spclty_exprnc_view AS
select
 npi,
 tax_id,
 mcid,
 etg_run_id,
 srvc_dt,
 hlth_srvc_cd,
 hlth_srvc_type_cd,
 spcl_proc_ctgry_desc,
 src_sys_sor_cd,
 load_log_key,
 crctd_load_log_key,
 updtd_load_log_key,
 prov_spclty_prmry_cd,
 prov_spclty_prmry_desc,
 cdh_load_log_key,
 cdh_sor_dtm,
 cdh_crctd_load_log_key,
 cdh_updtd_load_log_key,
 cdh_rcrd_stts_cd
from pr_cdlpdlrph_r000_wh_allphi.PCR_SUB_SPCLTY_EXPRNC;

DROP TABLE IF EXISTS pr_pdppndoph_gbd_r000_in.pcr_sub_spclty_exprnc;
CREATE TABLE IF NOT EXISTS pr_pdppndoph_gbd_r000_in.pcr_sub_spclty_exprnc(
npi string,
tax_id string,
mcid bigint,
etg_run_id bigint,
srvc_dt timestamp,
hlth_srvc_cd string,
hlth_srvc_type_cd string,
spcl_proc_ctgry_desc string,
src_sys_sor_cd string,
load_log_key bigint,
crctd_load_log_key bigint,
updtd_load_log_key bigint,
prov_spclty_prmry_cd string,
prov_spclty_prmry_desc string)
stored as parquet;

create external table IF NOT EXISTS peg_ctgry_cd(
PEG_CTGRY_CD string,
CD_VAL_NM string,
CD_VAL_SHRT_DESC string,
CD_VAL_LONG_DESC string,
CD_ID string,
STTS_CD string,
STTS_DT string )
row format delimited fields terminated by '\t'
location '/pr/hdfsdata/ve2/pdp/pndo/phi/gbd/r000/inbound/peg_ctgry_cd';
