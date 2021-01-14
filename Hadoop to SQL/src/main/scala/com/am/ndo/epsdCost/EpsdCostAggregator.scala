package com.am.ndo.epsdCost

import java.time.Period

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

import com.am.ndo.helper.NDOMonad

import grizzled.slf4j.Logging;
import org.apache.spark.sql.Dataset
import javassist.expr.Cast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

case class ndoWrkEpsdCost2(pcrCostEpsdDf: Dataset[Row], fnclProdCfDf: Dataset[Row], ndoZipSubmarketXwalkDf: Dataset[Row],
		pcrEtgMbrSmryDf: Dataset[Row], fnclMbuCfDf: Dataset[Row], etgBaseClsCdDf: Dataset[Row], mlsaZipCdDfFil: Dataset[Row], apiProvWrkDfFil: Dataset[Row],
		snapNbr: Long, ndowrkRatgAddrDf: Dataset[Row], pcrEtgSmryDf: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {

	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

//20190930 update - Change the etg_run_id to BKBN_SNAP_YEAR_MNTH_NBR
//and trim(col("pcrCostEpsdDf1.PEER_MRKT_CD")) filter change to single peer_mrkt_cd
			val pcrCostEpsdFil = pcrCostEpsdDf.filter(trim(pcrCostEpsdDf("BKBN_SNAP_YEAR_MNTH_NBR")) === snapNbr
			&& trim(pcrCostEpsdDf("GRP_AGRGTN_TYPE_CD")) === "TIN"
			&& trim(pcrCostEpsdDf("BNCHMRK_PROD_DESC")).isin("NP", "NA", "PAR") === false
			&& trim(pcrCostEpsdDf("SPCLTY_PRMRY_CD")).isin("UNK", "NA", "53", "17", "A0", "A5", "69", "32", "B4", "51", "87", "99", "43", "54", "88", "61") === false
			&& trim(pcrCostEpsdDf("PCR_LOB_DESC")).isin("NP") === false
			&& trim(pcrCostEpsdDf("NTWK_ST_CD")).isin("CA", "CO", "CT", "FL", "GA", "IA", "IN", "KS", "KY", "LA", "MD", "ME", "MO", "NH", "NJ", "NM", "NV", "NY", "OH", "SC", "TN", "TX", "VA", "WA", "WI", "WV")
			&& (trim(pcrCostEpsdDf("NTWK_ST_CD")).isin("CA","NY", "VA") && trim(pcrCostEpsdDf("PEER_MRKT_CD")).isin("8")
					|| trim(pcrCostEpsdDf("NTWK_ST_CD")).isin("CA", "NY", "VA") === false && trim(pcrCostEpsdDf("PEER_MRKT_CD")).isin("4")))
					
			val pcrCostEpsdJoin = pcrCostEpsdFil.join(apiProvWrkDfFil, trim(apiProvWrkDfFil("PROV_TAX_ID")) === trim(pcrCostEpsdDf("GRP_AGRGTN_ID")), "inner")
			
			val pcrJoinApi = pcrCostEpsdJoin.join(pcrEtgMbrSmryDf, trim(pcrEtgMbrSmryDf("mcid")) === trim(pcrCostEpsdDf("mcid"))
			&& trim(pcrEtgMbrSmryDf("ETG_RUN_ID")) === trim(pcrCostEpsdDf("ETG_RUN_ID")), "inner")
			.join(fnclMbuCfDf, trim(fnclMbuCfDf("MBU_CF_CD")) === trim(pcrEtgMbrSmryDf("MBU_CF_CD")), "inner")
			.join(fnclProdCfDf, trim(fnclProdCfDf("PROD_CF_CD")) === trim(pcrEtgMbrSmryDf("PROD_CF_CD")), "inner")
			
			val pcrApiJoinEtg = pcrJoinApi.join(etgBaseClsCdDf, trim(etgBaseClsCdDf("ETG_BASE_CLS_CD")) === trim(pcrCostEpsdDf("ETG_BASE_CLS_CD")), "left_outer")
			.join(mlsaZipCdDfFil, trim(mlsaZipCdDfFil("ZIP_CD")) === trim(pcrEtgMbrSmryDf("ZIP_CD")), "left_outer")
			.join(ndoZipSubmarketXwalkDf, trim(ndoZipSubmarketXwalkDf("ZIP_CODE")) === trim(pcrEtgMbrSmryDf("ZIP_CD"))
			&& trim(ndoZipSubmarketXwalkDf("ST_CD")) === trim(pcrEtgMbrSmryDf("MBR_RSDNT_ST_CD")) 
			&& trim(ndoZipSubmarketXwalkDf("Cnty_CD")) === trim(mlsaZipCdDfFil("MS_CNTY_CD")), "left_outer")
			.join(ndowrkRatgAddrDf, trim(ndowrkRatgAddrDf("Prov_Tax_id")) === trim(pcrCostEpsdDf("GRP_AGRGTN_ID"))
			&& trim(ndowrkRatgAddrDf("ratg_st_cd")) === trim(pcrCostEpsdDf("NTWK_ST_CD")), "left_outer")
			
			val pcrApiJoinPcrSmry = pcrApiJoinEtg.join(pcrEtgSmryDf,pcrEtgSmryDf("EPSD_NBR")===pcrApiJoinEtg("EPSD_NBR") 
			    && pcrEtgSmryDf("ETG_RUN_ID")===pcrCostEpsdDf("ETG_RUN_ID"),"left_outer" )
			
//20190930 update
//and trim(col("pcrCostEpsdDf1.PEER_MRKT_CD")) filter change to single peer_mrkt_cd
					
			val finalSel = pcrApiJoinPcrSmry.select(
					pcrCostEpsdJoin("BKBN_SNAP_YEAR_MNTH_NBR").alias("BKBN_SNAP_YEAR_MNTH_NBR"),
					pcrCostEpsdJoin("NTWK_ST_CD").alias("NTWK_ST_CD"),
					pcrCostEpsdJoin("PCR_LOB_DESC").alias("PCR_LOB_DESC"),
					pcrCostEpsdDf("EPSD_NBR").alias("EPSD_NBR"),
					fnclProdCfDf("PROD_LVL_2_DESC").alias("PROD_ID"),
					pcrCostEpsdDf("SPCLTY_PRMRY_CD").alias("SPCLTY_PRMRY_CD"),
					pcrCostEpsdDf("SPCLTY_PRMRY_DESC").alias("SPCLTY_PRMRY_DESC"),
					fnclMbuCfDf("MBU_LVL_2_DESC"),
					(when((when(pcrEtgMbrSmryDf("MBR_RSDNT_ST_CD").!==("NA"), pcrEtgMbrSmryDf("MBR_RSDNT_ST_CD")).otherwise(coalesce(mlsaZipCdDfFil("ST_CD"), lit("UNK")))).!==(pcrCostEpsdJoin("NTWK_ST_CD")),lit("UNK")) otherwise(coalesce(ndoZipSubmarketXwalkDf("SUBMARKET_NM"), lit("UNK")))).alias("MBR_RATNG_AREA"),
					when(pcrEtgMbrSmryDf("MBR_RSDNT_ST_CD") !== "NA", pcrEtgMbrSmryDf("MBR_RSDNT_ST_CD")).otherwise(coalesce(mlsaZipCdDfFil("ST_CD"), lit("UNK"))).alias("MBR_ST"),
					pcrEtgMbrSmryDf("ZIP_CD").alias("MBR_ZIP_CD"),
					pcrCostEpsdJoin("GRP_AGRGTN_ID").alias("GRP_AGRGTN_ID"),
					pcrCostEpsdJoin("GRP_AGRGTN_TYPE_CD").alias("GRP_AGRGTN_TYPE_CD"),
					pcrCostEpsdJoin("GRP_AGRGTN_NM").alias("GRP_AGRGTN_NM"),
					pcrCostEpsdDf("ETG_BASE_CLS_CD").alias("ETG_BASE_CLS_CD"),
					coalesce(etgBaseClsCdDf("cd_val_nm"), lit("NOTF")).alias("ETG_BASE_CLS_DESC"),
					pcrCostEpsdDf("PDL_SVRTY_LVL_CD").alias("PDL_SVRTY_LVL_CD"),
					pcrCostEpsdDf("SCRBL_ETG_IND_CD").alias("SCRBL_ETG_IND_CD"),
					coalesce(ndowrkRatgAddrDf("rating_area_desc"), lit("UNK")).alias("PROV_RATNG_AREA"),
					coalesce(pcrEtgSmryDf("RPTG_NTWK_DESC"), lit("UNK")).alias("RPTG_NTWK_DESC"),
					pcrCostEpsdDf("EPSD_TOTL_ANLZD_ALWD_AMT").alias("EPSD_TOTL_ANLZD_ALWD_AMT"),
					pcrCostEpsdDf("PEER_GRP_AVG_ANLZD_ALWD_AMT").alias("PEER_GRP_AVG_ANLZD_ALWD_AMT"),
					pcrCostEpsdDf("EPSD_OE_WGTD_NBR").alias("EPSD_OE_WGTD_NBR"),
					pcrCostEpsdDf("EPSD_NO_FI_TOTL_ANLZD_ALWD_AMT").alias("EPSD_NO_FI_TOTL_ANLZD_ALWD_AMT"),
					((pcrCostEpsdDf("PEER_TOTL_ANLZD_ALWD_AMT") - pcrCostEpsdDf("PEER_ANLZD_FCLTY_ALWD_AMT")) / pcrCostEpsdDf("PEER_GRP_EPSD_VOL_CNT")).alias("PEER_GRP_AVG_ANLZD_NOFI_AMT")).distinct()

			finalSel
	}
}

case class ndoWrkEpsdCost(ndoWrkEpsdCost2Df: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

					val slefjoin =  ndoWrkEpsdCost2Df.select($"BKBN_SNAP_YEAR_MNTH_NBR",
					$"NTWK_ST_CD",
					$"PCR_LOB_DESC",
					$"PROD_ID",
					$"SPCLTY_PRMRY_DESC".alias("SPCLTY_PRMRY_ID"),
					$"MBU_LVL_2_DESC".alias("BSNS_SGMNT"),
					$"MBR_RATNG_AREA",
					$"GRP_AGRGTN_ID",
					$"GRP_AGRGTN_TYPE_CD",
					$"ETG_BASE_CLS_CD",
					$"ETG_BASE_CLS_DESC",
					$"PDL_SVRTY_LVL_CD",
					$"SCRBL_ETG_IND_CD",
					$"PROV_RATNG_AREA",
					$"RPTG_NTWK_DESC",
					$"EPSD_TOTL_ANLZD_ALWD_AMT",
					$"PEER_GRP_AVG_ANLZD_ALWD_AMT",
					$"EPSD_NO_FI_TOTL_ANLZD_ALWD_AMT",
					$"PEER_GRP_AVG_ANLZD_NOFI_AMT",
					$"EPSD_NBR").groupBy(
							$"BKBN_SNAP_YEAR_MNTH_NBR",
							$"NTWK_ST_CD",
							$"PCR_LOB_DESC",
							$"PROD_ID",
							$"SPCLTY_PRMRY_ID",
							$"BSNS_SGMNT",
							$"MBR_RATNG_AREA",
							$"GRP_AGRGTN_ID",
							$"GRP_AGRGTN_TYPE_CD",
							$"ETG_BASE_CLS_CD",
							$"ETG_BASE_CLS_DESC",
							$"PDL_SVRTY_LVL_CD",
							$"SCRBL_ETG_IND_CD",
					    $"PROV_RATNG_AREA",
					    $"RPTG_NTWK_DESC").agg(
									countDistinct($"EPSD_NBR").alias("TOTL_EPSD_CNT"),
									(sum($"EPSD_TOTL_ANLZD_ALWD_AMT") / (count("*") / countDistinct($"EPSD_NBR"))).alias("EPSD_TOTLS_ANLZD_ALWD_AMT"),
									(sum($"PEER_GRP_AVG_ANLZD_ALWD_AMT") / (count("*") / countDistinct($"EPSD_NBR"))).alias("PEER_GRP_EXPCTD_ANLZD_ALWD_AMT"),
									(sum($"EPSD_NO_FI_TOTL_ANLZD_ALWD_AMT") / (count("*") / countDistinct($"EPSD_NBR"))).alias("EPSD_NO_FI_TOTLS_ANLZD_ALWD_AMT"),
									(sum($"PEER_GRP_AVG_ANLZD_NOFI_AMT") / (count("*") / countDistinct($"EPSD_NBR"))).alias("PEER_NO_FI_GRP_EXPCTD_ANLZD_AMT")).select(
							$"BKBN_SNAP_YEAR_MNTH_NBR",
							$"NTWK_ST_CD",
							$"PCR_LOB_DESC",
							$"PROD_ID",
							$"SPCLTY_PRMRY_ID",
							$"BSNS_SGMNT",
							$"MBR_RATNG_AREA",
							$"GRP_AGRGTN_ID",
							$"GRP_AGRGTN_TYPE_CD",
							$"ETG_BASE_CLS_CD",
							$"ETG_BASE_CLS_DESC",
							$"PDL_SVRTY_LVL_CD",
							$"SCRBL_ETG_IND_CD",
					    $"PROV_RATNG_AREA",
					    $"RPTG_NTWK_DESC",
							$"EPSD_TOTLS_ANLZD_ALWD_AMT",
							$"PEER_GRP_EXPCTD_ANLZD_ALWD_AMT",
							$"EPSD_NO_FI_TOTLS_ANLZD_ALWD_AMT",
							$"PEER_NO_FI_GRP_EXPCTD_ANLZD_AMT",
							$"TOTL_EPSD_CNT")
							    
			slefjoin
	}
}
