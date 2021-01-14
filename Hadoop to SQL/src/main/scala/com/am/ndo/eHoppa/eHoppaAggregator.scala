package com.am.ndo.eHoppa

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
import scala.collection.immutable.StringOps._

case class ndowrkOutpZip1(outpatientDetailDf: Dataset[Row] ,rangeStartDate : String,rangeEndDate : String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val ndowrkOutpZip1 = outpatientDetailDf.filter(outpatientDetailDf("MBR_State").isin("CA", "CO", "CT", "GA", "IN", "KY", "ME", "MO", "NH", "NV", "NY", "OH", "VA", "WI") &&
					outpatientDetailDf("PROV_ST_NM").isin("CA", "CO", "CT", "GA", "IN", "KY", "ME", "MO", "NH", "NV", "NY", "OH", "VA", "WI") &&
					outpatientDetailDf("prodlvl3").isNotNull &&
					outpatientDetailDf("MBUlvl2").isNotNull &&
					outpatientDetailDf("MBR_County").isNotNull &&
					outpatientDetailDf("MCS").!==("Y") &&
					outpatientDetailDf("liccd").===("01") &&
					outpatientDetailDf("MBU_CF_CD").isin("MCKYGP", "SRLGUN", "SRLGCO", "SRLGVA", "MCOHGP", "SRLGNV", "MCINGP", "SRLGME", "SRLGNH",
							"SRLGKS", "SRLGGA", "SRLGNA", "SRNAHS", "SRLGMO", "SRLGWI", "SRLGNY", "SRLGCT",
							"SRLGBC", "MJCAMC", "SPCAMC") === false &&
							$"Inc_Month".between(rangeStartDate, rangeEndDate)).select(
									outpatientDetailDf("MBR_State"),
									outpatientDetailDf("MBR_County"),
									outpatientDetailDf("PROV_ST_NM"),
									outpatientDetailDf("PROV_County"),
									outpatientDetailDf("MEDCR_ID"),
									outpatientDetailDf("MBR_ZIP3"),
									outpatientDetailDf("MBR_ZIP_CD"),
									outpatientDetailDf("prodlvl3"),
									outpatientDetailDf("fundlvl2"),
									outpatientDetailDf("MBUlvl2"),
									outpatientDetailDf("MBUlvl4"),
									outpatientDetailDf("MBUlvl3"),
									outpatientDetailDf("inn_cd")).distinct

								
			ndowrkOutpZip1
	}
}

case class ndowrkOutpZip2(ndowrkOutpZip1Df: Dataset[Row], ndoZipSubmarketXxwalkDf: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val joinndoZip = ndowrkOutpZip1Df.join(ndoZipSubmarketXxwalkDf, trim(ndoZipSubmarketXxwalkDf("st_cd")) === trim(ndowrkOutpZip1Df("MBR_State"))
			&& trim(ndoZipSubmarketXxwalkDf("Zip_Code")) === trim(ndowrkOutpZip1Df("MBR_ZIP_CD"))
			&& trim(ndoZipSubmarketXxwalkDf("CNTY_NM")) === trim(ndowrkOutpZip1Df("MBR_County")), "inner")

			val joinndoZip1 = joinndoZip.select(
					ndowrkOutpZip1Df("MBR_State"),
					ndowrkOutpZip1Df("MBR_County"),
					ndowrkOutpZip1Df("PROV_ST_NM"),
					ndowrkOutpZip1Df("PROV_County"),
					ndowrkOutpZip1Df("MEDCR_ID"),
					ndowrkOutpZip1Df("MBR_ZIP3"),
					(upper(coalesce(trim(ndoZipSubmarketXxwalkDf("submarket_NM")), lit("UNK")))).alias("submarket_NM_1"),
					ndowrkOutpZip1Df("prodlvl3"),
					ndowrkOutpZip1Df("fundlvl2"),
					ndowrkOutpZip1Df("MBUlvl2"),
					ndowrkOutpZip1Df("MBUlvl4"),
					ndowrkOutpZip1Df("MBUlvl3"),
					ndowrkOutpZip1Df("inn_cd"),
					trim(ndoZipSubmarketXxwalkDf("submarket_NM")).alias("submarket_NM"))

			val windowFunc1 = Window.partitionBy(
					ndowrkOutpZip1Df("MBR_State"),
					ndowrkOutpZip1Df("MBR_County"),
					ndowrkOutpZip1Df("PROV_ST_NM"),
					ndowrkOutpZip1Df("PROV_County"),
					ndowrkOutpZip1Df("MEDCR_ID"),
					ndowrkOutpZip1Df("prodlvl3"),
					ndowrkOutpZip1Df("fundlvl2"),
					ndowrkOutpZip1Df("MBUlvl2"),
					ndowrkOutpZip1Df("MBUlvl4"),
					ndowrkOutpZip1Df("MBUlvl3"),
					ndowrkOutpZip1Df("inn_cd"),
					ndowrkOutpZip1Df("MBR_ZIP3")).orderBy(
							(when(joinndoZip1("submarket_NM_1") === "UNK", 99)
									when (expr("instr(submarket_NM,trim(MBR_County))") > 0, lit(1)) otherwise (lit(2))).asc,
							joinndoZip1("submarket_NM_1"))
			val ndowrkOutpZip2 = joinndoZip1.withColumn("RANK", row_number() over (windowFunc1)).
			filter($"RANK".===(1)).drop($"RANK").drop("submarket_NM").select(
					ndowrkOutpZip1Df("MBR_State"),
					ndowrkOutpZip1Df("MBR_County"),
					ndowrkOutpZip1Df("PROV_ST_NM"),
					ndowrkOutpZip1Df("PROV_County"),
					ndowrkOutpZip1Df("MEDCR_ID"),
					ndowrkOutpZip1Df("MBR_ZIP3"),
					$"submarket_NM_1".alias("submarket_NM"),
					ndowrkOutpZip1Df("prodlvl3"),
					ndowrkOutpZip1Df("fundlvl2"),
					ndowrkOutpZip1Df("MBUlvl2"),
					ndowrkOutpZip1Df("MBUlvl4"),
					ndowrkOutpZip1Df("MBUlvl3"),
					ndowrkOutpZip1Df("inn_cd"))

					
					ndowrkOutpZip2
			
	}

}

case class ndowrkHoppaCmadBench(facilityAttributeProfileDfFil: Dataset[Row], inpatientSummaryFil: Dataset[Row], outpatientSummaryDfFil: Dataset[Row], stagingHiveDB: String,rangeStartDate : String,rangeEndDate : String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			
			val joinInpatFac = inpatientSummaryFil.join(
					facilityAttributeProfileDfFil,
					trim(facilityAttributeProfileDfFil("Medcr_ID")) === trim(inpatientSummaryFil("Medcr_ID")), "left_outer").filter(trim(inpatientSummaryFil("Prov_ST_NM")).isNotNull &&
							trim(inpatientSummaryFil("Prov_ST_NM")).!==("") && trim(inpatientSummaryFil("MBUlvl2")).isNotNull &&
							trim(inpatientSummaryFil("prodlvl3")).isNotNull && trim(inpatientSummaryFil("MCS")).!==("Y") && trim(inpatientSummaryFil("liccd")) === ("01") &&
							inpatientSummaryFil("MBU_CF_CD").isin("MCKYGP", "SRLGUN", "SRLGCO", "SRLGVA", "MCOHGP", "SRLGNV", "MCINGP", "SRLGME", "SRLGNH",
									"SRLGKS", "SRLGGA", "SRLGNA", "SRNAHS", "SRLGMO", "SRLGWI", "SRLGNY", "SRLGCT",
									"SRLGBC", "MJCAMC", "SPCAMC") === false && facilityAttributeProfileDfFil("factype").isin("Delete", "Long Term Care Hospital", "Snf", "Physical Rehab") === false &&
									$"Inc_Month".between(rangeStartDate, rangeEndDate)).withColumn("Category_CD", (when(trim(inpatientSummaryFil("cat1")).===("Maternity") ||
									(trim(inpatientSummaryFil("MEDCR_ID")).isin("110005", "110008", "110161") &&
											trim(inpatientSummaryFil("FNL_DRG_CD")).isin("939", "940", "941", "951")), lit("MATERNITY"))
											when (trim(inpatientSummaryFil("cat1")).===("Newborn") && trim(inpatientSummaryFil("cat2")).=== ("Neonate"), lit("NICU"))
											when (trim(inpatientSummaryFil("cat1")).===("Transplant"), lit("TRANSPLANT"))
											otherwise (lit("IPOTH")))).withColumn("CMAD_CASES", (when(inpatientSummaryFil("CMAD_CASES").isNull, lit(0)) otherwise (inpatientSummaryFil("CMAD_CASES"))))
			.withColumn("CMAD", (when(inpatientSummaryFil("CMAD").isNull, lit(0)) otherwise (inpatientSummaryFil("CMAD")))).groupBy($"Category_CD").agg(
													sum($"CMAD_CASES").alias("CMAD_CASE_CNT"),
													sum($"CMAD").alias("CMAD")).withColumn("CMI_Bench", (when($"CMAD_CASE_CNT" === 0, lit(0)) otherwise ($"CMAD" / $"CMAD_CASE_CNT"))).distinct


			//Sub query starts here

			val joinOutpatFac = outpatientSummaryDfFil.join(
					facilityAttributeProfileDfFil,
					trim(facilityAttributeProfileDfFil("Medcr_ID")) === trim(outpatientSummaryDfFil("Medcr_ID")), "left_outer").filter(trim(outpatientSummaryDfFil("Prov_ST_NM")).isNotNull &&
							trim(outpatientSummaryDfFil("Prov_ST_NM")).!==("") && trim(outpatientSummaryDfFil("MBUlvl2")).isNotNull &&
							trim(outpatientSummaryDfFil("prodlvl3")).isNotNull && trim(outpatientSummaryDfFil("MCS")).!==("Y") && trim(outpatientSummaryDfFil("liccd")) === ("01") &&
							trim(outpatientSummaryDfFil("MBU_CF_CD")).isin("MCKYGP", "SRLGUN", "SRLGCO", "SRLGVA", "MCOHGP", "SRLGNV", "MCINGP", "SRLGME", "SRLGNH",
									"SRLGKS", "SRLGGA", "SRLGNA", "SRNAHS", "SRLGMO", "SRLGWI", "SRLGNY", "SRLGCT",
									"SRLGBC", "MJCAMC", "SPCAMC") === false && facilityAttributeProfileDfFil("factype").isin("Delete", "Long Term Care Hospital", "Snf", "Physical Rehab") === false &&
									$"Inc_Month".between(rangeStartDate, rangeEndDate)).select(
											(when(trim(outpatientSummaryDfFil("cat1")).===("Surgery") && trim(outpatientSummaryDfFil("ER_Flag")) === "Y", lit("ER_SURGERY"))
													when (trim(outpatientSummaryDfFil("cat1")).===("Surgery") && trim(outpatientSummaryDfFil("ER_Flag")) === "N", lit("SURGERY"))
													when (trim(outpatientSummaryDfFil("cat1")).===("ER / Urgent"), lit("ER"))
                          when (trim(outpatientSummaryDfFil("cat1")).===("Lab"), lit("LAB"))
													when (trim(outpatientSummaryDfFil("cat1")).===("Radiology") && trim(outpatientSummaryDfFil("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms"), lit("RAD_SCANS"))
													when (trim(outpatientSummaryDfFil("cat1")).===("Radiology") && trim(outpatientSummaryDfFil("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms") === false, lit("RAD_OTHER"))
													when (trim(outpatientSummaryDfFil("cat1")).===("Other") && trim(outpatientSummaryDfFil("cat2")).===("Drugs-Infusion/Injection Therapy"), lit("DRUGS"))
													when (trim(outpatientSummaryDfFil("cat1")).===("Other") && trim(outpatientSummaryDfFil("cat2")).!==("Drugs-Infusion/Injection Therapy"), lit("OPOTH"))
													otherwise (concat(lit("OTHER: "), trim(outpatientSummaryDfFil("cat1")), lit(", "), trim(outpatientSummaryDfFil("cat2"))))).alias("Category_CD"),
											(when(trim(outpatientSummaryDfFil("CMAD_CASES")).isNull, lit(0)) otherwise (trim(outpatientSummaryDfFil("CMAD_CASES")))).alias("CMAD_CASES"), (when(trim(outpatientSummaryDfFil("CMAD")).isNull, lit(0)) otherwise (outpatientSummaryDfFil("CMAD"))).alias("CMAD")).groupBy($"Category_CD").agg(
													sum($"CMAD_CASES").alias("CMAD_CASE_CNT"),
													sum($"CMAD").alias("CMAD")).withColumn("CMI_Bench", (when($"CMAD_CASE_CNT" === 0, lit(0)) otherwise ($"CMAD" / $"CMAD_CASE_CNT"))).distinct

			val finalDf = joinInpatFac.union(joinOutpatFac)
			finalDf
	}
}

case class ndowrkHoppaOutp(facilityAttributeProfileDfFil: Dataset[Row], outpatientSummaryDf: Dataset[Row], tabndowrkHoppaCmadBench: String, ndoZipSubmarketXxwalkDf: Dataset[Row], stagingHiveDB: String,rangeStartDate : String,rangeEndDate : String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val ndowrkHoppaCmadBenchDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaCmadBench")

			val joinndowrkOutPat = outpatientSummaryDf.withColumn("category", (when(trim(outpatientSummaryDf("cat1")).===("ER / Urgent"), lit("ER"))
					when (trim(outpatientSummaryDf("cat1")).===("Lab"), lit("LAB"))
					when (trim(outpatientSummaryDf("cat1")).===("Radiology") && trim(outpatientSummaryDf("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms"), lit("RAD_SCANS"))
					when (trim(outpatientSummaryDf("cat1")).===("Radiology") && trim(outpatientSummaryDf("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms") === false, lit("RAD_OTHER"))
					when (trim(outpatientSummaryDf("cat1")).===("Surgery") && trim(outpatientSummaryDf("ER_Flag")) === "N", lit("SURGERY"))
					when (trim(outpatientSummaryDf("cat1")).===("Surgery") && trim(outpatientSummaryDf("ER_Flag")) === "Y", lit("ER_SURGERY"))
					when (trim(outpatientSummaryDf("cat1")).===("Other") && trim(outpatientSummaryDf("cat2")).===("Drugs-Infusion/Injection Therapy"), lit("DRUGS"))
					when (trim(outpatientSummaryDf("cat1")).===("Other") && trim(outpatientSummaryDf("cat2")).!==("Drugs-Infusion/Injection Therapy"), lit("OPOTH"))
					otherwise (concat(lit("OTHER: "), trim(outpatientSummaryDf("cat1")), lit(", "), trim(outpatientSummaryDf("cat2"))))))

			val joinndowrk = joinndowrkOutPat.join(ndowrkHoppaCmadBenchDf, ndowrkHoppaCmadBenchDf("category_cd") === $"category", "inner")
			.join(facilityAttributeProfileDfFil, trim(facilityAttributeProfileDfFil("Medcr_ID")) === trim(outpatientSummaryDf("Medcr_ID")), "left_outer")
			.join(ndoZipSubmarketXxwalkDf, trim(ndoZipSubmarketXxwalkDf("Zip_Code")) === trim(outpatientSummaryDf("PROV_ZIP_CD")) &&
			trim(ndoZipSubmarketXxwalkDf("ST_CD")) === trim(outpatientSummaryDf("PROV_ST_NM")) && trim(ndoZipSubmarketXxwalkDf("CNTY_NM")) === trim(outpatientSummaryDf("PROV_County")), "left_outer").filter(trim(outpatientSummaryDf("Prov_ST_NM")).isNotNull &&
					trim(outpatientSummaryDf("Prov_ST_NM")).!==("") && trim(outpatientSummaryDf("MBUlvl2")).isNotNull &&
					trim(outpatientSummaryDf("prodlvl3")).isNotNull && trim(outpatientSummaryDf("MCS")).!==("Y") && trim(outpatientSummaryDf("liccd")) === ("01") &&
					trim(outpatientSummaryDf("MBU_CF_CD")).isin("MCKYGP", "SRLGUN", "SRLGCO", "SRLGVA", "MCOHGP", "SRLGNV", "MCINGP", "SRLGME", "SRLGNH",
							"SRLGKS", "SRLGGA", "SRLGNA", "SRNAHS", "SRLGMO", "SRLGWI", "SRLGNY", "SRLGCT",
							"SRLGBC", "MJCAMC", "SPCAMC") === false && facilityAttributeProfileDfFil("factype").isin("Delete", "Long Term Care Hospital", "Snf", "Physical Rehab") === false &&
							$"Inc_Month".between(rangeStartDate, rangeEndDate)).select(
									outpatientSummaryDf("PROV_ST_NM"),
									(coalesce(outpatientSummaryDf("prodlvl3"), lit("UNK"))).alias("PROD_LVL3_DESC"),
									(coalesce(outpatientSummaryDf("fundlvl2"), lit("UNK"))).alias("FUNDG_LVL2_DESC"),
									(coalesce(outpatientSummaryDf("MBUlvl4"), lit("UNK"))).alias("MBU_LVL4_DESC"),
									(coalesce(outpatientSummaryDf("MBUlvl3"), lit("UNK"))).alias("MBU_LVL3_DESC"),
									(coalesce(outpatientSummaryDf("EXCHNG_IND_CD"), lit("N/A"))).alias("EXCHNG_IND_CD"),
									(coalesce(outpatientSummaryDf("MBUlvl2"), lit("UNK"))).alias("MBU_LVL2_DESC"),
									outpatientSummaryDf("MEDCR_ID").alias("MEDCR_NBR"),
									(when(facilityAttributeProfileDfFil("Hospital").isNull, upper($"PROV_NM")) otherwise (upper(facilityAttributeProfileDfFil("Hospital")))).alias("HOSP_NM"),
									(coalesce(outpatientSummaryDf("System_ID"), lit("UNK"))).alias("HOSP_SYS_NM"),
									(when(ndoZipSubmarketXxwalkDf("RATING_AREA_DESC").isNotNull, ndoZipSubmarketXxwalkDf("RATING_AREA_DESC"))
											when (facilityAttributeProfileDfFil("Rating_Area").isNotNull, facilityAttributeProfileDfFil("Rating_Area")) otherwise (lit("UNK"))).alias("SUBMRKT_DESC"),
									outpatientSummaryDf("prov_county").alias("PROV_CNTY_NM"),
									(coalesce(facilityAttributeProfileDfFil("factype"), lit("UNK"))).alias("FCLTY_TYPE_DESC"),
									outpatientSummaryDf("INN_CD").alias("PAR_STTS_CD"),
									(when(facilityAttributeProfileDfFil("Hospital").isNull, lit("B")) otherwise (lit("A"))).alias("PROV_EXCLSN_CD"),
									(when(trim(outpatientSummaryDf("cat1")).===("Surgery") && trim(outpatientSummaryDf("ER_Flag")) === "Y", lit("ER_SURGERY"))
											when (trim(outpatientSummaryDf("cat1")).===("Surgery") && trim(outpatientSummaryDf("ER_Flag")) === "N", lit("SURGERY"))
											when (trim(outpatientSummaryDf("cat1")).===("ER / Urgent"), lit("ER"))
                      when (trim(outpatientSummaryDf("cat1")).===("Lab"), lit("LAB"))
											when (trim(outpatientSummaryDf("cat1")).===("Radiology") && trim(outpatientSummaryDf("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms"), lit("RAD_SCANS"))
											when (trim(outpatientSummaryDf("cat1")).===("Radiology") && trim(outpatientSummaryDf("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms") === false, lit("RAD_OTHER"))
											when (trim(outpatientSummaryDf("cat1")).===("Other") && trim(outpatientSummaryDf("cat2")).===("Drugs-Infusion/Injection Therapy"), lit("DRUGS"))
											when (trim(outpatientSummaryDf("cat1")).===("Other") && trim(outpatientSummaryDf("cat2")).!==("Drugs-Infusion/Injection Therapy"), lit("OPOTH"))
											otherwise (concat(lit("OTHER: "), trim(outpatientSummaryDf("cat1")), lit(", "), trim(outpatientSummaryDf("cat2"))))).alias("Category"),
									ndowrkHoppaCmadBenchDf("CMI_Bench"),
                  (coalesce(trim(outpatientSummaryDf("RPTG_NTWK_DESC")), lit("UNK"))).alias("RPTG_NTWK_DESC"),					
									(when(outpatientSummaryDf("ALWD_AMT").isNull, lit(0)) otherwise (outpatientSummaryDf("ALWD_AMT"))).alias("ALWD_AMT"),
									(when(outpatientSummaryDf("BILLD_CHRG_AMT").isNull, lit(0)) otherwise (outpatientSummaryDf("BILLD_CHRG_AMT"))).alias("BILLD_CHRG_AMT"),
									(when(outpatientSummaryDf("PAID_AMT").isNull, lit(0)) otherwise (outpatientSummaryDf("PAID_AMT"))).alias("PAID_AMT"),
									(when(outpatientSummaryDf("cases").isNull, lit(0)) otherwise (outpatientSummaryDf("cases"))).alias("cases"),
									outpatientSummaryDf("ER_FLAG"),
									(when(outpatientSummaryDf("CMAD_CASES").isNull, lit(0)) otherwise (outpatientSummaryDf("CMAD_CASES"))).alias("CMAD_CASES"),
									(when(outpatientSummaryDf("CMAD_ALLOWED").isNull, lit(0)) otherwise (outpatientSummaryDf("CMAD_ALLOWED"))).alias("CMAD_ALLOWED"),
									(when(outpatientSummaryDf("CMAD").isNull, lit(0)) otherwise (outpatientSummaryDf("CMAD"))).alias("CMAD"),
									(when(outpatientSummaryDf("ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (outpatientSummaryDf("ALWD_AMT_WITH_CMS"))).alias("ALWD_AMT_WITH_CMS"),
									(when(outpatientSummaryDf("CMS_Allowed").isNull, lit(0)) otherwise (outpatientSummaryDf("CMS_Allowed"))).alias("CMS_REIMBMNT_AMT")).groupBy(
											$"PROV_ST_NM",
											$"PROD_LVL3_DESC",
											$"FUNDG_LVL2_DESC",
											$"MBU_LVL4_DESC",
											$"MBU_LVL3_DESC",
											$"EXCHNG_IND_CD",
											$"MBU_LVL2_DESC",
											$"MEDCR_NBR",
											$"HOSP_NM",
											$"HOSP_SYS_NM",
											$"SUBMRKT_DESC",
											$"PROV_CNTY_NM",
											$"FCLTY_TYPE_DESC",
											$"PAR_STTS_CD",
											$"PROV_EXCLSN_CD",
											$"Category",
											ndowrkHoppaCmadBenchDf("CMI_Bench"),
           		        $"RPTG_NTWK_DESC").agg(
													sum($"ALWD_AMT").alias("ALWD_AMT"),
													sum($"BILLD_CHRG_AMT").alias("BILLD_CHRG_AMT"),
													sum($"PAID_AMT").alias("PAID_AMT"),
													(when(sum($"cases") === 0 || sum($"ALWD_AMT") === 0, lit(0)) otherwise (sum($"ALWD_AMT") / sum($"cases"))).alias("Alwd_Case_Amt"),
													(when(sum($"cases") === 0 || sum($"BILLD_CHRG_AMT") === 0, lit(0)) otherwise (sum($"BILLD_CHRG_AMT") / sum($"cases"))).alias("BILLD_CHRG_Case_AMT"),
													(when(sum($"cases") === 0 || sum($"PAID_AMT") === 0, lit(0)) otherwise (sum($"PAID_AMT") / sum($"cases"))).alias("PAID_Case_AMT"),
													sum($"CASES").alias("CASE_CNT"),
													sum(when($"ER_FLAG" === "Y", $"cases") otherwise (lit(0))).alias("ER_CASE_CNT"),
													sum($"CMAD_CASES").alias("CMAD_CASE_CNT"),
													sum($"CMAD_ALLOWED").alias("CMAD_ALLOWED"),
													(sum($"CMAD")/ $"CMI_Bench").alias("CMAD"),
													sum($"ALWD_AMT_WITH_CMS").alias("ALWD_AMT_WITH_CMS"),
													sum($"CMS_REIMBMNT_AMT").alias("CMS_REIMBMNT_AMT"))
													.withColumn("CMI", (when($"CMAD_CASE_CNT" === 0, lit(0)) otherwise (($"CMAD" / $"CMAD_CASE_CNT") / $"CMI_Bench")))
													
				
			joinndowrk
	}
}

case class ndowrkHoppaInp(facilityAttributeProfileDfFil: Dataset[Row], inpatientSummaryDf: Dataset[Row], tabndowrkHoppaCmadBench: String, ndoZipSubmarketXxwalkDf: Dataset[Row], stagingHiveDB: String,rangeStartDate : String,rangeEndDate : String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val ndowrkHoppaCmadBenchDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaCmadBench")

			val joinndowrkOutPat = inpatientSummaryDf.withColumn("category", ((when(trim(inpatientSummaryDf("cat1")).===("Maternity") ||
			(trim(inpatientSummaryDf("MEDCR_ID")).isin("110005", "110008", "110161") &&
					trim(inpatientSummaryDf("FNL_DRG_CD")).isin("939", "940", "941", "951")), lit("MATERNITY"))
					when (trim(inpatientSummaryDf("cat1")).===("Newborn") && trim(inpatientSummaryDf("cat2")).=== ("Neonate"), lit("NICU"))
					otherwise (lit("IPOTH")))))

			val joinndowrk = joinndowrkOutPat.join(ndowrkHoppaCmadBenchDf, ndowrkHoppaCmadBenchDf("category_cd") === $"category", "inner")
			.join(facilityAttributeProfileDfFil, trim(facilityAttributeProfileDfFil("Medcr_ID")) === trim(inpatientSummaryDf("Medcr_ID")), "left_outer")
			.join(ndoZipSubmarketXxwalkDf, trim(ndoZipSubmarketXxwalkDf("Zip_Code")) === trim(inpatientSummaryDf("PROV_ZIP_CD")) &&
			trim(ndoZipSubmarketXxwalkDf("ST_CD")) === trim(inpatientSummaryDf("PROV_ST_NM")) && trim(ndoZipSubmarketXxwalkDf("CNTY_NM")) === trim(inpatientSummaryDf("PROV_County")), "left_outer").filter(trim(inpatientSummaryDf("Prov_ST_NM")).isNotNull &&
					trim(inpatientSummaryDf("Prov_ST_NM")).!==("") && trim(inpatientSummaryDf("MBUlvl2")).isNotNull &&
					trim(inpatientSummaryDf("prodlvl3")).isNotNull && trim(inpatientSummaryDf("MCS")).!==("Y") && trim(inpatientSummaryDf("liccd")) === ("01") &&
					trim(inpatientSummaryDf("MBU_CF_CD")).isin("MCKYGP", "SRLGUN", "SRLGCO", "SRLGVA", "MCOHGP", "SRLGNV", "MCINGP", "SRLGME", "SRLGNH",
							"SRLGKS", "SRLGGA", "SRLGNA", "SRNAHS", "SRLGMO", "SRLGWI", "SRLGNY", "SRLGCT",
							"SRLGBC", "MJCAMC", "SPCAMC") === false && facilityAttributeProfileDfFil("factype").isin("Delete", "Long Term Care Hospital", "Snf", "Physical Rehab") === false &&
							$"Inc_Month".between(rangeStartDate, rangeEndDate)).select(
									inpatientSummaryDf("PROV_ST_NM"),
									(coalesce(inpatientSummaryDf("prodlvl3"), lit("UNK"))).alias("PROD_LVL3_DESC"),
									(coalesce(inpatientSummaryDf("fundlvl2"), lit("UNK"))).alias("FUNDG_LVL2_DESC"),
									(coalesce(inpatientSummaryDf("MBUlvl4"), lit("UNK"))).alias("MBU_LVL4_DESC"),
									(coalesce(inpatientSummaryDf("MBUlvl3"), lit("UNK"))).alias("MBU_LVL3_DESC"),
									(coalesce(inpatientSummaryDf("EXCHNG_IND_CD"), lit("N/A"))).alias("EXCHNG_IND_CD"),
									(coalesce(inpatientSummaryDf("MBUlvl2"), lit("UNK"))).alias("MBU_LVL2_DESC"),
									inpatientSummaryDf("MEDCR_ID").alias("MEDCR_NBR"),
									(when(facilityAttributeProfileDfFil("Hospital").isNull, upper($"PROV_NM")) otherwise (upper(facilityAttributeProfileDfFil("Hospital")))).alias("HOSP_NM"),
									(coalesce(inpatientSummaryDf("System_ID"), lit("UNK"))).alias("HOSP_SYS_NM"),
									(when(ndoZipSubmarketXxwalkDf("RATING_AREA_DESC").isNotNull, ndoZipSubmarketXxwalkDf("RATING_AREA_DESC"))
											when (facilityAttributeProfileDfFil("Rating_Area").isNotNull, facilityAttributeProfileDfFil("Rating_Area")) otherwise (lit("UNK"))).alias("SUBMRKT_DESC"),
									inpatientSummaryDf("prov_county").alias("PROV_CNTY_NM"),
									(coalesce(facilityAttributeProfileDfFil("factype"), lit("UNK"))).alias("FCLTY_TYPE_DESC"),
									inpatientSummaryDf("INN_CD").alias("PAR_STTS_CD"),
									(when(facilityAttributeProfileDfFil("Hospital").isNull, lit("B")) otherwise (lit("A"))).alias("PROV_EXCLSN_CD"),
									(when(trim(inpatientSummaryDf("cat1")).===("Maternity") ||
									(trim(inpatientSummaryDf("MEDCR_ID")).isin("110005", "110008", "110161") &&
											trim(inpatientSummaryDf("FNL_DRG_CD")).isin("939", "940", "941", "951")), lit("MATERNITY"))
											when (trim(inpatientSummaryDf("cat1")).===("Newborn")&& trim(inpatientSummaryDf("cat2")).=== ("Neonate"), lit("NICU"))
											when (trim(inpatientSummaryDf("cat1")).===("Transplant"), lit("TRANSPLANT"))
											otherwise (lit("IPOTH"))).alias("Category"),
									ndowrkHoppaCmadBenchDf("CMI_Bench"),
                  (coalesce(inpatientSummaryDf("RPTG_NTWK_DESC"), lit("UNK"))).alias("RPTG_NTWK_DESC"),		
									(when(inpatientSummaryDf("ALWD_AMT").isNull, lit(0)) otherwise (inpatientSummaryDf("ALWD_AMT"))).alias("ALWD_AMT"),
									(when(inpatientSummaryDf("BILLD_CHRG_AMT").isNull, lit(0)) otherwise (inpatientSummaryDf("BILLD_CHRG_AMT"))).alias("BILLD_CHRG_AMT"),
									(when(inpatientSummaryDf("PAID_AMT").isNull, lit(0)) otherwise (inpatientSummaryDf("PAID_AMT"))).alias("PAID_AMT"),
									(when(inpatientSummaryDf("cases").isNull, lit(0)) otherwise (inpatientSummaryDf("cases"))).alias("cases"),
									inpatientSummaryDf("ER_FLAG"),
									(when(inpatientSummaryDf("CMAD_CASES").isNull, lit(0)) otherwise (inpatientSummaryDf("CMAD_CASES"))).alias("CMAD_CASES"),
									(when(inpatientSummaryDf("CMAD_ALLOWED").isNull, lit(0)) otherwise (inpatientSummaryDf("CMAD_ALLOWED"))).alias("CMAD_ALLOWED"),
									(when(inpatientSummaryDf("CMAD").isNull, lit(0)) otherwise (inpatientSummaryDf("CMAD"))).alias("CMAD"),
									(when(inpatientSummaryDf("ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (inpatientSummaryDf("ALWD_AMT_WITH_CMS"))).alias("ALWD_AMT_WITH_CMS"),
									(when(inpatientSummaryDf("CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (inpatientSummaryDf("CMS_REIMBMNT_AMT"))).alias("CMS_REIMBMNT_AMT")).groupBy(
											$"PROV_ST_NM",
											$"PROD_LVL3_DESC",
											$"FUNDG_LVL2_DESC",
											$"MBU_LVL4_DESC",
											$"MBU_LVL3_DESC",
											$"EXCHNG_IND_CD",
											$"MBU_LVL2_DESC",
											$"MEDCR_NBR",
											$"HOSP_NM",
											$"HOSP_SYS_NM",
											$"SUBMRKT_DESC",
											$"PROV_CNTY_NM",
											$"FCLTY_TYPE_DESC",
											$"PAR_STTS_CD",
											$"PROV_EXCLSN_CD",
											$"Category",
											ndowrkHoppaCmadBenchDf("CMI_Bench"),
			                $"RPTG_NTWK_DESC").agg(
													sum($"ALWD_AMT").alias("ALWD_AMT"),
													sum($"BILLD_CHRG_AMT").alias("BILLD_CHRG_AMT"),
													sum($"PAID_AMT").alias("PAID_AMT"),
													(when(sum($"cases") === 0 || sum($"ALWD_AMT") === 0, lit(0)) otherwise (sum($"ALWD_AMT") / sum($"cases"))).alias("Alwd_Case_Amt"),
													(when(sum($"cases") === 0 || sum($"BILLD_CHRG_AMT") === 0, lit(0)) otherwise (sum($"BILLD_CHRG_AMT") / sum($"cases"))).alias("BILLD_CHRG_Case_AMT"),
													(when(sum($"cases") === 0 || sum($"PAID_AMT") === 0, lit(0)) otherwise (sum($"PAID_AMT") / sum($"cases"))).alias("PAID_Case_AMT"),
													sum($"CASES").alias("CASE_CNT"),
													sum(when($"ER_FLAG" === "Y", $"cases") otherwise (lit(0))).alias("ER_CASE_CNT"),
													sum($"CMAD_CASES").alias("CMAD_CASE_CNT"),
													sum($"CMAD_ALLOWED").alias("CMAD_ALLOWED"),
													(sum($"CMAD") / $"CMI_Bench").alias("CMAD"),
													sum($"ALWD_AMT_WITH_CMS").alias("ALWD_AMT_WITH_CMS"),
													sum($"CMS_REIMBMNT_AMT").alias("CMS_REIMBMNT_AMT")).withColumn("CMI", (when($"CMAD_CASE_CNT" === 0, lit(0)) otherwise (($"CMAD" / $"CMAD_CASE_CNT") / $"CMI_Bench")))

			joinndowrk

	}
}

case class ndowrkEhoppaBase(tabndowrkHoppaInpDf: String, tabndowrkHoppaOutpDf: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			//val ndowrkHoppaProvListDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaProvList")
			val ndowrkHoppaInpDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaInpDf")
			val ndowrkHoppaOutpDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaOutpDf")

			val ndowrkHoppaInpDfDist = ndowrkHoppaInpDf.select(
					$"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MEDCR_NBR",
				  $"HOSP_NM",
					$"HOSP_SYS_NM",
					$"SUBMRKT_DESC",
					$"PROV_CNTY_NM",
					$"FCLTY_TYPE_DESC",
					$"PAR_STTS_CD",
					$"PROV_EXCLSN_CD",
		      $"RPTG_NTWK_DESC").distinct

			val ndowrkHoppaOutpDfDist = ndowrkHoppaOutpDf.select(
				$"PROV_ST_NM",
				$"PROD_LVL3_DESC",
				$"FUNDG_LVL2_DESC",
				$"MBU_LVL2_DESC",
				$"MBU_LVL4_DESC",
				$"EXCHNG_IND_CD",
				$"MBU_LVL3_DESC",
				$"MEDCR_NBR",
				$"HOSP_NM",
        $"HOSP_SYS_NM",
				$"SUBMRKT_DESC",
				$"PROV_CNTY_NM",
				$"FCLTY_TYPE_DESC",
				$"PAR_STTS_CD",
				$"PROV_EXCLSN_CD",
	      $"RPTG_NTWK_DESC").distinct
	      



		val ndowrkHoppaProvListDist=ndowrkHoppaInpDfDist.union(ndowrkHoppaOutpDfDist).distinct
			
   
//		val ndowrkHoppaProvListDist = spark.sql(s"select distinct PROV_ST_NM,PROD_LVL3_DESC,FUNDG_LVL2_DESC,MBU_LVL2_DESC,MBU_LVL4_DESC,EXCHNG_IND_CD,MBU_LVL3_DESC,MEDCR_NBR,HOSP_NM,HOSP_SYS_NM,SUBMRKT_DESC,PROV_CNTY_NM,FCLTY_TYPE_DESC,PAR_STTS_CD,PROV_EXCLSN_CD,RPTG_NTWK_DESC from $stagingHiveDB.$tabndowrkHoppaInpDf union select distinct PROV_ST_NM ,PROD_LVL3_DESC,FUNDG_LVL2_DESC,MBU_LVL2_DESC,MBU_LVL4_DESC,EXCHNG_IND_CD,MBU_LVL3_DESC,MEDCR_NBR,HOSP_NM,HOSP_SYS_NM,SUBMRKT_DESC,PROV_CNTY_NM,FCLTY_TYPE_DESC,PAR_STTS_CD,PROV_EXCLSN_CD, RPTG_NTWK_DESC from $stagingHiveDB.$tabndowrkHoppaOutpDf")
		
//		println(s"count of union distinct : " +  ndowrkHoppaProvListDist.count)
		
		val ndowrkHoppaInpDfCms = ndowrkHoppaInpDf.select(
					$"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MEDCR_NBR",
					$"PROV_CNTY_NM",
					$"PAR_STTS_CD",
					$"SUBMRKT_DESC",
		      $"RPTG_NTWK_DESC",
					$"ALWD_AMT_WITH_CMS",
					$"CMS_REIMBMNT_AMT"
					).groupBy(
					    $"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MEDCR_NBR",
					$"PROV_CNTY_NM",
					$"PAR_STTS_CD",
					$"SUBMRKT_DESC",
		      $"RPTG_NTWK_DESC").agg(sum($"ALWD_AMT_WITH_CMS").as("ALWD_AMT_WITH_CMS"),
					                     sum($"CMS_REIMBMNT_AMT").as("CMS_REIMBMNT_AMT"))

			val ndowrkHoppaOutpDfCms = ndowrkHoppaOutpDf.select(
					$"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MEDCR_NBR",
					$"PROV_CNTY_NM",
					$"PAR_STTS_CD",
					$"SUBMRKT_DESC",
					$"RPTG_NTWK_DESC",
					$"ALWD_AMT_WITH_CMS",
					$"CMS_REIMBMNT_AMT"
					).groupBy(
					    $"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MEDCR_NBR",
					$"PROV_CNTY_NM",
					$"PAR_STTS_CD",
					$"SUBMRKT_DESC",
					$"RPTG_NTWK_DESC").agg(sum($"ALWD_AMT_WITH_CMS").as("ALWD_AMT_WITH_CMS"),
					                     sum($"CMS_REIMBMNT_AMT").as("CMS_REIMBMNT_AMT"))

		val ndowrkHoppaInpDf1 = ndowrkHoppaInpDf.alias("ndowrkHoppaInpDf1")
			val ndowrkHoppaInpDf2 = ndowrkHoppaInpDf.alias("ndowrkHoppaInpDf2")
			val ndowrkHoppaInpDf3 = ndowrkHoppaInpDf.alias("ndowrkHoppaInpDf3")
			val ndowrkHoppaInpDf4 = ndowrkHoppaInpDf.alias("ndowrkHoppaInpDf4")
			val ndowrkHoppaInpDf5 = ndowrkHoppaInpDf.alias("ndowrkHoppaInpDf5")
			val ndowrkHoppaOutpDf1 = ndowrkHoppaOutpDf.alias("ndowrkHoppaOutpDf1")
			val ndowrkHoppaOutpDf2 = ndowrkHoppaOutpDf.alias("ndowrkHoppaOutpDf2")
			val ndowrkHoppaOutpDf3 = ndowrkHoppaOutpDf.alias("ndowrkHoppaOutpDf3")
			val ndowrkHoppaOutpDf4 = ndowrkHoppaOutpDf.alias("ndowrkHoppaOutpDf4")
			val ndowrkHoppaOutpDf5 = ndowrkHoppaOutpDf.alias("ndowrkHoppaOutpDf5")
			val ndowrkHoppaOutpDf6 = ndowrkHoppaOutpDf.alias("ndowrkHoppaOutpDf6")
			val ndowrkHoppaOutpDf7 = ndowrkHoppaOutpDf.alias("ndowrkHoppaOutpDf7")
			val ndowrkHoppaOutpDf8 = ndowrkHoppaOutpDf.alias("ndowrkHoppaOutpDf8")

			val joinInpcms = ndowrkHoppaProvListDist.join(ndowrkHoppaInpDfCms,
			    ndowrkHoppaInpDfCms("PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
					ndowrkHoppaInpDfCms("PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
					ndowrkHoppaInpDfCms("FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
					ndowrkHoppaInpDfCms("MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
					ndowrkHoppaInpDfCms("MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
					ndowrkHoppaInpDfCms("EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
					ndowrkHoppaInpDfCms("MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
					ndowrkHoppaInpDfCms("MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
					ndowrkHoppaInpDfCms("PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
					ndowrkHoppaInpDfCms("PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
					ndowrkHoppaInpDfCms("SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC")&&
		      ndowrkHoppaInpDfCms("RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC"), "left_outer")
	
		     
		      
				val joinoutpcms=joinInpcms.join(ndowrkHoppaOutpDfCms,
			    ndowrkHoppaOutpDfCms("PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
					ndowrkHoppaOutpDfCms("PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
					ndowrkHoppaOutpDfCms("FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
					ndowrkHoppaOutpDfCms("MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
					ndowrkHoppaOutpDfCms("MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
					ndowrkHoppaOutpDfCms("EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
					ndowrkHoppaOutpDfCms("MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
					ndowrkHoppaOutpDfCms("MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
					ndowrkHoppaOutpDfCms("PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
					ndowrkHoppaOutpDfCms("PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
					ndowrkHoppaOutpDfCms("SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
					ndowrkHoppaOutpDfCms("RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC"),
					"left_outer")

					
					
			val joinInp1=joinoutpcms.join(ndowrkHoppaInpDf1,
					col("ndowrkHoppaInpDf1.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
					col("ndowrkHoppaInpDf1.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
					col("ndowrkHoppaInpDf1.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
					col("ndowrkHoppaInpDf1.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
					col("ndowrkHoppaInpDf1.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
					col("ndowrkHoppaInpDf1.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
					col("ndowrkHoppaInpDf1.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
					col("ndowrkHoppaInpDf1.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
					col("ndowrkHoppaInpDf1.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
					col("ndowrkHoppaInpDf1.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
					col("ndowrkHoppaInpDf1.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
		      col("ndowrkHoppaInpDf1.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
					//col("ndowrkHoppaInpDf1.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
					col("ndowrkHoppaInpDf1.category") === "MATERNITY", "left_outer")
					
		
					

		val joinInp2 = joinInp1.join(ndowrkHoppaInpDf2, col("ndowrkHoppaInpDf2.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaInpDf2.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaInpDf2.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaInpDf2.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaInpDf2.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaInpDf2.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaInpDf2.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaInpDf2.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaInpDf2.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaInpDf2.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaInpDf2.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
      col("ndowrkHoppaInpDf2.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			//col("ndowrkHoppaInpDf2.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaInpDf2.category") === "NICU", "left_outer")

			val joinInp5 = joinInp2.join(ndowrkHoppaInpDf5, col("ndowrkHoppaInpDf5.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaInpDf5.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaInpDf5.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaInpDf5.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaInpDf5.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaInpDf5.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaInpDf5.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaInpDf5.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaInpDf5.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaInpDf5.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaInpDf5.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
      col("ndowrkHoppaInpDf5.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			//col("ndowrkHoppaInpDf4.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaInpDf5.category") === "TRANSPLANT", "left_outer")		
			
			val joinInp3 = joinInp5.join(ndowrkHoppaInpDf3, col("ndowrkHoppaInpDf3.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaInpDf3.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaInpDf3.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaInpDf3.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaInpDf3.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaInpDf3.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaInpDf3.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaInpDf3.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaInpDf3.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaInpDf3.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaInpDf3.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
      col("ndowrkHoppaInpDf3.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			//col("ndowrkHoppaInpDf3.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaInpDf3.category") === "IPOTH", "left_outer")

			
			
			val joinInp4 = joinInp3.join(ndowrkHoppaInpDf4, col("ndowrkHoppaInpDf4.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaInpDf4.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaInpDf4.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaInpDf4.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaInpDf4.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaInpDf4.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaInpDf4.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaInpDf4.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaInpDf4.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaInpDf4.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaInpDf4.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
      col("ndowrkHoppaInpDf4.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			//col("ndowrkHoppaInpDf4.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaInpDf4.category").isin("IPOTH", "NICU", "MATERNITY","TRANSPLANT"), "left_outer")


			
			
			val joinOut1 = joinInp4.join(ndowrkHoppaOutpDf1, col("ndowrkHoppaOutpDf1.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaOutpDf1.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf1.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf1.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf1.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaOutpDf1.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaOutpDf1.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf1.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaOutpDf1.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaOutpDf1.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			//col("ndowrkHoppaOutpDf1.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
      col("ndowrkHoppaOutpDf1.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			//col("ndowrkHoppaOutpDf1.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaOutpDf1.category") === "ER", "left_outer")

			val joinOut2 = joinOut1.join(ndowrkHoppaOutpDf2, col("ndowrkHoppaOutpDf2.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaOutpDf2.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf2.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf2.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf2.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaOutpDf2.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaOutpDf2.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf2.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaOutpDf2.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaOutpDf2.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			//col("ndowrkHoppaOutpDf2.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
      col("ndowrkHoppaOutpDf2.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			//col("ndowrkHoppaOutpDf2.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaOutpDf2.category") === "ER_SURGERY", "left_outer")

			val joinOut3 = joinOut2.join(ndowrkHoppaOutpDf3, col("ndowrkHoppaOutpDf3.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaOutpDf3.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf3.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf3.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf3.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaOutpDf3.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaOutpDf3.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf3.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaOutpDf3.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaOutpDf3.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			//col("ndowrkHoppaOutpDf3.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
      col("ndowrkHoppaOutpDf3.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			//col("ndowrkHoppaOutpDf3.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaOutpDf3.category") === "SURGERY", "left_outer")

			val joinOut4 = joinOut3.join(ndowrkHoppaOutpDf4, col("ndowrkHoppaOutpDf4.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaOutpDf4.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf4.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf4.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf4.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaOutpDf4.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaOutpDf4.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf4.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaOutpDf4.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaOutpDf4.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			//col("ndowrkHoppaOutpDf4.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
      col("ndowrkHoppaOutpDf4.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			//col("ndowrkHoppaOutpDf4.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaOutpDf4.category") === "LAB", "left_outer")

			val joinOut5 = joinOut4.join(ndowrkHoppaOutpDf5, col("ndowrkHoppaOutpDf5.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaOutpDf5.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf5.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf5.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf5.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaOutpDf5.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaOutpDf5.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf5.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaOutpDf5.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaOutpDf5.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			//col("ndowrkHoppaOutpDf5.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
      col("ndowrkHoppaOutpDf5.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			//col("ndowrkHoppaOutpDf5.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaOutpDf5.category") === "RAD_SCANS", "left_outer")

			val joinOut6 = joinOut5.join(ndowrkHoppaOutpDf6, col("ndowrkHoppaOutpDf6.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaOutpDf6.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf6.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf6.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf6.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaOutpDf6.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaOutpDf6.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf6.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaOutpDf6.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaOutpDf6.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			//col("ndowrkHoppaOutpDf6.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
      col("ndowrkHoppaOutpDf6.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			//col("ndowrkHoppaOutpDf6.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaOutpDf6.category") === "RAD_OTHER", "left_outer")

			val joinOut7 = joinOut6.join(ndowrkHoppaOutpDf7, col("ndowrkHoppaOutpDf7.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaOutpDf7.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf7.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf7.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf7.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaOutpDf7.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaOutpDf7.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf7.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaOutpDf7.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaOutpDf7.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			//col("ndowrkHoppaOutpDf7.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
      col("ndowrkHoppaOutpDf7.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			//col("ndowrkHoppaOutpDf7.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaOutpDf7.category") === "DRUGS", "left_outer")

			val joinOut8 = joinOut7.join(ndowrkHoppaOutpDf8, col("ndowrkHoppaOutpDf8.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaOutpDf8.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf8.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf8.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaOutpDf8.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaOutpDf8.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaOutpDf8.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaOutpDf8.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaOutpDf8.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaOutpDf8.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			//col("ndowrkHoppaOutpDf8.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
      col("ndowrkHoppaOutpDf8.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			//col("ndowrkHoppaOutpDf8.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaOutpDf8.category") === "OPOTH", "left_outer")

			
			
			val joinOut8Sel = joinOut8.select(
					ndowrkHoppaProvListDist("PROV_ST_NM"),
					ndowrkHoppaProvListDist("PROD_LVL3_DESC"),
					ndowrkHoppaProvListDist("FUNDG_LVL2_DESC"),
					ndowrkHoppaProvListDist("MBU_LVL4_DESC"),
					ndowrkHoppaProvListDist("MBU_LVL3_DESC"),
					ndowrkHoppaProvListDist("EXCHNG_IND_CD"),
					ndowrkHoppaProvListDist("MBU_LVL2_DESC"),
					(concat(trim(ndowrkHoppaProvListDist("MEDCR_NBR")), lit(": "), trim(ndowrkHoppaProvListDist("HOSP_NM")))).alias("HOSP_NM"),
					ndowrkHoppaProvListDist("HOSP_SYS_NM"),
					ndowrkHoppaProvListDist("SUBMRKT_DESC"),
					ndowrkHoppaProvListDist("PROV_CNTY_NM"),
					ndowrkHoppaProvListDist("FCLTY_TYPE_DESC"),
					ndowrkHoppaProvListDist("PAR_STTS_CD"),
					ndowrkHoppaProvListDist("PROV_EXCLSN_CD"),
					ndowrkHoppaProvListDist("MEDCR_NBR"),
      		ndowrkHoppaProvListDist("RPTG_NTWK_DESC"),
					
					(when(col("ndowrkHoppaInpDf1.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf1.BILLD_CHRG_AMT"))).alias("MTRNTY_BILLD_AMT"),
					(when(col("ndowrkHoppaInpDf1.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf1.ALWD_AMT"))).alias("MTRNTY_ALLWD_AMT"),
					(when(col("ndowrkHoppaInpDf1.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf1.PAID_AMT"))).alias("MTRNTY_PAID_AMT"),
					(when(col("ndowrkHoppaInpDf1.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf1.BILLD_CHRG_Case_AMT"))).alias("MTRNTY_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaInpDf1.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf1.Alwd_Case_Amt"))).alias("MTRNTY_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaInpDf1.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf1.PAID_Case_AMT"))).alias("MTRNTY_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaInpDf1.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf1.CMAD_ALLOWED"))).alias("MTRNTY_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaInpDf1.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf1.CASE_CNT"))).alias("MTRNTY_CASE_NBR"),
					(when(col("ndowrkHoppaInpDf1.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf1.CMI"))).alias("MTRNTY_CMI_NBR"),
					(when(col("ndowrkHoppaInpDf1.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf1.CMAD_CASE_CNT"))).alias("MTRNTY_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaInpDf1.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf1.CMAD"))).alias("MTRNTY_CMAD_NBR"),
//					(when(col("ndowrkHoppaInpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf1.ALWD_AMT_WITH_CMS"))).alias("MTRNTY_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaInpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf1.CMS_REIMBMNT_AMT"))).alias("MTRNTY_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaInpDf2.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf2.BILLD_CHRG_AMT"))).alias("NICU_BILLD_AMT"),
					(when(col("ndowrkHoppaInpDf2.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf2.ALWD_AMT"))).alias("NICU_ALLWD_AMT"),
					(when(col("ndowrkHoppaInpDf2.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf2.PAID_AMT"))).alias("NICU_PAID_AMT"),
					(when(col("ndowrkHoppaInpDf2.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf2.BILLD_CHRG_Case_AMT"))).alias("NICU_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaInpDf2.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf2.Alwd_Case_Amt"))).alias("NICU_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaInpDf2.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf2.PAID_Case_AMT"))).alias("NICU_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaInpDf2.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf2.CMAD_ALLOWED"))).alias("NICU_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaInpDf2.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf2.CASE_CNT"))).alias("NICU_CASE_NBR"),
					(when(col("ndowrkHoppaInpDf2.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf2.CMI"))).alias("NICU_CMI_NBR"),
					(when(col("ndowrkHoppaInpDf2.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf2.CMAD_CASE_CNT"))).alias("NICU_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaInpDf2.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf2.CMAD"))).alias("NICU_CMAD_NBR"),
//					(when(col("ndowrkHoppaInpDf2.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf2.ALWD_AMT_WITH_CMS"))).alias("NICU_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaInpDf2.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf2.CMS_REIMBMNT_AMT"))).alias("NICU_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaInpDf5.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf5.BILLD_CHRG_AMT"))).alias("TRANSPLANT_BILLD_AMT"),
	        (when(col("ndowrkHoppaInpDf5.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf5.ALWD_AMT"))).alias("TRANSPLANT_ALLWD_AMT"),
	        (when(col("ndowrkHoppaInpDf5.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf5.PAID_AMT"))).alias("TRANSPLANT_PAID_AMT"),
	        (when(col("ndowrkHoppaInpDf5.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf5.BILLD_CHRG_Case_AMT"))).alias("TRANSPLANT_BILLD_CASE_AMT"),
	        (when(col("ndowrkHoppaInpDf5.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf5.Alwd_Case_Amt"))).alias("TRANSPLANT_ALLWD_CASE_AMT"),
	        (when(col("ndowrkHoppaInpDf5.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf5.PAID_Case_AMT"))).alias("TRANSPLANT_PAID_CASE_AMT"),
	        (when(col("ndowrkHoppaInpDf5.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf5.CMAD_ALLOWED"))).alias("TRANSPLANT_ALLWD_CMAD_AMT"),
	        (when(col("ndowrkHoppaInpDf5.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf5.CASE_CNT"))).alias("TRANSPLANT_CASE_NBR"),
	        (when(col("ndowrkHoppaInpDf5.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf5.CMI"))).alias("TRANSPLANT_CMI_NBR"),
	        (when(col("ndowrkHoppaInpDf5.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf5.CMAD_CASE_CNT"))).alias("TRANSPLANT_CMAD_CASE_CNT"),
	        (when(col("ndowrkHoppaInpDf5.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf5.CMAD"))).alias("TRANSPLANT_CMAD_NBR"),
					
					(when(col("ndowrkHoppaInpDf3.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf3.BILLD_CHRG_AMT"))).alias("INPAT_OTH_BILLD_AMT"),
					(when(col("ndowrkHoppaInpDf3.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf3.ALWD_AMT"))).alias("INPAT_OTH_ALLWD_AMT"),
					(when(col("ndowrkHoppaInpDf3.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf3.PAID_AMT"))).alias("INPAT_OTH_PAID_AMT"),
					(when(col("ndowrkHoppaInpDf3.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf3.BILLD_CHRG_Case_AMT"))).alias("INPAT_OTH_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaInpDf3.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf3.Alwd_Case_Amt"))).alias("INPAT_OTH_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaInpDf3.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf3.PAID_Case_AMT"))).alias("INPAT_OTH_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaInpDf3.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf3.CMAD_ALLOWED"))).alias("INPAT_OTH_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaInpDf3.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf3.CASE_CNT"))).alias("INPAT_OTH_CASE_NBR"),
					(when(col("ndowrkHoppaInpDf3.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf3.CMI"))).alias("INPAT_OTH_CMI_NBR"),
					(when(col("ndowrkHoppaInpDf3.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf3.CMAD_CASE_CNT"))).alias("INPAT_OTH_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaInpDf3.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf3.CMAD"))).alias("INPAT_OTH_CMAD_NBR"),
//					(when(col("ndowrkHoppaInpDf3.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf3.ALWD_AMT_WITH_CMS"))).alias("INPAT_OTH_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaInpDf3.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaInpDf3.CMS_REIMBMNT_AMT"))).alias("INPAT_OTH_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaOutpDf1.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf1.BILLD_CHRG_AMT"))).alias("ER_BILLD_AMT"),
					(when(col("ndowrkHoppaOutpDf1.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf1.ALWD_AMT"))).alias("ER_ALLWD_AMT"),
					(when(col("ndowrkHoppaOutpDf1.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf1.PAID_AMT"))).alias("ER_PAID_AMT"),
					(when(col("ndowrkHoppaOutpDf1.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf1.BILLD_CHRG_Case_AMT"))).alias("ER_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf1.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf1.Alwd_Case_Amt"))).alias("ER_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf1.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf1.PAID_Case_AMT"))).alias("ER_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf1.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf1.CMAD_ALLOWED"))).alias("ER_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaOutpDf1.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf1.CASE_CNT"))).alias("ER_CASE_NBR"),
					(when(col("ndowrkHoppaOutpDf1.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf1.CMI"))).alias("ER_CMI_NBR"),
					(when(col("ndowrkHoppaOutpDf1.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf1.CMAD_CASE_CNT"))).alias("ER_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaOutpDf1.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf1.CMAD"))).alias("ER_CMAD_NBR"),
//					(when(col("ndowrkHoppaOutpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf1.ALWD_AMT_WITH_CMS"))).alias("ER_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaOutpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf1.CMS_REIMBMNT_AMT"))).alias("ER_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaOutpDf2.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf2.BILLD_CHRG_AMT"))).alias("SRGY_ER_BILLD_AMT"),
					(when(col("ndowrkHoppaOutpDf2.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf2.ALWD_AMT"))).alias("SRGY_ER_ALLWD_AMT"),
					(when(col("ndowrkHoppaOutpDf2.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf2.PAID_AMT"))).alias("SRGY_ER_PAID_AMT"),
					(when(col("ndowrkHoppaOutpDf2.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf2.BILLD_CHRG_Case_AMT"))).alias("SRGY_ER_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf2.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf2.Alwd_Case_Amt"))).alias("SRGY_ER_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf2.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf2.PAID_Case_AMT"))).alias("SRGY_ER_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf2.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf2.CMAD_ALLOWED"))).alias("SRGY_ER_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaOutpDf2.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf2.CASE_CNT"))).alias("SRGY_ER_CASE_NBR"),
					(when(col("ndowrkHoppaOutpDf2.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf2.CMI"))).alias("SRGY_ER_CMI_NBR"),
					(when(col("ndowrkHoppaOutpDf2.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf2.CMAD_CASE_CNT"))).alias("SRGY_ER_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaOutpDf2.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf2.CMAD"))).alias("SRGY_ER_CMAD_NBR"),
//					(when(col("ndowrkHoppaOutpDf2.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf2.ALWD_AMT_WITH_CMS"))).alias("SRGY_ER_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaOutpDf2.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf2.CMS_REIMBMNT_AMT"))).alias("SRGY_ER_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaOutpDf3.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf3.BILLD_CHRG_AMT"))).alias("SRGY_BILLD_AMT"),
					(when(col("ndowrkHoppaOutpDf3.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf3.ALWD_AMT"))).alias("SRGY_ALLWD_AMT"),
					(when(col("ndowrkHoppaOutpDf3.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf3.PAID_AMT"))).alias("SRGY_PAID_AMT"),
					(when(col("ndowrkHoppaOutpDf3.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf3.BILLD_CHRG_Case_AMT"))).alias("SRGY_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf3.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf3.Alwd_Case_Amt"))).alias("SRGY_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf3.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf3.PAID_Case_AMT"))).alias("SRGY_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf3.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf3.CMAD_ALLOWED"))).alias("SRGY_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaOutpDf3.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf3.CASE_CNT"))).alias("SRGY_CASE_NBR"),
					(when(col("ndowrkHoppaOutpDf3.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf3.CMI"))).alias("SRGY_CMI_NBR"),
					(when(col("ndowrkHoppaOutpDf3.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf3.CMAD_CASE_CNT"))).alias("SRGY_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaOutpDf3.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf3.CMAD"))).alias("SRGY_CMAD_NBR"),
//					(when(col("ndowrkHoppaOutpDf3.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf3.ALWD_AMT_WITH_CMS"))).alias("SRGY_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaOutpDf3.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf3.CMS_REIMBMNT_AMT"))).alias("SRGY_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaOutpDf4.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf4.BILLD_CHRG_AMT"))).alias("LAB_BILLD_AMT"),
					(when(col("ndowrkHoppaOutpDf4.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf4.ALWD_AMT"))).alias("LAB_ALLWD_AMT"),
					(when(col("ndowrkHoppaOutpDf4.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf4.PAID_AMT"))).alias("LAB_PAID_AMT"),
					(when(col("ndowrkHoppaOutpDf4.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf4.BILLD_CHRG_Case_AMT"))).alias("LAB_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf4.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf4.Alwd_Case_Amt"))).alias("LAB_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf4.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf4.PAID_Case_AMT"))).alias("LAB_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf4.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf4.CMAD_ALLOWED"))).alias("LAB_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaOutpDf4.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf4.CASE_CNT"))).alias("LAB_CASE_NBR"),
					(when(col("ndowrkHoppaOutpDf4.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf4.CMI"))).alias("LAB_CMI_NBR"),
					(when(col("ndowrkHoppaOutpDf4.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf4.CMAD_CASE_CNT"))).alias("LAB_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaOutpDf4.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf4.CMAD"))).alias("LAB_CMAD_NBR"),
//					(when(col("ndowrkHoppaOutpDf4.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf4.ALWD_AMT_WITH_CMS"))).alias("LAB_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaOutpDf4.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf4.CMS_REIMBMNT_AMT"))).alias("LAB_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaOutpDf5.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf5.BILLD_CHRG_AMT"))).alias("RDLGY_SCANS_BILLD_AMT"),
					(when(col("ndowrkHoppaOutpDf5.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf5.ALWD_AMT"))).alias("RDLGY_SCANS_ALLWD_AMT"),
					(when(col("ndowrkHoppaOutpDf5.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf5.PAID_AMT"))).alias("RDLGY_SCANS_PAID_AMT"),
					(when(col("ndowrkHoppaOutpDf5.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf5.BILLD_CHRG_Case_AMT"))).alias("RDLGY_SCANS_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf5.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf5.Alwd_Case_Amt"))).alias("RDLGY_SCANS_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf5.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf5.PAID_Case_AMT"))).alias("RDLGY_SCANS_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf5.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf5.CMAD_ALLOWED"))).alias("RDLGY_SCANS_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaOutpDf5.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf5.CASE_CNT"))).alias("RDLGY_SCANS_CASE_NBR"),
					(when(col("ndowrkHoppaOutpDf5.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf5.CMI"))).alias("RDLGY_SCANS_CMI_NBR"),
					(when(col("ndowrkHoppaOutpDf5.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf5.CMAD_CASE_CNT"))).alias("RDLGY_SCANS_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaOutpDf5.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf5.CMAD"))).alias("RDLGY_SCANS_CMAD_NBR"),
//					(when(col("ndowrkHoppaOutpDf5.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf5.ALWD_AMT_WITH_CMS"))).alias("RDLGY_SCANS_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaOutpDf5.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf5.CMS_REIMBMNT_AMT"))).alias("RDLGY_SCANS_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaOutpDf6.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf6.BILLD_CHRG_AMT"))).alias("RDLGY_OTHR_BILLD_AMT"),
					(when(col("ndowrkHoppaOutpDf6.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf6.ALWD_AMT"))).alias("RDLGY_OTHR_ALLWD_AMT"),
					(when(col("ndowrkHoppaOutpDf6.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf6.PAID_AMT"))).alias("RDLGY_OTHR_PAID_AMT"),
					(when(col("ndowrkHoppaOutpDf6.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf6.BILLD_CHRG_Case_AMT"))).alias("RDLGY_OTHR_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf6.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf6.Alwd_Case_Amt"))).alias("RDLGY_OTHR_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf6.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf6.PAID_Case_AMT"))).alias("RDLGY_OTHR_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf6.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf6.CMAD_ALLOWED"))).alias("RDLGY_OTHR_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaOutpDf6.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf6.CASE_CNT"))).alias("RDLGY_OTHR_CASE_NBR"),
					(when(col("ndowrkHoppaOutpDf6.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf6.CMI"))).alias("RDLGY_OTHR_CMI_NBR"),
					(when(col("ndowrkHoppaOutpDf6.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf6.CMAD_CASE_CNT"))).alias("RDLGY_OTHR_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaOutpDf6.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf6.CMAD"))).alias("RDLGY_OTHR_CMAD_NBR"),
//					(when(col("ndowrkHoppaOutpDf6.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf6.ALWD_AMT_WITH_CMS"))).alias("RDLGY_OTHR_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaOutpDf6.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf6.CMS_REIMBMNT_AMT"))).alias("RDLGY_OTHR_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaOutpDf7.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf7.BILLD_CHRG_AMT"))).alias("DRUG_BILLD_AMT"),
					(when(col("ndowrkHoppaOutpDf7.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf7.ALWD_AMT"))).alias("DRUG_ALLWD_AMT"),
					(when(col("ndowrkHoppaOutpDf7.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf7.PAID_AMT"))).alias("DRUG_PAID_AMT"),
					(when(col("ndowrkHoppaOutpDf7.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf7.BILLD_CHRG_Case_AMT"))).alias("DRUG_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf7.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf7.Alwd_Case_Amt"))).alias("DRUG_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf7.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf7.PAID_Case_AMT"))).alias("DRUG_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf7.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf7.CMAD_ALLOWED"))).alias("DRUG_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaOutpDf7.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf7.CASE_CNT"))).alias("DRUG_CASE_NBR"),
					(when(col("ndowrkHoppaOutpDf7.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf7.CMI"))).alias("DRUG_CMI_NBR"),
					(when(col("ndowrkHoppaOutpDf7.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf7.CMAD_CASE_CNT"))).alias("DRUG_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaOutpDf7.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf7.CMAD"))).alias("DRUG_CMAD_NBR"),
//					(when(col("ndowrkHoppaOutpDf7.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf7.ALWD_AMT_WITH_CMS"))).alias("DRUG_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaOutpDf7.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf7.CMS_REIMBMNT_AMT"))).alias("DRUG_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaOutpDf8.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf8.BILLD_CHRG_AMT"))).alias("OUTPAT_OTHR_BILLD_AMT"),
					(when(col("ndowrkHoppaOutpDf8.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf8.ALWD_AMT"))).alias("OUTPAT_OTHR_ALLWD_AMT"),
					(when(col("ndowrkHoppaOutpDf8.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf8.PAID_AMT"))).alias("OUTPAT_OTHR_PAID_AMT"),
					(when(col("ndowrkHoppaOutpDf8.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf8.BILLD_CHRG_Case_AMT"))).alias("OUTPAT_OTHR_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf8.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf8.Alwd_Case_Amt"))).alias("OUTPAT_OTHR_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf8.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf8.PAID_Case_AMT"))).alias("OUTPAT_OTHR_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaOutpDf8.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf8.CMAD_ALLOWED"))).alias("OUTPAT_OTHR_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaOutpDf8.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf8.CASE_CNT"))).alias("OUTPAT_OTHR_CASE_NBR"),
					(when(col("ndowrkHoppaOutpDf8.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf8.CMI"))).alias("OUTPAT_OTHR_CMI_NBR"),
					(when(col("ndowrkHoppaOutpDf8.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf8.CMAD_CASE_CNT"))).alias("OUTPAT_OTHR_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaOutpDf8.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf8.CMAD"))).alias("OUTPAT_OTHR_CMAD_NBR"),
//					(when(col("ndowrkHoppaOutpDf8.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf8.ALWD_AMT_WITH_CMS"))).alias("OUTPAT_OTHR_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaOutpDf8.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaOutpDf8.CMS_REIMBMNT_AMT"))).alias("OUTPAT_OTHR_CMS_REIMBMNT_AMT"),

					col("ndowrkHoppaInpDf4.case_cnt"),
					col("ndowrkHoppaInpDf4.ER_Case_Cnt"),
					(when(ndowrkHoppaInpDfCms("ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (ndowrkHoppaInpDfCms("ALWD_AMT_WITH_CMS"))).alias("INP_ALWD_AMT_WITH_CMS"),
					(when(ndowrkHoppaOutpDfCms("ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (ndowrkHoppaOutpDfCms("ALWD_AMT_WITH_CMS"))).alias("OUTP_ALWD_AMT_WITH_CMS"),
					(when(ndowrkHoppaInpDfCms("CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (ndowrkHoppaInpDfCms("CMS_REIMBMNT_AMT"))).alias("INP_CMS_REIMBMNT_AMT"),
					(when(ndowrkHoppaOutpDfCms("CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (ndowrkHoppaOutpDfCms("CMS_REIMBMNT_AMT"))).alias("OUTP_CMS_REIMBMNT_AMT")
					).groupBy(
							ndowrkHoppaProvListDist("PROV_ST_NM"),
							ndowrkHoppaProvListDist("PROD_LVL3_DESC"),
							ndowrkHoppaProvListDist("FUNDG_LVL2_DESC"),
							ndowrkHoppaProvListDist("MBU_LVL4_DESC"),
							ndowrkHoppaProvListDist("MBU_LVL3_DESC"),
							ndowrkHoppaProvListDist("EXCHNG_IND_CD"),
							ndowrkHoppaProvListDist("MBU_LVL2_DESC"),
							$"HOSP_NM",
							ndowrkHoppaProvListDist("HOSP_SYS_NM"),
							ndowrkHoppaProvListDist("SUBMRKT_DESC"),
							ndowrkHoppaProvListDist("PROV_CNTY_NM"),
							ndowrkHoppaProvListDist("FCLTY_TYPE_DESC"),
							ndowrkHoppaProvListDist("PAR_STTS_CD"),
							ndowrkHoppaProvListDist("PROV_EXCLSN_CD"),
							ndowrkHoppaProvListDist("MEDCR_NBR"), 
      				ndowrkHoppaProvListDist("RPTG_NTWK_DESC"),
							$"MTRNTY_BILLD_AMT",
							$"MTRNTY_ALLWD_AMT",
							$"MTRNTY_PAID_AMT",
							$"MTRNTY_BILLD_CASE_AMT",
							$"MTRNTY_ALLWD_CASE_AMT",
							$"MTRNTY_PAID_CASE_AMT",
							$"MTRNTY_ALLWD_CMAD_AMT",
							$"MTRNTY_CASE_NBR",
							$"MTRNTY_CMI_NBR",
							$"MTRNTY_CMAD_CASE_CNT",
							$"MTRNTY_CMAD_NBR",
//							$"MTRNTY_ALWD_AMT_WITH_CMS",
//              $"MTRNTY_CMS_REIMBMNT_AMT",


							$"NICU_BILLD_AMT",
							$"NICU_ALLWD_AMT",
							$"NICU_PAID_AMT",
							$"NICU_BILLD_CASE_AMT",
							$"NICU_ALLWD_CASE_AMT",
							$"NICU_PAID_CASE_AMT",
							$"NICU_ALLWD_CMAD_AMT",
							$"NICU_CASE_NBR",
							$"NICU_CMI_NBR",
							$"NICU_CMAD_CASE_CNT",
							$"NICU_CMAD_NBR",
//							$"NICU_ALWD_AMT_WITH_CMS",
//              $"NICU_CMS_REIMBMNT_AMT",
 
							$"TRANSPLANT_BILLD_AMT",
							$"TRANSPLANT_ALLWD_AMT",
							$"TRANSPLANT_PAID_AMT",
							$"TRANSPLANT_BILLD_CASE_AMT",
							$"TRANSPLANT_ALLWD_CASE_AMT",
							$"TRANSPLANT_PAID_CASE_AMT",
							$"TRANSPLANT_ALLWD_CMAD_AMT",
							$"TRANSPLANT_CASE_NBR",
							$"TRANSPLANT_CMI_NBR",
							$"TRANSPLANT_CMAD_CASE_CNT",
							$"TRANSPLANT_CMAD_NBR",
							
							$"INPAT_OTH_BILLD_AMT",
							$"INPAT_OTH_ALLWD_AMT",
							$"INPAT_OTH_PAID_AMT",
							$"INPAT_OTH_BILLD_CASE_AMT",
							$"INPAT_OTH_ALLWD_CASE_AMT",
							$"INPAT_OTH_PAID_CASE_AMT",
							$"INPAT_OTH_ALLWD_CMAD_AMT",
							$"INPAT_OTH_CASE_NBR",
							$"INPAT_OTH_CMI_NBR",
							$"INPAT_OTH_CMAD_CASE_CNT",
							$"INPAT_OTH_CMAD_NBR",
//							$"INPAT_OTH_ALWD_AMT_WITH_CMS",
//							$"INPAT_OTH_CMS_REIMBMNT_AMT",
							
							$"ER_BILLD_AMT",
							$"ER_ALLWD_AMT",
							$"ER_PAID_AMT",
							$"ER_BILLD_CASE_AMT",
							$"ER_ALLWD_CASE_AMT",
							$"ER_PAID_CASE_AMT",
							$"ER_ALLWD_CMAD_AMT",
							$"ER_CASE_NBR",
							$"ER_CMI_NBR",
							$"ER_CMAD_CASE_CNT",
							$"ER_CMAD_NBR",
//							$"ER_ALWD_AMT_WITH_CMS",
//							$"ER_CMS_REIMBMNT_AMT",

							$"SRGY_ER_BILLD_AMT",
							$"SRGY_ER_ALLWD_AMT",
							$"SRGY_ER_PAID_AMT",
							$"SRGY_ER_BILLD_CASE_AMT",
							$"SRGY_ER_ALLWD_CASE_AMT",
							$"SRGY_ER_PAID_CASE_AMT",
							$"SRGY_ER_ALLWD_CMAD_AMT",
							$"SRGY_ER_CASE_NBR",
							$"SRGY_ER_CMI_NBR",
							$"SRGY_ER_CMAD_CASE_CNT",
							$"SRGY_ER_CMAD_NBR",
//							$"SRGY_ER_ALWD_AMT_WITH_CMS",
//							$"SRGY_ER_CMS_REIMBMNT_AMT",

							$"SRGY_BILLD_AMT",
							$"SRGY_ALLWD_AMT",
							$"SRGY_PAID_AMT",
							$"SRGY_BILLD_CASE_AMT",
							$"SRGY_ALLWD_CASE_AMT",
							$"SRGY_PAID_CASE_AMT",
							$"SRGY_ALLWD_CMAD_AMT",
							$"SRGY_CASE_NBR",
							$"SRGY_CMI_NBR",
							$"SRGY_CMAD_CASE_CNT",
							$"SRGY_CMAD_NBR",
//							$"SRGY_ALWD_AMT_WITH_CMS",
//							$"SRGY_CMS_REIMBMNT_AMT",

							$"LAB_BILLD_AMT",
							$"LAB_ALLWD_AMT",
							$"LAB_PAID_AMT",
							$"LAB_BILLD_CASE_AMT",
							$"LAB_ALLWD_CASE_AMT",
							$"LAB_PAID_CASE_AMT",
							$"LAB_ALLWD_CMAD_AMT",
							$"LAB_CASE_NBR",
							$"LAB_CMI_NBR",
							$"LAB_CMAD_CASE_CNT",
							$"LAB_CMAD_NBR",					
//              $"LAB_ALWD_AMT_WITH_CMS",
//              $"LAB_CMS_REIMBMNT_AMT",

							$"RDLGY_SCANS_BILLD_AMT",
							$"RDLGY_SCANS_ALLWD_AMT",
							$"RDLGY_SCANS_PAID_AMT",
							$"RDLGY_SCANS_BILLD_CASE_AMT",
							$"RDLGY_SCANS_ALLWD_CASE_AMT",
							$"RDLGY_SCANS_PAID_CASE_AMT",
							$"RDLGY_SCANS_ALLWD_CMAD_AMT",
							$"RDLGY_SCANS_CASE_NBR",
							$"RDLGY_SCANS_CMI_NBR",
							$"RDLGY_SCANS_CMAD_CASE_CNT",
							$"RDLGY_SCANS_CMAD_NBR",
//							$"RDLGY_SCANS_ALWD_AMT_WITH_CMS",
//              $"RDLGY_SCANS_CMS_REIMBMNT_AMT",
						

							$"RDLGY_OTHR_BILLD_AMT",
							$"RDLGY_OTHR_ALLWD_AMT",
							$"RDLGY_OTHR_PAID_AMT",
							$"RDLGY_OTHR_BILLD_CASE_AMT",
							$"RDLGY_OTHR_ALLWD_CASE_AMT",
							$"RDLGY_OTHR_PAID_CASE_AMT",
							$"RDLGY_OTHR_ALLWD_CMAD_AMT",
							$"RDLGY_OTHR_CASE_NBR",
							$"RDLGY_OTHR_CMI_NBR",
							$"RDLGY_OTHR_CMAD_CASE_CNT",
							$"RDLGY_OTHR_CMAD_NBR",
//							$"RDLGY_OTHR_ALWD_AMT_WITH_CMS",
//							$"RDLGY_OTHR_CMS_REIMBMNT_AMT",

							$"DRUG_BILLD_AMT",
							$"DRUG_ALLWD_AMT",
							$"DRUG_PAID_AMT",
							$"DRUG_BILLD_CASE_AMT",
							$"DRUG_ALLWD_CASE_AMT",
							$"DRUG_PAID_CASE_AMT",
							$"DRUG_ALLWD_CMAD_AMT",
							$"DRUG_CASE_NBR",
							$"DRUG_CMI_NBR",
							$"DRUG_CMAD_CASE_CNT",
							$"DRUG_CMAD_NBR",
//							$"DRUG_ALWD_AMT_WITH_CMS",
//							$"DRUG_CMS_REIMBMNT_AMT",

							$"OUTPAT_OTHR_BILLD_AMT",
							$"OUTPAT_OTHR_ALLWD_AMT",
							$"OUTPAT_OTHR_PAID_AMT",
							$"OUTPAT_OTHR_BILLD_CASE_AMT",
							$"OUTPAT_OTHR_ALLWD_CASE_AMT",
							$"OUTPAT_OTHR_PAID_CASE_AMT",
							$"OUTPAT_OTHR_ALLWD_CMAD_AMT",
							$"OUTPAT_OTHR_CASE_NBR",
							$"OUTPAT_OTHR_CMI_NBR",
							$"OUTPAT_OTHR_CMAD_CASE_CNT",
							$"OUTPAT_OTHR_CMAD_NBR",
//							$"OUTPAT_OTHR_ALWD_AMT_WITH_CMS",
//							$"OUTPAT_OTHR_CMS_REIMBMNT_AMT"
							$"INP_ALWD_AMT_WITH_CMS",
							$"OUTP_ALWD_AMT_WITH_CMS",
							$"INP_CMS_REIMBMNT_AMT",
							$"OUTP_CMS_REIMBMNT_AMT"
							).agg(
									(when(sum(col("ndowrkHoppaInpDf4.case_cnt")) > 0, ((sum(col("ndowrkHoppaInpDf4.ER_Case_Cnt")).cast(DecimalType(18, 4)) / sum(col("ndowrkHoppaInpDf4.case_cnt")).cast(DecimalType(18, 4))) * 100).cast(DecimalType(18, 4)))
											otherwise (lit(0))).alias("ER_PCT"))
			.withColumn("INPAT_ALLWD_AMT", ($"MTRNTY_ALLWD_AMT" + $"NICU_ALLWD_AMT" + $"INPAT_OTH_ALLWD_AMT" + $"TRANSPLANT_ALLWD_AMT"))
			.withColumn("OUTPAT_ALLWD_AMT", ($"ER_ALLWD_AMT" + $"SRGY_ER_ALLWD_AMT" + $"SRGY_ALLWD_AMT" + $"LAB_ALLWD_AMT" + $"RDLGY_SCANS_ALLWD_AMT" + $"RDLGY_OTHR_ALLWD_AMT" + $"DRUG_ALLWD_AMT" + $"OUTPAT_OTHR_ALLWD_AMT"))
			.withColumn("TOTAL_ALLWD_AMT", ($"INPAT_ALLWD_AMT" + $"OUTPAT_ALLWD_AMT"))
			.withColumn("TOTAL_ALWD_AMT_WITH_CMS", ($"INP_ALWD_AMT_WITH_CMS".cast(DecimalType(18, 2)) + $"OUTP_ALWD_AMT_WITH_CMS".cast(DecimalType(18, 2))))
			.withColumn("TOTAL_CMS_REIMBMNT_AMT", ($"INP_CMS_REIMBMNT_AMT".cast(DecimalType(18, 2)) + $"OUTP_CMS_REIMBMNT_AMT".cast(DecimalType(18, 2))))
			.withColumn("MTRNTY_PCT_CMAD_NBR", (when($"MTRNTY_CASE_NBR" > 0, (($"MTRNTY_CMAD_CASE_CNT".cast(DecimalType(18, 4)) / $"MTRNTY_CASE_NBR".cast(DecimalType(18, 4))) * 100).cast(DecimalType(18, 4))) otherwise (lit(0))))
			.withColumn("NICU_PCT_CMAD_NBR", (when($"NICU_CASE_NBR" > 0, (($"NICU_CMAD_CASE_CNT".cast(DecimalType(18, 4)) / $"NICU_CASE_NBR".cast(DecimalType(18, 4))) * 100).cast(DecimalType(18, 4))) otherwise (lit(0))))
			.withColumn("TRANSPLANT_PCT_CMAD_NBR", (when($"TRANSPLANT_CASE_NBR" > 0, (($"TRANSPLANT_CMAD_CASE_CNT".cast(DecimalType(18, 4)) / $"TRANSPLANT_CASE_NBR".cast(DecimalType(18, 4))) * 100).cast(DecimalType(18, 4))) otherwise (lit(0))))
			.withColumn("ER_PCT_CMAD_NBR", (when($"ER_CASE_NBR" > 0, (($"ER_CMAD_CASE_CNT".cast(DecimalType(18, 4)) / $"ER_CASE_NBR".cast(DecimalType(18, 4))) * 100).cast(DecimalType(18, 4))) otherwise (lit(0))))
			.withColumn("SRGY_ER_PCT_CMAD_NBR", (when($"SRGY_ER_CASE_NBR" > 0, (($"SRGY_ER_CMAD_CASE_CNT".cast(DecimalType(18, 4)) / $"SRGY_ER_CASE_NBR".cast(DecimalType(18, 4))) * 100).cast(DecimalType(18, 4))) otherwise (lit(0))))
			.withColumn("SRGY_PCT_CMAD_NBR", (when($"SRGY_CASE_NBR" > 0, (($"SRGY_CMAD_CASE_CNT".cast(DecimalType(18, 4)) / $"SRGY_CASE_NBR".cast(DecimalType(18, 4))) * 100).cast(DecimalType(18, 4))) otherwise (lit(0))))
			.withColumn("LAB_PCT_CMAD_NBR", (when($"LAB_CASE_NBR" > 0, (($"LAB_CMAD_CASE_CNT".cast(DecimalType(18, 4)) / $"LAB_CASE_NBR".cast(DecimalType(18, 4))) * 100).cast(DecimalType(18, 4))) otherwise (lit(0))))
			.withColumn("RDLGY_SCANS_PCT_CMAD_NBR", (when($"RDLGY_SCANS_CASE_NBR" > 0, (($"RDLGY_SCANS_CMAD_CASE_CNT".cast(DecimalType(18, 4)) / $"RDLGY_SCANS_CASE_NBR".cast(DecimalType(18, 4))) * 100).cast(DecimalType(18, 4))) otherwise (lit(0))))
			.withColumn("RDLGY_OTHR_PCT_CMAD_NBR", (when($"RDLGY_OTHR_CASE_NBR" > 0, (($"RDLGY_OTHR_CMAD_CASE_CNT".cast(DecimalType(18, 4)) / $"RDLGY_OTHR_CASE_NBR".cast(DecimalType(18, 4))) * 100).cast(DecimalType(18, 4))) otherwise (lit(0))))
			.withColumn("DRUG_PCT_CMAD_NBR", (when($"DRUG_CASE_NBR" > 0, (($"DRUG_CMAD_CASE_CNT".cast(DecimalType(18, 4)) / $"DRUG_CASE_NBR".cast(DecimalType(18, 4))) * 100).cast(DecimalType(18, 4))) otherwise (lit(0))))
			.drop($"INP_ALWD_AMT_WITH_CMS")
			.drop($"OUTP_ALWD_AMT_WITH_CMS")
			.drop($"INP_CMS_REIMBMNT_AMT")
			.drop($"OUTP_CMS_REIMBMNT_AMT")
			
			
			
			val joinOut8SelFil = joinOut8Sel.filter($"TOTAL_ALLWD_AMT" > 0)
			
			

			joinOut8SelFil
	}
}

case class ndoEhoppaBase(ndowrkEhoppaBaseDf: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			ndowrkEhoppaBaseDf.select(
					(when($"PROV_ST_NM".isNull, lit("NA")) otherwise ($"PROV_ST_NM")).alias("PROV_ST_CD"),
					(when($"PROD_LVL3_DESC".isNull, lit("NA")) otherwise ($"PROD_LVL3_DESC")).alias("PROD_LVL3_DESC"),
					(when($"FUNDG_LVL2_DESC".isNull, lit("NA")) otherwise ($"FUNDG_LVL2_DESC")).alias("FUNDG_LVL2_DESC"),
					(when($"MBU_LVL4_DESC".isNull, lit("NA")) otherwise ($"MBU_LVL4_DESC")).alias("MBU_LVL4_DESC"),
					(when($"MBU_LVL3_DESC".isNull, lit("NA")) otherwise ($"MBU_LVL3_DESC")).alias("MBU_LVL3_DESC"),
					(when($"EXCHNG_IND_CD".isNull, lit("NA")) otherwise ($"EXCHNG_IND_CD")).alias("EXCHNG_IND_CD"),
					(when($"MBU_LVL2_DESC".isNull, lit("NA")) otherwise ($"MBU_LVL2_DESC")).alias("MBU_LVL2_DESC"),
					(when($"HOSP_NM".isNull, lit("NA")) otherwise ($"HOSP_NM")).alias("HOSP_NM"),
					(when($"HOSP_SYS_NM".isNull, lit("NA")) otherwise ($"HOSP_SYS_NM")).alias("HOSP_SYS_NM"),
					(when($"SUBMRKT_DESC".isNull, lit("NA")) otherwise ($"SUBMRKT_DESC")).alias("SUBMRKT_DESC"),
					(when($"PROV_CNTY_NM".isNull, lit("NA")) otherwise ($"PROV_CNTY_NM")).alias("PROV_CNTY_NM"),
					(when($"FCLTY_TYPE_DESC".isNull, lit("NA")) otherwise ($"FCLTY_TYPE_DESC")).alias("FCLTY_TYPE_DESC"),
					(when($"PAR_STTS_CD".isNull, lit("NA")) otherwise ($"PAR_STTS_CD")).alias("PAR_STTS_CD"),

          (when($"RPTG_NTWK_DESC".isNull, lit("NA")) otherwise ($"RPTG_NTWK_DESC")).alias("RPTG_NTWK_DESC"),
					(when($"MTRNTY_BILLD_AMT".isNull, lit(0)) otherwise ($"MTRNTY_BILLD_AMT")).alias("MTRNTY_BILLD_AMT"),
					(when($"MTRNTY_ALLWD_AMT".isNull, lit(0)) otherwise ($"MTRNTY_ALLWD_AMT")).alias("MTRNTY_ALLWD_AMT"),
					(when($"MTRNTY_PAID_AMT".isNull, lit(0)) otherwise ($"MTRNTY_PAID_AMT")).alias("MTRNTY_PAID_AMT"),
					(when($"MTRNTY_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"MTRNTY_BILLD_CASE_AMT")).alias("MTRNTY_BILLD_CASE_AMT"),
					(when($"MTRNTY_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"MTRNTY_ALLWD_CASE_AMT")).alias("MTRNTY_ALLWD_CASE_AMT"),
					(when($"MTRNTY_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"MTRNTY_PAID_CASE_AMT")).alias("MTRNTY_PAID_CASE_AMT"),
					(when($"MTRNTY_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"MTRNTY_ALLWD_CMAD_AMT")).alias("MTRNTY_ALLWD_CMAD_AMT"),
					(when($"MTRNTY_CASE_NBR".isNull, lit(0)) otherwise ($"MTRNTY_CASE_NBR")).alias("MTRNTY_CASE_NBR"),
					(when($"MTRNTY_CMI_NBR".isNull, lit(0)) otherwise ($"MTRNTY_CMI_NBR")).alias("MTRNTY_CMI_NBR"),
//					(when($"MTRNTY_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"MTRNTY_ALWD_AMT_WITH_CMS")).alias("MTRNTY_ALWD_AMT_WITH_CMS"),
//					(when($"MTRNTY_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"MTRNTY_CMS_REIMBMNT_AMT")).alias("MTRNTY_CMS_REIMBMNT_AMT"),
					
					(when($"NICU_BILLD_AMT".isNull, lit(0)) otherwise ($"NICU_BILLD_AMT")).alias("NICU_BILLD_AMT"),
					(when($"NICU_ALLWD_AMT".isNull, lit(0)) otherwise ($"NICU_ALLWD_AMT")).alias("NICU_ALLWD_AMT"),
					(when($"NICU_PAID_AMT".isNull, lit(0)) otherwise ($"NICU_PAID_AMT")).alias("NICU_PAID_AMT"),
					(when($"NICU_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"NICU_BILLD_CASE_AMT")).alias("NICU_BILLD_CASE_AMT"),
					(when($"NICU_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"NICU_ALLWD_CASE_AMT")).alias("NICU_ALLWD_CASE_AMT"),
					(when($"NICU_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"NICU_PAID_CASE_AMT")).alias("NICU_PAID_CASE_AMT"),
					(when($"NICU_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"NICU_ALLWD_CMAD_AMT")).alias("NICU_ALLWD_CMAD_AMT"),
					(when($"NICU_CASE_NBR".isNull, lit(0)) otherwise ($"NICU_CASE_NBR")).alias("NICU_CASE_NBR"),
					(when($"NICU_CMI_NBR".isNull, lit(0)) otherwise ($"NICU_CMI_NBR")).alias("NICU_CMI_NBR"),
//					(when($"NICU_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"NICU_ALWD_AMT_WITH_CMS")).alias("NICU_ALWD_AMT_WITH_CMS"),
//					(when($"NICU_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"NICU_CMS_REIMBMNT_AMT")).alias("NICU_CMS_REIMBMNT_AMT"),
	
					
					(when($"TRANSPLANT_BILLD_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_BILLD_AMT")).alias("TRANSPLANT_BILLD_AMT"),
					(when($"TRANSPLANT_ALLWD_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_ALLWD_AMT")).alias("TRANSPLANT_ALLWD_AMT"),
					(when($"TRANSPLANT_PAID_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_PAID_AMT")).alias("TRANSPLANT_PAID_AMT"),
					(when($"TRANSPLANT_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_BILLD_CASE_AMT")).alias("TRANSPLANT_BILLD_CASE_AMT"),
					(when($"TRANSPLANT_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_ALLWD_CASE_AMT")).alias("TRANSPLANT_ALLWD_CASE_AMT"),
					(when($"TRANSPLANT_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_PAID_CASE_AMT")).alias("TRANSPLANT_PAID_CASE_AMT"),
					(when($"TRANSPLANT_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_ALLWD_CMAD_AMT")).alias("TRANSPLANT_ALLWD_CMAD_AMT"),
					(when($"TRANSPLANT_CASE_NBR".isNull, lit(0)) otherwise ($"TRANSPLANT_CASE_NBR")).alias("TRANSPLANT_CASE_NBR"),
					(when($"TRANSPLANT_CMI_NBR".isNull, lit(0)) otherwise ($"TRANSPLANT_CMI_NBR")).alias("TRANSPLANT_CMI_NBR"),
					
					
					(when($"INPAT_OTH_BILLD_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_BILLD_AMT")).alias("INPAT_OTH_BILLD_AMT"),
					(when($"INPAT_OTH_ALLWD_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_ALLWD_AMT")).alias("INPAT_OTH_ALLWD_AMT"),
					(when($"INPAT_OTH_PAID_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_PAID_AMT")).alias("INPAT_OTH_PAID_AMT"),
					(when($"INPAT_OTH_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_BILLD_CASE_AMT")).alias("INPAT_OTH_BILLD_CASE_AMT"),
					(when($"INPAT_OTH_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_ALLWD_CASE_AMT")).alias("INPAT_OTH_ALLWD_CASE_AMT"),
					(when($"INPAT_OTH_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_PAID_CASE_AMT")).alias("INPAT_OTH_PAID_CASE_AMT"),
					(when($"INPAT_OTH_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_ALLWD_CMAD_AMT")).alias("INPAT_OTH_ALLWD_CMAD_AMT"),
					(when($"INPAT_OTH_CASE_NBR".isNull, lit(0)) otherwise ($"INPAT_OTH_CASE_NBR")).alias("INPAT_OTH_CASE_NBR"),
					(when($"INPAT_OTH_CMI_NBR".isNull, lit(0)) otherwise ($"INPAT_OTH_CMI_NBR")).alias("INPAT_OTH_CMI_NBR"),
//					(when($"INPAT_OTH_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"INPAT_OTH_ALWD_AMT_WITH_CMS")).alias("INPAT_OTH_ALWD_AMT_WITH_CMS"),
//					(when($"INPAT_OTH_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_CMS_REIMBMNT_AMT")).alias("INPAT_OTH_CMS_REIMBMNT_AMT"),
					
					(when($"ER_PCT".isNull, lit(0)) otherwise ($"ER_PCT")).alias("ER_PCT_NBR"),
					(when($"ER_BILLD_AMT".isNull, lit(0)) otherwise ($"ER_BILLD_AMT")).alias("ER_BILLD_AMT"),
					(when($"ER_ALLWD_AMT".isNull, lit(0)) otherwise ($"ER_ALLWD_AMT")).alias("ER_ALLWD_AMT"),
					(when($"ER_PAID_AMT".isNull, lit(0)) otherwise ($"ER_PAID_AMT")).alias("ER_PAID_AMT"),
					(when($"ER_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"ER_BILLD_CASE_AMT")).alias("ER_BILLD_CASE_AMT"),
					(when($"ER_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"ER_ALLWD_CASE_AMT")).alias("ER_ALLWD_CASE_AMT"),
					(when($"ER_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"ER_PAID_CASE_AMT")).alias("ER_PAID_CASE_AMT"),
					(when($"ER_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"ER_ALLWD_CMAD_AMT")).alias("ER_ALLWD_CMAD_AMT"),
					(when($"ER_CASE_NBR".isNull, lit(0)) otherwise ($"ER_CASE_NBR")).alias("ER_CASE_NBR"),
					(when($"ER_CMI_NBR".isNull, lit(0)) otherwise ($"ER_CMI_NBR")).alias("ER_CMI_NBR"),
//					(when($"ER_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"ER_ALWD_AMT_WITH_CMS")).alias("ER_ALWD_AMT_WITH_CMS"),
//					(when($"ER_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"ER_CMS_REIMBMNT_AMT")).alias("ER_CMS_REIMBMNT_AMT"),
					
					(when($"SRGY_ER_BILLD_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_BILLD_AMT")).alias("SRGY_ER_BILLD_AMT"),
					(when($"SRGY_ER_ALLWD_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_ALLWD_AMT")).alias("SRGY_ER_ALLWD_AMT"),
					(when($"SRGY_ER_PAID_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_PAID_AMT")).alias("SRGY_ER_PAID_AMT"),
					(when($"SRGY_ER_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_BILLD_CASE_AMT")).alias("SRGY_ER_BILLD_CASE_AMT"),
					(when($"SRGY_ER_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_ALLWD_CASE_AMT")).alias("SRGY_ER_ALLWD_CASE_AMT"),
					(when($"SRGY_ER_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_PAID_CASE_AMT")).alias("SRGY_ER_PAID_CASE_AMT"),
					(when($"SRGY_ER_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_ALLWD_CMAD_AMT")).alias("SRGY_ER_ALLWD_CMAD_AMT"),
					(when($"SRGY_ER_CASE_NBR".isNull, lit(0)) otherwise ($"SRGY_ER_CASE_NBR")).alias("SRGY_ER_CASE_NBR"),
					(when($"SRGY_ER_CMI_NBR".isNull, lit(0)) otherwise ($"SRGY_ER_CMI_NBR")).alias("SRGY_ER_CMI_NBR"),
//					(when($"SRGY_ER_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"SRGY_ER_ALWD_AMT_WITH_CMS")).alias("SRGY_ER_ALWD_AMT_WITH_CMS"),
//					(when($"SRGY_ER_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_CMS_REIMBMNT_AMT")).alias("SRGY_ER_CMS_REIMBMNT_AMT"),
					
					
					(when($"SRGY_BILLD_AMT".isNull, lit(0)) otherwise ($"SRGY_BILLD_AMT")).alias("SRGY_BILLD_AMT"),
					(when($"SRGY_ALLWD_AMT".isNull, lit(0)) otherwise ($"SRGY_ALLWD_AMT")).alias("SRGY_ALLWD_AMT"),
					(when($"SRGY_PAID_AMT".isNull, lit(0)) otherwise ($"SRGY_PAID_AMT")).alias("SRGY_PAID_AMT"),
					(when($"SRGY_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_BILLD_CASE_AMT")).alias("SRGY_BILLD_CASE_AMT"),
					(when($"SRGY_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_ALLWD_CASE_AMT")).alias("SRGY_ALLWD_CASE_AMT"),
					(when($"SRGY_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_PAID_CASE_AMT")).alias("SRGY_PAID_CASE_AMT"),
					(when($"SRGY_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"SRGY_ALLWD_CMAD_AMT")).alias("SRGY_ALLWD_CMAD_AMT"),
					(when($"SRGY_CASE_NBR".isNull, lit(0)) otherwise ($"SRGY_CASE_NBR")).alias("SRGY_CASE_NBR"),
					(when($"SRGY_CMI_NBR".isNull, lit(0)) otherwise ($"SRGY_CMI_NBR")).alias("SRGY_CMI_NBR"),
//					(when($"SRGY_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"SRGY_ALWD_AMT_WITH_CMS")).alias("SRGY_ALWD_AMT_WITH_CMS"),
//					(when($"SRGY_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"SRGY_CMS_REIMBMNT_AMT")).alias("SRGY_CMS_REIMBMNT_AMT"),
					
					(when($"LAB_BILLD_AMT".isNull, lit(0)) otherwise ($"LAB_BILLD_AMT")).alias("LAB_BILLD_AMT"),
					(when($"LAB_ALLWD_AMT".isNull, lit(0)) otherwise ($"LAB_ALLWD_AMT")).alias("LAB_ALLWD_AMT"),
					(when($"LAB_PAID_AMT".isNull, lit(0)) otherwise ($"LAB_PAID_AMT")).alias("LAB_PAID_AMT"),
					(when($"LAB_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"LAB_BILLD_CASE_AMT")).alias("LAB_BILLD_CASE_AMT"),
					(when($"LAB_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"LAB_ALLWD_CASE_AMT")).alias("LAB_ALLWD_CASE_AMT"),
					(when($"LAB_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"LAB_PAID_CASE_AMT")).alias("LAB_PAID_CASE_AMT"),
					(when($"LAB_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"LAB_ALLWD_CMAD_AMT")).alias("LAB_ALLWD_CMAD_AMT"),
					(when($"LAB_CASE_NBR".isNull, lit(0)) otherwise ($"LAB_CASE_NBR")).alias("LAB_CASE_NBR"),
					(when($"LAB_CMI_NBR".isNull, lit(0)) otherwise ($"LAB_CMI_NBR")).alias("LAB_CMI_NBR"),
//					(when($"LAB_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"LAB_ALWD_AMT_WITH_CMS")).alias("LAB_ALWD_AMT_WITH_CMS"),
//					(when($"LAB_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"LAB_CMS_REIMBMNT_AMT")).alias("LAB_CMS_REIMBMNT_AMT"),
					
					(when($"RDLGY_SCANS_BILLD_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_BILLD_AMT")).alias("RDLGY_SCANS_BILLD_AMT"),
					(when($"RDLGY_SCANS_ALLWD_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_ALLWD_AMT")).alias("RDLGY_SCANS_ALLWD_AMT"),
					(when($"RDLGY_SCANS_PAID_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_PAID_AMT")).alias("RDLGY_SCANS_PAID_AMT"),
					(when($"RDLGY_SCANS_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_BILLD_CASE_AMT")).alias("RDLGY_SCANS_BILLD_CASE_AMT"),
					(when($"RDLGY_SCANS_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_ALLWD_CASE_AMT")).alias("RDLGY_SCANS_ALLWD_CASE_AMT"),
					(when($"RDLGY_SCANS_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_PAID_CASE_AMT")).alias("RDLGY_SCANS_PAID_CASE_AMT"),
					(when($"RDLGY_SCANS_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_ALLWD_CMAD_AMT")).alias("RDLGY_SCANS_ALLWD_CMAD_AMT"),
					(when($"RDLGY_SCANS_CASE_NBR".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CASE_NBR")).alias("RDLGY_SCANS_CASE_NBR"),
					(when($"RDLGY_SCANS_CMI_NBR".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CMI_NBR")).alias("RDLGY_SCANS_CMI_NBR"),
//					(when($"RDLGY_SCANS_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"RDLGY_SCANS_ALWD_AMT_WITH_CMS")).alias("RDLGY_SCANS_ALWD_AMT_WITH_CMS"),
//					(when($"RDLGY_SCANS_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CMS_REIMBMNT_AMT")).alias("RDLGY_SCANS_CMS_REIMBMNT_AMT"),
					
					(when($"RDLGY_OTHR_BILLD_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_BILLD_AMT")).alias("RDLGY_OTH_BILLD_AMT"),
					(when($"RDLGY_OTHR_ALLWD_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_ALLWD_AMT")).alias("RDLGY_OTH_ALLWD_AMT"),
					(when($"RDLGY_OTHR_PAID_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_PAID_AMT")).alias("RDLGY_OTH_PAID_AMT"),
					(when($"RDLGY_OTHR_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_BILLD_CASE_AMT")).alias("RDLGY_OTH_BILLD_CASE_AMT"),
					(when($"RDLGY_OTHR_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_ALLWD_CASE_AMT")).alias("RDLGY_OTH_ALLWD_CASE_AMT"),
					(when($"RDLGY_OTHR_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_PAID_CASE_AMT")).alias("RDLGY_OTH_PAID_CASE_AMT"),
					(when($"RDLGY_OTHR_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_ALLWD_CMAD_AMT")).alias("RDLGY_OTH_ALLWD_CMAD_AMT"),
					(when($"RDLGY_OTHR_CASE_NBR".isNull, lit(0)) otherwise ($"RDLGY_OTHR_CASE_NBR")).alias("RDLGY_OTH_CASE_NBR"),
					(when($"RDLGY_OTHR_CMI_NBR".isNull, lit(0)) otherwise ($"RDLGY_OTHR_CMI_NBR")).alias("RDLGY_OTH_CMI_NBR"),
//					(when($"RDLGY_OTHR_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"RDLGY_OTHR_ALWD_AMT_WITH_CMS")).alias("RDLGY_OTHR_ALWD_AMT_WITH_CMS"),
//					(when($"RDLGY_OTHR_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_CMS_REIMBMNT_AMT")).alias("RDLGY_OTHR_CMS_REIMBMNT_AMT"),
					
					(when($"DRUG_BILLD_AMT".isNull, lit(0)) otherwise ($"DRUG_BILLD_AMT")).alias("DRUG_BILLD_AMT"),
					(when($"DRUG_ALLWD_AMT".isNull, lit(0)) otherwise ($"DRUG_ALLWD_AMT")).alias("DRUG_ALLWD_AMT"),
					(when($"DRUG_PAID_AMT".isNull, lit(0)) otherwise ($"DRUG_PAID_AMT")).alias("DRUG_PAID_AMT"),
					(when($"DRUG_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"DRUG_BILLD_CASE_AMT")).alias("DRUG_BILLD_CASE_AMT"),
					(when($"DRUG_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"DRUG_ALLWD_CASE_AMT")).alias("DRUG_ALLWD_CASE_AMT"),
					(when($"DRUG_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"DRUG_PAID_CASE_AMT")).alias("DRUG_PAID_CASE_AMT"),
					(when($"DRUG_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"DRUG_ALLWD_CMAD_AMT")).alias("DRUG_ALLWD_CMAD_AMT"),
					(when($"DRUG_CASE_NBR".isNull, lit(0)) otherwise ($"DRUG_CASE_NBR")).alias("DRUG_CASE_NBR"),
					(when($"DRUG_CMI_NBR".isNull, lit(0)) otherwise ($"DRUG_CMI_NBR")).alias("DRUG_CMI_NBR"),
//					(when($"DRUG_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"DRUG_ALWD_AMT_WITH_CMS")).alias("DRUG_ALWD_AMT_WITH_CMS"),
//					(when($"DRUG_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"DRUG_CMS_REIMBMNT_AMT")).alias("DRUG_CMS_REIMBMNT_AMT"),
					
					(when($"OUTPAT_OTHR_BILLD_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_BILLD_AMT")).alias("OUTPAT_OTH_BILLD_AMT"),
					(when($"OUTPAT_OTHR_ALLWD_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_ALLWD_AMT")).alias("OUTPAT_OTH_ALLWD_AMT"),
					(when($"OUTPAT_OTHR_PAID_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_PAID_AMT")).alias("OUTPAT_OTH_PAID_AMT"),
					(when($"OUTPAT_OTHR_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_BILLD_CASE_AMT")).alias("OUTPAT_OTH_BILLD_CASE_AMT"),
					(when($"OUTPAT_OTHR_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_ALLWD_CASE_AMT")).alias("OUTPAT_OTH_ALLWD_CASE_AMT"),
					(when($"OUTPAT_OTHR_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_PAID_CASE_AMT")).alias("OUTPAT_OTH_PAID_CASE_AMT"),
					(when($"OUTPAT_OTHR_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_ALLWD_CMAD_AMT")).alias("OUTPAT_OTH_ALLWD_CMAD_AMT"),
					(when($"OUTPAT_OTHR_CASE_NBR".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CASE_NBR")).alias("OUTPAT_OTH_CASE_NBR"),
					(when($"OUTPAT_OTHR_CMI_NBR".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CMI_NBR")).alias("OUTPAT_OTH_CMI_NBR"),
//					(when($"OUTPAT_OTHR_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_ALWD_AMT_WITH_CMS")).alias("OUTPAT_OTHR_ALWD_AMT_WITH_CMS"),
//					(when($"OUTPAT_OTHR_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CMS_REIMBMNT_AMT")).alias("OUTPAT_OTHR_CMS_REIMBMNT_AMT"),
					
					(when($"INPAT_ALLWD_AMT".isNull, lit(0)) otherwise ($"INPAT_ALLWD_AMT")).alias("INPAT_ALLWD_AMT"),
					(when($"OUTPAT_ALLWD_AMT".isNull, lit(0)) otherwise ($"OUTPAT_ALLWD_AMT")).alias("OUTPAT_ALLWD_AMT"),
					(when($"TOTAL_ALLWD_AMT".isNull, lit(0)) otherwise ($"TOTAL_ALLWD_AMT")).alias("TOTAL_ALLWD_AMT"),
					(when($"TOTAL_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"TOTAL_ALWD_AMT_WITH_CMS")).alias("TOTAL_ALWD_AMT_WITH_CMS"),
					(when($"TOTAL_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"TOTAL_CMS_REIMBMNT_AMT")).alias("TOTAL_CMS_REIMBMNT_AMT"),
					(when($"MTRNTY_PCT_CMAD_NBR".isNull, lit(0)) otherwise ($"MTRNTY_PCT_CMAD_NBR")).alias("MTRNTY_PCT_CMAD_NBR"),
					(when($"NICU_PCT_CMAD_NBR".isNull, lit(0)) otherwise ($"NICU_PCT_CMAD_NBR")).alias("NICU_PCT_CMAD_NBR"),
					(lit(0)).alias("INPAT_OTH_PCT_CMAD_NBR"),
					(when($"ER_PCT_CMAD_NBR".isNull, lit(0)) otherwise ($"ER_PCT_CMAD_NBR")).alias("ER_PCT_CMAD_NBR"),
					(when($"SRGY_ER_PCT_CMAD_NBR".isNull, lit(0)) otherwise ($"SRGY_ER_PCT_CMAD_NBR")).alias("SRGRY_ER_PCT_CMAD_NBR"),
					(when($"SRGY_PCT_CMAD_NBR".isNull, lit(0)) otherwise ($"SRGY_PCT_CMAD_NBR")).alias("SRGRY_PCT_CMAD_NBR"),
					(when($"LAB_PCT_CMAD_NBR".isNull, lit(0)) otherwise ($"LAB_PCT_CMAD_NBR")).alias("LAB_PCT_CMAD_NBR"),
					(when($"RDLGY_SCANS_PCT_CMAD_NBR".isNull, lit(0)) otherwise ($"RDLGY_SCANS_PCT_CMAD_NBR")).alias("RDLGY_SCANS_PCT_CMAD_NBR"),
					(when($"RDLGY_OTHR_PCT_CMAD_NBR".isNull, lit(0)) otherwise ($"RDLGY_OTHR_PCT_CMAD_NBR")).alias("RDLGY_PCT_CMAD_NBR"),
					(when($"DRUG_PCT_CMAD_NBR".isNull, lit(0)) otherwise ($"DRUG_PCT_CMAD_NBR")).alias("DRUGS_PCT_CMAD_NBR"),

					(when($"PROV_EXCLSN_CD".isNull, lit("NA")) otherwise ($"PROV_EXCLSN_CD")).alias("PROV_EXCLSN_CD"),
					(when($"MTRNTY_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"MTRNTY_CMAD_CASE_CNT")).alias("MTRNTY_CMAD_CASE_CNT"),
					(when($"MTRNTY_CMAD_NBR".isNull, lit(0)) otherwise ($"MTRNTY_CMAD_NBR")).alias("MTRNTY_CMAD_NBR"),
					(when($"NICU_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"NICU_CMAD_CASE_CNT")).alias("NICU_CMAD_CASE_CNT"),
					(when($"NICU_CMAD_NBR".isNull, lit(0)) otherwise ($"NICU_CMAD_NBR")).alias("NICU_CMAD_NBR"),
					(when($"TRANSPLANT_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"TRANSPLANT_CMAD_CASE_CNT")).alias("TRANSPLANT_CMAD_CASE_CNT"),
					(when($"TRANSPLANT_CMAD_NBR".isNull, lit(0)) otherwise ($"TRANSPLANT_CMAD_NBR")).alias("TRANSPLANT_CMAD_NBR"),
					(when($"INPAT_OTH_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"INPAT_OTH_CMAD_CASE_CNT")).alias("INPAT_OTH_CMAD_CASE_CNT"),
					(when($"INPAT_OTH_CMAD_NBR".isNull, lit(0)) otherwise ($"INPAT_OTH_CMAD_NBR")).alias("INPAT_OTH_CMAD_NBR"),
					(when($"ER_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"ER_CMAD_CASE_CNT")).alias("ER_CMAD_CASE_CNT"),
					(when($"ER_CMAD_NBR".isNull, lit(0)) otherwise ($"ER_CMAD_NBR")).alias("ER_CMAD_CASE_NBR"),
					(when($"SRGY_ER_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"SRGY_ER_CMAD_CASE_CNT")).alias("SRGRY_ER_CMAD_CASE_CNT"),
					(when($"SRGY_ER_CMAD_NBR".isNull, lit(0)) otherwise ($"SRGY_ER_CMAD_NBR")).alias("SRGRY_ER_CMAD_CASE_NBR"),
					(when($"SRGY_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"SRGY_CMAD_CASE_CNT")).alias("SRGRY_CMAD_CASE_CNT"),
					(when($"SRGY_CMAD_NBR".isNull, lit(0)) otherwise ($"SRGY_CMAD_NBR")).alias("SRGRY_CMAD_CASE_NBR"),
					(when($"LAB_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"LAB_CMAD_CASE_CNT")).alias("LAB_CMAD_CASE_CNT"),
					(when($"LAB_CMAD_NBR".isNull, lit(0)) otherwise ($"LAB_CMAD_NBR")).alias("LAB_CMAD_CASE_NBR"),
					(when($"RDLGY_SCANS_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CMAD_CASE_CNT")).alias("RDLGY_SCANS_CMAD_CASE_CNT"),
					(when($"RDLGY_SCANS_CMAD_NBR".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CMAD_NBR")).alias("RDLGY_SCANS_CMAD_CASE_NBR"),
					(when($"RDLGY_OTHR_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_CMAD_CASE_CNT")).alias("RDLGY_OTHR_CMAD_CASE_CNT"),
					(when($"RDLGY_OTHR_CMAD_NBR".isNull, lit(0)) otherwise ($"RDLGY_OTHR_CMAD_NBR")).alias("RDLGY_OTHR_CMAD_CASE_NBR"),
					(when($"DRUG_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"DRUG_CMAD_CASE_CNT")).alias("DRUG_OTHR_CMAD_CASE_CNT"),
					(when($"DRUG_CMAD_NBR".isNull, lit(0)) otherwise ($"DRUG_CMAD_NBR")).alias("DRUG_OTHR_CMAD_CASE_NBR"),
					(when($"OUTPAT_OTHR_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CMAD_CASE_CNT")).alias("OUTPAT_OTHR_CMAD_CASE_CNT"),
					(when($"OUTPAT_OTHR_CMAD_NBR".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CMAD_NBR")).alias("OUTPAT_OTHR_CMAD_CASE_NBR"))
				/*.dropDuplicates(
							"PROV_ST_CD",
							"PROD_LVL3_DESC",
							"FUNDG_LVL2_DESC",
							"MBU_LVL4_DESC",
							"MBU_LVL3_DESC",
							"EXCHNG_IND_CD",
							"MBU_LVL2_DESC",
							"HOSP_NM",
							"HOSP_SYS_NM",
							"SUBMRKT_DESC",
							"PROV_CNTY_NM",
							"FCLTY_TYPE_DESC",
							"PAR_STTS_CD",
							"PROV_EXCLSN_CD")   //======Jan-07-2019 :Remove drop duplicate as part of Design fix  =====// */

	}
}

case class ndoEhoppaBaseMbrCnty(ndowrkEhoppaMbrCntyBaseDf: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			ndowrkEhoppaMbrCntyBaseDf.select(
					(when($"PROV_ST_NM".isNull, lit("NA")) otherwise ($"PROV_ST_NM")).alias("PROV_ST_CD"),
					(when($"PROD_LVL3_DESC".isNull, lit("NA")) otherwise ($"PROD_LVL3_DESC")).alias("PROD_LVL3_DESC"),
					(when($"FUNDG_LVL2_DESC".isNull, lit("NA")) otherwise ($"FUNDG_LVL2_DESC")).alias("FUNDG_LVL2_DESC"),
					(when($"MBU_LVL4_DESC".isNull, lit("NA")) otherwise ($"MBU_LVL4_DESC")).alias("MBU_LVL4_DESC"),
					(when($"MBU_LVL3_DESC".isNull, lit("NA")) otherwise ($"MBU_LVL3_DESC")).alias("MBU_LVL3_DESC"),
					(when($"EXCHNG_IND_CD".isNull, lit("NA")) otherwise ($"EXCHNG_IND_CD")).alias("EXCHNG_IND_CD"),
					(when($"MBU_LVL2_DESC".isNull, lit("NA")) otherwise ($"MBU_LVL2_DESC")).alias("MBU_LVL2_DESC"),
					(when($"MBR_CNTY_NM".isNull, lit("NA")) otherwise ($"MBR_CNTY_NM")).alias("MBR_CNTY_NM"),
					(when($"HOSP_NM".isNull, lit("NA")) otherwise ($"HOSP_NM")).alias("HOSP_NM"),
					(when($"HOSP_SYS_NM".isNull, lit("NA")) otherwise ($"HOSP_SYS_NM")).alias("HOSP_SYS_NM"),
					(when($"SUBMRKT_DESC".isNull, lit("NA")) otherwise ($"SUBMRKT_DESC")).alias("SUBMRKT_DESC"),
					(when($"PROV_CNTY_NM".isNull, lit("NA")) otherwise ($"PROV_CNTY_NM")).alias("PROV_CNTY_NM"),
					(when($"FCLTY_TYPE_DESC".isNull, lit("NA")) otherwise ($"FCLTY_TYPE_DESC")).alias("FCLTY_TYPE_DESC"),
          (when($"RPTG_NTWK_DESC".isNull, lit("NA")) otherwise ($"RPTG_NTWK_DESC")).alias("RPTG_NTWK_DESC"),
					(when($"PAR_STTS_CD".isNull, lit("NA")) otherwise ($"PAR_STTS_CD")).alias("PAR_STTS_CD"),
					(when($"PROV_EXCLSN_CD".isNull, lit(0)) otherwise ($"PROV_EXCLSN_CD")).alias("PROV_EXCLSN_CD"),
					(when($"MTRNTY_BILLD_AMT".isNull, lit(0)) otherwise ($"MTRNTY_BILLD_AMT")).alias("MTRNTY_BILLD_AMT"),
					(when($"MTRNTY_ALLWD_AMT".isNull, lit(0)) otherwise ($"MTRNTY_ALLWD_AMT")).alias("MTRNTY_ALLWD_AMT"),
					(when($"MTRNTY_PAID_AMT".isNull, lit(0)) otherwise ($"MTRNTY_PAID_AMT")).alias("MTRNTY_PAID_AMT"),
					(when($"MTRNTY_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"MTRNTY_BILLD_CASE_AMT")).alias("MTRNTY_BILLD_CASE_AMT"),
					(when($"MTRNTY_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"MTRNTY_ALLWD_CASE_AMT")).alias("MTRNTY_ALLWD_CASE_AMT"),
					(when($"MTRNTY_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"MTRNTY_PAID_CASE_AMT")).alias("MTRNTY_PAID_CASE_AMT"),
					(when($"MTRNTY_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"MTRNTY_ALLWD_CMAD_AMT")).alias("MTRNTY_ALLWD_CMAD_AMT"),
					(when($"MTRNTY_CASE_NBR".isNull, lit(0)) otherwise ($"MTRNTY_CASE_NBR")).alias("MTRNTY_CASE_NBR"),
					(when($"MTRNTY_CMI_NBR".isNull, lit(0)) otherwise ($"MTRNTY_CMI_NBR")).alias("MTRNTY_CMI_NBR"),
					(when($"MTRNTY_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"MTRNTY_CMAD_CASE_CNT")).alias("MTRNTY_CMAD_CASE_CNT"),
					(when($"MTRNTY_CMAD_NBR".isNull, lit(0)) otherwise ($"MTRNTY_CMAD_NBR")).alias("MTRNTY_CMAD_NBR"),
					(when($"NICU_BILLD_AMT".isNull, lit(0)) otherwise ($"NICU_BILLD_AMT")).alias("NICU_BILLD_AMT"),
					(when($"NICU_ALLWD_AMT".isNull, lit(0)) otherwise ($"NICU_ALLWD_AMT")).alias("NICU_ALLWD_AMT"),
					(when($"NICU_PAID_AMT".isNull, lit(0)) otherwise ($"NICU_PAID_AMT")).alias("NICU_PAID_AMT"),
					(when($"NICU_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"NICU_BILLD_CASE_AMT")).alias("NICU_BILLD_CASE_AMT"),
					(when($"NICU_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"NICU_ALLWD_CASE_AMT")).alias("NICU_ALLWD_CASE_AMT"),
					(when($"NICU_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"NICU_PAID_CASE_AMT")).alias("NICU_PAID_CASE_AMT"),
					(when($"NICU_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"NICU_ALLWD_CMAD_AMT")).alias("NICU_ALLWD_CMAD_AMT"),
					(when($"NICU_CASE_NBR".isNull, lit(0)) otherwise ($"NICU_CASE_NBR")).alias("NICU_CASE_NBR"),
					(when($"NICU_CMI_NBR".isNull, lit(0)) otherwise ($"NICU_CMI_NBR")).alias("NICU_CMI_NBR"),
					(when($"NICU_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"NICU_CMAD_CASE_CNT")).alias("NICU_CMAD_CASE_CNT"),
					(when($"NICU_CMAD_NBR".isNull, lit(0)) otherwise ($"NICU_CMAD_NBR")).alias("NICU_CMAD_NBR"),
					
					(when($"TRANSPLANT_BILLD_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_BILLD_AMT")).alias("TRANSPLANT_BILLD_AMT"),
					(when($"TRANSPLANT_ALLWD_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_ALLWD_AMT")).alias("TRANSPLANT_ALLWD_AMT"),
					(when($"TRANSPLANT_PAID_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_PAID_AMT")).alias("TRANSPLANT_PAID_AMT"),
					(when($"TRANSPLANT_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_BILLD_CASE_AMT")).alias("TRANSPLANT_BILLD_CASE_AMT"),
					(when($"TRANSPLANT_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_ALLWD_CASE_AMT")).alias("TRANSPLANT_ALLWD_CASE_AMT"),
					(when($"TRANSPLANT_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_PAID_CASE_AMT")).alias("TRANSPLANT_PAID_CASE_AMT"),
					(when($"TRANSPLANT_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_ALLWD_CMAD_AMT")).alias("TRANSPLANT_ALLWD_CMAD_AMT"),
					(when($"TRANSPLANT_CASE_NBR".isNull, lit(0)) otherwise ($"TRANSPLANT_CASE_NBR")).alias("TRANSPLANT_CASE_NBR"),
					(when($"TRANSPLANT_CMI_NBR".isNull, lit(0)) otherwise ($"TRANSPLANT_CMI_NBR")).alias("TRANSPLANT_CMI_NBR"),
					(when($"TRANSPLANT_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"TRANSPLANT_CMAD_CASE_CNT")).alias("TRANSPLANT_CMAD_CASE_CNT"),
					(when($"TRANSPLANT_CMAD_NBR".isNull, lit(0)) otherwise ($"TRANSPLANT_CMAD_NBR")).alias("TRANSPLANT_CMAD_NBR"),
					
					(when($"INPAT_OTH_BILLD_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_BILLD_AMT")).alias("INPAT_OTH_BILLD_AMT"),
					(when($"INPAT_OTH_ALLWD_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_ALLWD_AMT")).alias("INPAT_OTH_ALLWD_AMT"),
					(when($"INPAT_OTH_PAID_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_PAID_AMT")).alias("INPAT_OTH_PAID_AMT"),
					(when($"INPAT_OTH_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_BILLD_CASE_AMT")).alias("INPAT_OTH_BILLD_CASE_AMT"),
					(when($"INPAT_OTH_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_ALLWD_CASE_AMT")).alias("INPAT_OTH_ALLWD_CASE_AMT"),
					(when($"INPAT_OTH_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_PAID_CASE_AMT")).alias("INPAT_OTH_PAID_CASE_AMT"),
					(when($"INPAT_OTH_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_ALLWD_CMAD_AMT")).alias("INPAT_OTH_ALLWD_CMAD_AMT"),
					(when($"INPAT_OTH_CASE_NBR".isNull, lit(0)) otherwise ($"INPAT_OTH_CASE_NBR")).alias("INPAT_OTH_CASE_NBR"),
					(when($"INPAT_OTH_CMI_NBR".isNull, lit(0)) otherwise ($"INPAT_OTH_CMI_NBR")).alias("INPAT_OTH_CMI_NBR"),
					(when($"INPAT_OTH_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"INPAT_OTH_CMAD_CASE_CNT")).alias("INPAT_OTH_CMAD_CASE_CNT"),
					(when($"INPAT_OTH_CMAD_NBR".isNull, lit(0)) otherwise ($"INPAT_OTH_CMAD_NBR")).alias("INPAT_OTH_CMAD_NBR"),
					(when($"ER_PCT".isNull, lit(0)) otherwise ($"ER_PCT")).alias("ER_PCT_NBR"),
					(when($"ER_BILLD_AMT".isNull, lit(0)) otherwise ($"ER_BILLD_AMT")).alias("ER_BILLD_AMT"),
					(when($"ER_ALLWD_AMT".isNull, lit(0)) otherwise ($"ER_ALLWD_AMT")).alias("ER_ALLWD_AMT"),
					(when($"ER_PAID_AMT".isNull, lit(0)) otherwise ($"ER_PAID_AMT")).alias("ER_PAID_AMT"),
					(when($"ER_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"ER_BILLD_CASE_AMT")).alias("ER_BILLD_CASE_AMT"),
					(when($"ER_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"ER_ALLWD_CASE_AMT")).alias("ER_ALLWD_CASE_AMT"),
					(when($"ER_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"ER_PAID_CASE_AMT")).alias("ER_PAID_CASE_AMT"),
					(when($"ER_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"ER_ALLWD_CMAD_AMT")).alias("ER_ALLWD_CMAD_AMT"),
					(when($"ER_CASE_NBR".isNull, lit(0)) otherwise ($"ER_CASE_NBR")).alias("ER_CASE_NBR"),
					(when($"ER_CMI_NBR".isNull, lit(0)) otherwise ($"ER_CMI_NBR")).alias("ER_CMI_NBR"),
					(when($"ER_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"ER_CMAD_CASE_CNT")).alias("ER_CMAD_CASE_CNT"),
					(when($"ER_CMAD_NBR".isNull, lit(0)) otherwise ($"ER_CMAD_NBR")).alias("ER_CMAD_CASE_NBR"),
					(when($"SRGY_ER_BILLD_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_BILLD_AMT")).alias("SRGY_ER_BILLD_AMT"),
					(when($"SRGY_ER_ALLWD_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_ALLWD_AMT")).alias("SRGY_ER_ALLWD_AMT"),
					(when($"SRGY_ER_PAID_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_PAID_AMT")).alias("SRGY_ER_PAID_AMT"),
					(when($"SRGY_ER_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_BILLD_CASE_AMT")).alias("SRGY_ER_BILLD_CASE_AMT"),
					(when($"SRGY_ER_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_ALLWD_CASE_AMT")).alias("SRGY_ER_ALLWD_CASE_AMT"),
					(when($"SRGY_ER_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_PAID_CASE_AMT")).alias("SRGY_ER_PAID_CASE_AMT"),
					(when($"SRGY_ER_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_ALLWD_CMAD_AMT")).alias("SRGY_ER_ALLWD_CMAD_AMT"),
					(when($"SRGY_ER_CASE_NBR".isNull, lit(0)) otherwise ($"SRGY_ER_CASE_NBR")).alias("SRGY_ER_CASE_NBR"),
					(when($"SRGY_ER_CMI_NBR".isNull, lit(0)) otherwise ($"SRGY_ER_CMI_NBR")).alias("SRGY_ER_CMI_NBR"),
					(when($"SRGY_ER_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"SRGY_ER_CMAD_CASE_CNT")).alias("SRGRY_ER_CMAD_CASE_CNT"),
					(when($"SRGY_ER_CMAD_NBR".isNull, lit(0)) otherwise ($"SRGY_ER_CMAD_NBR")).alias("SRGRY_ER_CMAD_CASE_NBR"),
					(when($"SRGY_BILLD_AMT".isNull, lit(0)) otherwise ($"SRGY_BILLD_AMT")).alias("SRGY_BILLD_AMT"),
					(when($"SRGY_ALLWD_AMT".isNull, lit(0)) otherwise ($"SRGY_ALLWD_AMT")).alias("SRGY_ALLWD_AMT"),
					(when($"SRGY_PAID_AMT".isNull, lit(0)) otherwise ($"SRGY_PAID_AMT")).alias("SRGY_PAID_AMT"),
					(when($"SRGY_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_BILLD_CASE_AMT")).alias("SRGY_BILLD_CASE_AMT"),
					(when($"SRGY_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_ALLWD_CASE_AMT")).alias("SRGY_ALLWD_CASE_AMT"),
					(when($"SRGY_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_PAID_CASE_AMT")).alias("SRGY_PAID_CASE_AMT"),
					(when($"SRGY_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"SRGY_ALLWD_CMAD_AMT")).alias("SRGY_ALLWD_CMAD_AMT"),
					(when($"SRGY_CASE_NBR".isNull, lit(0)) otherwise ($"SRGY_CASE_NBR")).alias("SRGY_CASE_NBR"),
					(when($"SRGY_CMI_NBR".isNull, lit(0)) otherwise ($"SRGY_CMI_NBR")).alias("SRGY_CMI_NBR"),
					(when($"SRGY_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"SRGY_CMAD_CASE_CNT")).alias("SRGRY_CMAD_CASE_CNT"),
					(when($"SRGY_CMAD_NBR".isNull, lit(0)) otherwise ($"SRGY_CMAD_NBR")).alias("SRGRY_CMAD_CASE_NBR"),
					(when($"LAB_BILLD_AMT".isNull, lit(0)) otherwise ($"LAB_BILLD_AMT")).alias("LAB_BILLD_AMT"),
					(when($"LAB_ALLWD_AMT".isNull, lit(0)) otherwise ($"LAB_ALLWD_AMT")).alias("LAB_ALLWD_AMT"),
					(when($"LAB_PAID_AMT".isNull, lit(0)) otherwise ($"LAB_PAID_AMT")).alias("LAB_PAID_AMT"),
					(when($"LAB_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"LAB_BILLD_CASE_AMT")).alias("LAB_BILLD_CASE_AMT"),
					(when($"LAB_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"LAB_ALLWD_CASE_AMT")).alias("LAB_ALLWD_CASE_AMT"),
					(when($"LAB_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"LAB_PAID_CASE_AMT")).alias("LAB_PAID_CASE_AMT"),
					(when($"LAB_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"LAB_ALLWD_CMAD_AMT")).alias("LAB_ALLWD_CMAD_AMT"),
					(when($"LAB_CASE_NBR".isNull, lit(0)) otherwise ($"LAB_CASE_NBR")).alias("LAB_CASE_NBR"),
					(when($"LAB_CMI_NBR".isNull, lit(0)) otherwise ($"LAB_CMI_NBR")).alias("LAB_CMI_NBR"),
					(when($"LAB_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"LAB_CMAD_CASE_CNT")).alias("LAB_CMAD_CASE_CNT"),
					(when($"LAB_CMAD_NBR".isNull, lit(0)) otherwise ($"LAB_CMAD_NBR")).alias("LAB_CMAD_CASE_NBR"),
					(when($"RDLGY_SCANS_BILLD_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_BILLD_AMT")).alias("RDLGY_SCANS_BILLD_AMT"),
					(when($"RDLGY_SCANS_ALLWD_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_ALLWD_AMT")).alias("RDLGY_SCANS_ALLWD_AMT"),
					(when($"RDLGY_SCANS_PAID_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_PAID_AMT")).alias("RDLGY_SCANS_PAID_AMT"),
					(when($"RDLGY_SCANS_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_BILLD_CASE_AMT")).alias("RDLGY_SCANS_BILLD_CASE_AMT"),
					(when($"RDLGY_SCANS_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_ALLWD_CASE_AMT")).alias("RDLGY_SCANS_ALLWD_CASE_AMT"),
					(when($"RDLGY_SCANS_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_PAID_CASE_AMT")).alias("RDLGY_SCANS_PAID_CASE_AMT"),
					(when($"RDLGY_SCANS_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_ALLWD_CMAD_AMT")).alias("RDLGY_SCANS_ALLWD_CMAD_AMT"),
					(when($"RDLGY_SCANS_CASE_NBR".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CASE_NBR")).alias("RDLGY_SCANS_CASE_NBR"),
					(when($"RDLGY_SCANS_CMI_NBR".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CMI_NBR")).alias("RDLGY_SCANS_CMI_NBR"),
					(when($"RDLGY_SCANS_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CMAD_CASE_CNT")).alias("RDLGY_SCANS_CMAD_CASE_CNT"),
					(when($"RDLGY_SCANS_CMAD_NBR".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CMAD_NBR")).alias("RDLGY_SCANS_CMAD_CASE_NBR"),
					$"RDLGY_OTHR_BILLD_AMT".alias("RDLGY_OTH_BILLD_AMT"),
					$"RDLGY_OTHR_ALLWD_AMT".alias("RDLGY_OTH_ALLWD_AMT"),
					$"RDLGY_OTHR_PAID_AMT".alias("RDLGY_OTH_PAID_AMT"),
					$"RDLGY_OTHR_BILLD_CASE_AMT".alias("RDLGY_OTH_BILLD_CASE_AMT"),
					$"RDLGY_OTHR_ALLWD_CASE_AMT".alias("RDLGY_OTH_ALLWD_CASE_AMT"),
					$"RDLGY_OTHR_PAID_CASE_AMT".alias("RDLGY_OTH_PAID_CASE_AMT"),
					$"RDLGY_OTHR_ALLWD_CMAD_AMT".alias("RDLGY_OTH_ALLWD_CMAD_AMT"),
					$"RDLGY_OTHR_CASE_NBR".alias("RDLGY_OTH_CASE_NBR"),
					$"RDLGY_OTHR_CMI_NBR".alias("RDLGY_OTH_CMI_NBR"),
					(when($"RDLGY_OTHR_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_CMAD_CASE_CNT")).alias("RDLGY_OTHR_CMAD_CASE_CNT"),
					(when($"RDLGY_OTHR_CMAD_NBR".isNull, lit(0)) otherwise ($"RDLGY_OTHR_CMAD_NBR")).alias("RDLGY_OTHR_CMAD_CASE_NBR"),
					(when($"DRUG_BILLD_AMT".isNull, lit(0)) otherwise ($"DRUG_BILLD_AMT")).alias("DRUG_BILLD_AMT"),
					(when($"DRUG_ALLWD_AMT".isNull, lit(0)) otherwise ($"DRUG_ALLWD_AMT")).alias("DRUG_ALLWD_AMT"),
					(when($"DRUG_PAID_AMT".isNull, lit(0)) otherwise ($"DRUG_PAID_AMT")).alias("DRUG_PAID_AMT"),
					(when($"DRUG_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"DRUG_BILLD_CASE_AMT")).alias("DRUG_BILLD_CASE_AMT"),
					(when($"DRUG_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"DRUG_ALLWD_CASE_AMT")).alias("DRUG_ALLWD_CASE_AMT"),
					(when($"DRUG_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"DRUG_PAID_CASE_AMT")).alias("DRUG_PAID_CASE_AMT"),
					(when($"DRUG_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"DRUG_ALLWD_CMAD_AMT")).alias("DRUG_ALLWD_CMAD_AMT"),
					(when($"DRUG_CASE_NBR".isNull, lit(0)) otherwise ($"DRUG_CASE_NBR")).alias("DRUG_CASE_NBR"),
					(when($"DRUG_CMI_NBR".isNull, lit(0)) otherwise ($"DRUG_CMI_NBR")).alias("DRUG_CMI_NBR"),
					(when($"DRUG_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"DRUG_CMAD_CASE_CNT")).alias("DRUG_OTHR_CMAD_CASE_CNT"),
					(when($"DRUG_CMAD_NBR".isNull, lit(0)) otherwise ($"DRUG_CMAD_NBR")).alias("DRUG_OTHR_CMAD_CASE_NBR"),
					(when($"OUTPAT_OTHR_BILLD_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_BILLD_AMT")).alias("OUTPAT_OTH_BILLD_AMT"),
					(when($"OUTPAT_OTHR_ALLWD_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_ALLWD_AMT")).alias("OUTPAT_OTH_ALLWD_AMT"),
					(when($"OUTPAT_OTHR_PAID_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_PAID_AMT")).alias("OUTPAT_OTH_PAID_AMT"),
					(when($"OUTPAT_OTHR_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_BILLD_CASE_AMT")).alias("OUTPAT_OTH_BILLD_CASE_AMT"),
					(when($"OUTPAT_OTHR_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_ALLWD_CASE_AMT")).alias("OUTPAT_OTH_ALLWD_CASE_AMT"),
					(when($"OUTPAT_OTHR_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_PAID_CASE_AMT")).alias("OUTPAT_OTH_PAID_CASE_AMT"),
					(when($"OUTPAT_OTHR_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_ALLWD_CMAD_AMT")).alias("OUTPAT_OTH_ALLWD_CMAD_AMT"),
					(when($"OUTPAT_OTHR_CASE_NBR".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CASE_NBR")).alias("OUTPAT_OTH_CASE_NBR"),
					(when($"OUTPAT_OTHR_CMI_NBR".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CMI_NBR")).alias("OUTPAT_OTH_CMI_NBR"),
					(when($"OUTPAT_OTHR_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CMAD_CASE_CNT")).alias("OUTPAT_OTHR_CMAD_CASE_CNT"),
					(when($"OUTPAT_OTHR_CMAD_NBR".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CMAD_NBR")).alias("OUTPAT_OTHR_CMAD_CASE_NBR"),
					(when($"INPAT_ALLWD_AMT".isNull, lit(0)) otherwise ($"INPAT_ALLWD_AMT")).alias("INPAT_ALLWD_AMT"),
					(when($"OUTPAT_ALLWD_AMT".isNull, lit(0)) otherwise ($"OUTPAT_ALLWD_AMT")).alias("OUTPAT_ALLWD_AMT"),
					(when($"TOTAL_ALLWD_AMT".isNull, lit(0)) otherwise ($"TOTAL_ALLWD_AMT")).alias("TOTAL_ALLWD_AMT"),
					(when($"TOTAL_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"TOTAL_ALWD_AMT_WITH_CMS")).alias("TOTAL_ALWD_AMT_WITH_CMS"),
					(when($"TOTAL_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"TOTAL_CMS_REIMBMNT_AMT")).alias("TOTAL_CMS_REIMBMNT_AMT"))/*.filter(trim($"MBR_CNTY_NM").notEqual("")).dropDuplicates(
							"PROV_ST_CD",
							"PROD_LVL3_DESC",
							"FUNDG_LVL2_DESC",
							"MBU_LVL4_DESC",
							"MBU_LVL3_DESC",
							"EXCHNG_IND_CD",
							"MBU_LVL2_DESC",
							"MBR_CNTY_NM",
							"HOSP_NM",
							"HOSP_SYS_NM",
							"SUBMRKT_DESC",
							"PROV_CNTY_NM",
							"FCLTY_TYPE_DESC",
							"PAR_STTS_CD",
							"PROV_EXCLSN_CD") // Retrofit change - for Primary key constraints*/  //======Jan-14-2019 :Remove drop duplicate as part of Design fix  =====//
	}
}

case class ndoEhoppaBaseMbr(ndowrkEhoppaMbrRatgBaseDf: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			ndowrkEhoppaMbrRatgBaseDf.select(
					(when($"PROV_ST_NM".isNull, lit("NA")) otherwise ($"PROV_ST_NM")).alias("PROV_ST_CD"),
					(when($"PROD_LVL3_DESC".isNull, lit("NA")) otherwise ($"PROD_LVL3_DESC")).alias("PROD_LVL3_DESC"),
					(when($"FUNDG_LVL2_DESC".isNull, lit("NA")) otherwise ($"FUNDG_LVL2_DESC")).alias("FUNDG_LVL2_DESC"),
					(when($"MBU_LVL4_DESC".isNull, lit("NA")) otherwise ($"MBU_LVL4_DESC")).alias("MBU_LVL4_DESC"),
					(when($"MBU_LVL3_DESC".isNull, lit("NA")) otherwise ($"MBU_LVL3_DESC")).alias("MBU_LVL3_DESC"),
					(when($"EXCHNG_IND_CD".isNull, lit("NA")) otherwise ($"EXCHNG_IND_CD")).alias("EXCHNG_IND_CD"),
					(when($"MBU_LVL2_DESC".isNull, lit("NA")) otherwise ($"MBU_LVL2_DESC")).alias("MBU_LVL2_DESC"),
					(when($"MBR_RATG_AREA_NM".isNull, lit("NA")) otherwise ($"MBR_RATG_AREA_NM")).alias("MBR_RATG_AREA_NM"),
					(when($"HOSP_NM".isNull, lit("NA")) otherwise ($"HOSP_NM")).alias("HOSP_NM"),
					(when($"HOSP_SYS_NM".isNull, lit("NA")) otherwise ($"HOSP_SYS_NM")).alias("HOSP_SYS_NM"),
					(when($"SUBMRKT_DESC".isNull, lit("NA")) otherwise ($"SUBMRKT_DESC")).alias("SUBMRKT_DESC"),
					(when($"PROV_CNTY_NM".isNull, lit("NA")) otherwise ($"PROV_CNTY_NM")).alias("PROV_CNTY_NM"),
					(when($"FCLTY_TYPE_DESC".isNull, lit("NA")) otherwise ($"FCLTY_TYPE_DESC")).alias("FCLTY_TYPE_DESC"),
          (when($"RPTG_NTWK_DESC".isNull, lit("NA")) otherwise ($"RPTG_NTWK_DESC")).alias("RPTG_NTWK_DESC"),					
					(when($"PAR_STTS_CD".isNull, lit("NA")) otherwise ($"PAR_STTS_CD")).alias("PAR_STTS_CD"),
					(when($"MTRNTY_BILLD_AMT".isNull, lit(0)) otherwise ($"MTRNTY_BILLD_AMT")).alias("MTRNTY_BILLD_AMT"),
					(when($"MTRNTY_ALLWD_AMT".isNull, lit(0)) otherwise ($"MTRNTY_ALLWD_AMT")).alias("MTRNTY_ALLWD_AMT"),
					(when($"MTRNTY_PAID_AMT".isNull, lit(0)) otherwise ($"MTRNTY_PAID_AMT")).alias("MTRNTY_PAID_AMT"),
					(when($"MTRNTY_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"MTRNTY_BILLD_CASE_AMT")).alias("MTRNTY_BILLD_CASE_AMT"),
					(when($"MTRNTY_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"MTRNTY_ALLWD_CASE_AMT")).alias("MTRNTY_ALLWD_CASE_AMT"),
					(when($"MTRNTY_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"MTRNTY_PAID_CASE_AMT")).alias("MTRNTY_PAID_CASE_AMT"),
					(when($"MTRNTY_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"MTRNTY_ALLWD_CMAD_AMT")).alias("MTRNTY_ALLWD_CMAD_AMT"),
					(when($"MTRNTY_CASE_NBR".isNull, lit(0)) otherwise ($"MTRNTY_CASE_NBR")).alias("MTRNTY_CASE_NBR"),
					(when($"MTRNTY_CMI_NBR".isNull, lit(0)) otherwise ($"MTRNTY_CMI_NBR")).alias("MTRNTY_CMI_NBR"),
//					(when($"MTRNTY_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"MTRNTY_ALWD_AMT_WITH_CMS")).alias("MTRNTY_ALWD_AMT_WITH_CMS"),
//					(when($"MTRNTY_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"MTRNTY_CMS_REIMBMNT_AMT")).alias("MTRNTY_CMS_REIMBMNT_AMT"),
					
					(when($"NICU_BILLD_AMT".isNull, lit(0)) otherwise ($"NICU_BILLD_AMT")).alias("NICU_BILLD_AMT"),
					(when($"NICU_ALLWD_AMT".isNull, lit(0)) otherwise ($"NICU_ALLWD_AMT")).alias("NICU_ALLWD_AMT"),
					(when($"NICU_PAID_AMT".isNull, lit(0)) otherwise ($"NICU_PAID_AMT")).alias("NICU_PAID_AMT"),
					(when($"NICU_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"NICU_BILLD_CASE_AMT")).alias("NICU_BILLD_CASE_AMT"),
					(when($"NICU_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"NICU_ALLWD_CASE_AMT")).alias("NICU_ALLWD_CASE_AMT"),
					(when($"NICU_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"NICU_PAID_CASE_AMT")).alias("NICU_PAID_CASE_AMT"),
					(when($"NICU_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"NICU_ALLWD_CMAD_AMT")).alias("NICU_ALLWD_CMAD_AMT"),
					(when($"NICU_CASE_NBR".isNull, lit(0)) otherwise ($"NICU_CASE_NBR")).alias("NICU_CASE_NBR"),
					(when($"NICU_CMI_NBR".isNull, lit(0)) otherwise ($"NICU_CMI_NBR")).alias("NICU_CMI_NBR"),
//					(when($"NICU_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"NICU_ALWD_AMT_WITH_CMS")).alias("NICU_ALWD_AMT_WITH_CMS"),
//					(when($"NICU_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"NICU_CMS_REIMBMNT_AMT")).alias("NICU_CMS_REIMBMNT_AMT"),
					
					(when($"TRANSPLANT_BILLD_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_BILLD_AMT")).alias("TRANSPLANT_BILLD_AMT"),
					(when($"TRANSPLANT_ALLWD_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_ALLWD_AMT")).alias("TRANSPLANT_ALLWD_AMT"),
					(when($"TRANSPLANT_PAID_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_PAID_AMT")).alias("TRANSPLANT_PAID_AMT"),
					(when($"TRANSPLANT_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_BILLD_CASE_AMT")).alias("TRANSPLANT_BILLD_CASE_AMT"),
					(when($"TRANSPLANT_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_ALLWD_CASE_AMT")).alias("TRANSPLANT_ALLWD_CASE_AMT"),
					(when($"TRANSPLANT_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_PAID_CASE_AMT")).alias("TRANSPLANT_PAID_CASE_AMT"),
					(when($"TRANSPLANT_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"TRANSPLANT_ALLWD_CMAD_AMT")).alias("TRANSPLANT_ALLWD_CMAD_AMT"),
					(when($"TRANSPLANT_CASE_NBR".isNull, lit(0)) otherwise ($"TRANSPLANT_CASE_NBR")).alias("TRANSPLANT_CASE_NBR"),
					(when($"TRANSPLANT_CMI_NBR".isNull, lit(0)) otherwise ($"TRANSPLANT_CMI_NBR")).alias("TRANSPLANT_CMI_NBR"),
					
					(when($"INPAT_OTH_BILLD_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_BILLD_AMT")).alias("INPAT_OTH_BILLD_AMT"),
					(when($"INPAT_OTH_ALLWD_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_ALLWD_AMT")).alias("INPAT_OTH_ALLWD_AMT"),
					(when($"INPAT_OTH_PAID_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_PAID_AMT")).alias("INPAT_OTH_PAID_AMT"),
					(when($"INPAT_OTH_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_BILLD_CASE_AMT")).alias("INPAT_OTH_BILLD_CASE_AMT"),
					(when($"INPAT_OTH_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_ALLWD_CASE_AMT")).alias("INPAT_OTH_ALLWD_CASE_AMT"),
					(when($"INPAT_OTH_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_PAID_CASE_AMT")).alias("INPAT_OTH_PAID_CASE_AMT"),
					(when($"INPAT_OTH_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_ALLWD_CMAD_AMT")).alias("INPAT_OTH_ALLWD_CMAD_AMT"),
					(when($"INPAT_OTH_CASE_NBR".isNull, lit(0)) otherwise ($"INPAT_OTH_CASE_NBR")).alias("INPAT_OTH_CASE_NBR"),
					(when($"INPAT_OTH_CMI_NBR".isNull, lit(0)) otherwise ($"INPAT_OTH_CMI_NBR")).alias("INPAT_OTH_CMI_NBR"),
//					(when($"INPAT_OTH_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"INPAT_OTH_ALWD_AMT_WITH_CMS")).alias("INPAT_OTH_ALWD_AMT_WITH_CMS"),
//					(when($"INPAT_OTH_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"INPAT_OTH_CMS_REIMBMNT_AMT")).alias("INPAT_OTH_CMS_REIMBMNT_AMT"),
					
					(when($"ER_PCT".isNull, lit(0)) otherwise ($"ER_PCT")).alias("ER_PCT_NBR"),
					(when($"ER_BILLD_AMT".isNull, lit(0)) otherwise ($"ER_BILLD_AMT")).alias("ER_BILLD_AMT"),
					(when($"ER_ALLWD_AMT".isNull, lit(0)) otherwise ($"ER_ALLWD_AMT")).alias("ER_ALLWD_AMT"),
					(when($"ER_PAID_AMT".isNull, lit(0)) otherwise ($"ER_PAID_AMT")).alias("ER_PAID_AMT"),
					(when($"ER_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"ER_BILLD_CASE_AMT")).alias("ER_BILLD_CASE_AMT"),
					(when($"ER_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"ER_ALLWD_CASE_AMT")).alias("ER_ALLWD_CASE_AMT"),
					(when($"ER_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"ER_PAID_CASE_AMT")).alias("ER_PAID_CASE_AMT"),
					(when($"ER_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"ER_ALLWD_CMAD_AMT")).alias("ER_ALLWD_CMAD_AMT"),
					(when($"ER_CASE_NBR".isNull, lit(0)) otherwise ($"ER_CASE_NBR")).alias("ER_CASE_NBR"),
					(when($"ER_CMI_NBR".isNull, lit(0)) otherwise ($"ER_CMI_NBR")).alias("ER_CMI_NBR"),
//					(when($"ER_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"ER_ALWD_AMT_WITH_CMS")).alias("ER_ALWD_AMT_WITH_CMS"),
//					(when($"ER_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"ER_CMS_REIMBMNT_AMT")).alias("ER_CMS_REIMBMNT_AMT"),
					
					(when($"SRGY_ER_BILLD_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_BILLD_AMT")).alias("SRGY_ER_BILLD_AMT"),
					(when($"SRGY_ER_ALLWD_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_ALLWD_AMT")).alias("SRGY_ER_ALLWD_AMT"),
					(when($"SRGY_ER_PAID_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_PAID_AMT")).alias("SRGY_ER_PAID_AMT"),
					(when($"SRGY_ER_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_BILLD_CASE_AMT")).alias("SRGY_ER_BILLD_CASE_AMT"),
					(when($"SRGY_ER_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_ALLWD_CASE_AMT")).alias("SRGY_ER_ALLWD_CASE_AMT"),
					(when($"SRGY_ER_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_PAID_CASE_AMT")).alias("SRGY_ER_PAID_CASE_AMT"),
					(when($"SRGY_ER_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_ALLWD_CMAD_AMT")).alias("SRGY_ER_ALLWD_CMAD_AMT"),
					(when($"SRGY_ER_CASE_NBR".isNull, lit(0)) otherwise ($"SRGY_ER_CASE_NBR")).alias("SRGY_ER_CASE_NBR"),
					(when($"SRGY_ER_CMI_NBR".isNull, lit(0)) otherwise ($"SRGY_ER_CMI_NBR")).alias("SRGY_ER_CMI_NBR"),
//					(when($"SRGY_ER_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"SRGY_ER_ALWD_AMT_WITH_CMS")).alias("SRGY_ER_ALWD_AMT_WITH_CMS"),
//					(when($"SRGY_ER_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"SRGY_ER_CMS_REIMBMNT_AMT")).alias("SRGY_ER_CMS_REIMBMNT_AMT"),
					
					(when($"SRGY_BILLD_AMT".isNull, lit(0)) otherwise ($"SRGY_BILLD_AMT")).alias("SRGY_BILLD_AMT"),
					(when($"SRGY_ALLWD_AMT".isNull, lit(0)) otherwise ($"SRGY_ALLWD_AMT")).alias("SRGY_ALLWD_AMT"),
					(when($"SRGY_PAID_AMT".isNull, lit(0)) otherwise ($"SRGY_PAID_AMT")).alias("SRGY_PAID_AMT"),
					(when($"SRGY_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_BILLD_CASE_AMT")).alias("SRGY_BILLD_CASE_AMT"),
					(when($"SRGY_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_ALLWD_CASE_AMT")).alias("SRGY_ALLWD_CASE_AMT"),
					(when($"SRGY_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"SRGY_PAID_CASE_AMT")).alias("SRGY_PAID_CASE_AMT"),
					(when($"SRGY_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"SRGY_ALLWD_CMAD_AMT")).alias("SRGY_ALLWD_CMAD_AMT"),
					(when($"SRGY_CASE_NBR".isNull, lit(0)) otherwise ($"SRGY_CASE_NBR")).alias("SRGY_CASE_NBR"),
					(when($"SRGY_CMI_NBR".isNull, lit(0)) otherwise ($"SRGY_CMI_NBR")).alias("SRGY_CMI_NBR"),
//					(when($"SRGY_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"SRGY_ALWD_AMT_WITH_CMS")).alias("SRGY_ALWD_AMT_WITH_CMS"),
//					(when($"SRGY_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"SRGY_CMS_REIMBMNT_AMT")).alias("SRGY_CMS_REIMBMNT_AMT"),
					
					(when($"LAB_BILLD_AMT".isNull, lit(0)) otherwise ($"LAB_BILLD_AMT")).alias("LAB_BILLD_AMT"),
					(when($"LAB_ALLWD_AMT".isNull, lit(0)) otherwise ($"LAB_ALLWD_AMT")).alias("LAB_ALLWD_AMT"),
					(when($"LAB_PAID_AMT".isNull, lit(0)) otherwise ($"LAB_PAID_AMT")).alias("LAB_PAID_AMT"),
					(when($"LAB_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"LAB_BILLD_CASE_AMT")).alias("LAB_BILLD_CASE_AMT"),
					(when($"LAB_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"LAB_ALLWD_CASE_AMT")).alias("LAB_ALLWD_CASE_AMT"),
					(when($"LAB_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"LAB_PAID_CASE_AMT")).alias("LAB_PAID_CASE_AMT"),
					(when($"LAB_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"LAB_ALLWD_CMAD_AMT")).alias("LAB_ALLWD_CMAD_AMT"),
					(when($"LAB_CASE_NBR".isNull, lit(0)) otherwise ($"LAB_CASE_NBR")).alias("LAB_CASE_NBR"),
					(when($"LAB_CMI_NBR".isNull, lit(0)) otherwise ($"LAB_CMI_NBR")).alias("LAB_CMI_NBR"),
//					(when($"LAB_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"LAB_ALWD_AMT_WITH_CMS")).alias("LAB_ALWD_AMT_WITH_CMS"),
//					(when($"LAB_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"LAB_CMS_REIMBMNT_AMT")).alias("LAB_CMS_REIMBMNT_AMT"),
					
					(when($"RDLGY_SCANS_BILLD_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_BILLD_AMT")).alias("RDLGY_SCANS_BILLD_AMT"),
					(when($"RDLGY_SCANS_ALLWD_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_ALLWD_AMT")).alias("RDLGY_SCANS_ALLWD_AMT"),
					(when($"RDLGY_SCANS_PAID_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_PAID_AMT")).alias("RDLGY_SCANS_PAID_AMT"),
					(when($"RDLGY_SCANS_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_BILLD_CASE_AMT")).alias("RDLGY_SCANS_BILLD_CASE_AMT"),
					(when($"RDLGY_SCANS_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_ALLWD_CASE_AMT")).alias("RDLGY_SCANS_ALLWD_CASE_AMT"),
					(when($"RDLGY_SCANS_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_PAID_CASE_AMT")).alias("RDLGY_SCANS_PAID_CASE_AMT"),
					(when($"RDLGY_SCANS_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_ALLWD_CMAD_AMT")).alias("RDLGY_SCANS_ALLWD_CMAD_AMT"),
					(when($"RDLGY_SCANS_CASE_NBR".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CASE_NBR")).alias("RDLGY_SCANS_CASE_NBR"),
					(when($"RDLGY_SCANS_CMI_NBR".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CMI_NBR")).alias("RDLGY_SCANS_CMI_NBR"),
//					(when($"RDLGY_SCANS_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"RDLGY_SCANS_ALWD_AMT_WITH_CMS")).alias("RDLGY_SCANS_ALWD_AMT_WITH_CMS"),
//					(when($"RDLGY_SCANS_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CMS_REIMBMNT_AMT")).alias("RDLGY_SCANS_CMS_REIMBMNT_AMT"),
					
					(when($"RDLGY_OTHR_BILLD_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_BILLD_AMT")).alias("RDLGY_OTH_BILLD_AMT"),
					(when($"RDLGY_OTHR_ALLWD_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_ALLWD_AMT")).alias("RDLGY_OTH_ALLWD_AMT"),
					(when($"RDLGY_OTHR_PAID_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_PAID_AMT")).alias("RDLGY_OTH_PAID_AMT"),
					(when($"RDLGY_OTHR_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_BILLD_CASE_AMT")).alias("RDLGY_OTH_BILLD_CASE_AMT"),
					(when($"RDLGY_OTHR_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_ALLWD_CASE_AMT")).alias("RDLGY_OTH_ALLWD_CASE_AMT"),
					(when($"RDLGY_OTHR_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_PAID_CASE_AMT")).alias("RDLGY_OTH_PAID_CASE_AMT"),
					(when($"RDLGY_OTHR_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_ALLWD_CMAD_AMT")).alias("RDLGY_OTH_ALLWD_CMAD_AMT"),
					(when($"RDLGY_OTHR_CASE_NBR".isNull, lit(0)) otherwise ($"RDLGY_OTHR_CASE_NBR")).alias("RDLGY_OTH_CASE_NBR"),
					(when($"RDLGY_OTHR_CMI_NBR".isNull, lit(0)) otherwise ($"RDLGY_OTHR_CMI_NBR")).alias("RDLGY_OTH_CMI_NBR"),
//					(when($"RDLGY_OTHR_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"RDLGY_OTHR_ALWD_AMT_WITH_CMS")).alias("RDLGY_OTHR_ALWD_AMT_WITH_CMS"),
//					(when($"RDLGY_OTHR_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_CMS_REIMBMNT_AMT")).alias("RDLGY_OTHR_CMS_REIMBMNT_AMT"),
					
					(when($"DRUG_BILLD_AMT".isNull, lit(0)) otherwise ($"DRUG_BILLD_AMT")).alias("DRUG_BILLD_AMT"),
					(when($"DRUG_ALLWD_AMT".isNull, lit(0)) otherwise ($"DRUG_ALLWD_AMT")).alias("DRUG_ALLWD_AMT"),
					(when($"DRUG_PAID_AMT".isNull, lit(0)) otherwise ($"DRUG_PAID_AMT")).alias("DRUG_PAID_AMT"),
					(when($"DRUG_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"DRUG_BILLD_CASE_AMT")).alias("DRUG_BILLD_CASE_AMT"),
					(when($"DRUG_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"DRUG_ALLWD_CASE_AMT")).alias("DRUG_ALLWD_CASE_AMT"),
					(when($"DRUG_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"DRUG_PAID_CASE_AMT")).alias("DRUG_PAID_CASE_AMT"),
					(when($"DRUG_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"DRUG_ALLWD_CMAD_AMT")).alias("DRUG_ALLWD_CMAD_AMT"),
					(when($"DRUG_CASE_NBR".isNull, lit(0)) otherwise ($"DRUG_CASE_NBR")).alias("DRUG_CASE_NBR"),
					(when($"DRUG_CMI_NBR".isNull, lit(0)) otherwise ($"DRUG_CMI_NBR")).alias("DRUG_CMI_NBR"),
//					(when($"DRUG_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"DRUG_ALWD_AMT_WITH_CMS")).alias("DRUG_ALWD_AMT_WITH_CMS"),
//					(when($"DRUG_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"DRUG_CMS_REIMBMNT_AMT")).alias("DRUG_CMS_REIMBMNT_AMT"),
					
					(when($"OUTPAT_OTHR_BILLD_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_BILLD_AMT")).alias("OUTPAT_OTH_BILLD_AMT"),
					(when($"OUTPAT_OTHR_ALLWD_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_ALLWD_AMT")).alias("OUTPAT_OTH_ALLWD_AMT"),
					(when($"OUTPAT_OTHR_PAID_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_PAID_AMT")).alias("OUTPAT_OTH_PAID_AMT"),
					(when($"OUTPAT_OTHR_BILLD_CASE_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_BILLD_CASE_AMT")).alias("OUTPAT_OTH_BILLD_CASE_AMT"),
					(when($"OUTPAT_OTHR_ALLWD_CASE_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_ALLWD_CASE_AMT")).alias("OUTPAT_OTH_ALLWD_CASE_AMT"),
					(when($"OUTPAT_OTHR_PAID_CASE_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_PAID_CASE_AMT")).alias("OUTPAT_OTH_PAID_CASE_AMT"),
					(when($"OUTPAT_OTHR_ALLWD_CMAD_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_ALLWD_CMAD_AMT")).alias("OUTPAT_OTH_ALLWD_CMAD_AMT"),
					(when($"OUTPAT_OTHR_CASE_NBR".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CASE_NBR")).alias("OUTPAT_OTH_CASE_NBR"),
					(when($"OUTPAT_OTHR_CMI_NBR".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CMI_NBR")).alias("OUTPAT_OTH_CMI_NBR"),
//					(when($"OUTPAT_OTHR_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_ALWD_AMT_WITH_CMS")).alias("OUTPAT_OTHR_ALWD_AMT_WITH_CMS"),
//					(when($"OUTPAT_OTHR_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CMS_REIMBMNT_AMT")).alias("OUTPAT_OTHR_CMS_REIMBMNT_AMT"),
					
					(when($"INPAT_ALLWD_AMT".isNull, lit(0)) otherwise ($"INPAT_ALLWD_AMT")).alias("INPAT_ALLWD_AMT"),
					(when($"OUTPAT_ALLWD_AMT".isNull, lit(0)) otherwise ($"OUTPAT_ALLWD_AMT")).alias("OUTPAT_ALLWD_AMT"),
					(when($"TOTAL_ALLWD_AMT".isNull, lit(0)) otherwise ($"TOTAL_ALLWD_AMT")).alias("TOTAL_ALLWD_AMT"),
					(when($"TOTAL_ALWD_AMT_WITH_CMS".isNull, lit(0)) otherwise ($"TOTAL_ALWD_AMT_WITH_CMS")).alias("TOTAL_ALWD_AMT_WITH_CMS"),
					(when($"TOTAL_CMS_REIMBMNT_AMT".isNull, lit(0)) otherwise ($"TOTAL_CMS_REIMBMNT_AMT")).alias("TOTAL_CMS_REIMBMNT_AMT"),
					(when($"PROV_EXCLSN_CD".isNull, lit(0)) otherwise ($"PROV_EXCLSN_CD")).alias("PROV_EXCLSN_CD"),
					(when($"MTRNTY_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"MTRNTY_CMAD_CASE_CNT")).alias("MTRNTY_CMAD_CASE_CNT"),
					(when($"MTRNTY_CMAD_NBR".isNull, lit(0)) otherwise ($"MTRNTY_CMAD_NBR")).alias("MTRNTY_CMAD_NBR"),
					(when($"NICU_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"NICU_CMAD_CASE_CNT")).alias("NICU_CMAD_CASE_CNT"),
					(when($"NICU_CMAD_NBR".isNull, lit(0)) otherwise ($"NICU_CMAD_NBR")).alias("NICU_CMAD_NBR"),
					(when($"TRANSPLANT_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"TRANSPLANT_CMAD_CASE_CNT")).alias("TRANSPLANT_CMAD_CASE_CNT"),
					(when($"TRANSPLANT_CMAD_NBR".isNull, lit(0)) otherwise ($"TRANSPLANT_CMAD_NBR")).alias("TRANSPLANT_CMAD_NBR"),
					(when($"INPAT_OTH_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"INPAT_OTH_CMAD_CASE_CNT")).alias("INPAT_OTH_CMAD_CASE_CNT"),
					(when($"INPAT_OTH_CMAD_NBR".isNull, lit(0)) otherwise ($"INPAT_OTH_CMAD_NBR")).alias("INPAT_OTH_CMAD_NBR"),
					(when($"ER_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"ER_CMAD_CASE_CNT")).alias("ER_CMAD_CASE_CNT"),
					(when($"ER_CMAD_NBR".isNull, lit(0)) otherwise ($"ER_CMAD_NBR")).alias("ER_CMAD_CASE_NBR"),
					(when($"SRGY_ER_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"SRGY_ER_CMAD_CASE_CNT")).alias("SRGRY_ER_CMAD_CASE_CNT"),
					(when($"SRGY_ER_CMAD_NBR".isNull, lit(0)) otherwise ($"SRGY_ER_CMAD_NBR")).alias("SRGRY_ER_CMAD_CASE_NBR"),
					(when($"SRGY_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"SRGY_CMAD_CASE_CNT")).alias("SRGRY_CMAD_CASE_CNT"),
					(when($"SRGY_CMAD_NBR".isNull, lit(0)) otherwise ($"SRGY_CMAD_NBR")).alias("SRGRY_CMAD_CASE_NBR"),
					(when($"LAB_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"LAB_CMAD_CASE_CNT")).alias("LAB_CMAD_CASE_CNT"),
					(when($"LAB_CMAD_NBR".isNull, lit(0)) otherwise ($"LAB_CMAD_NBR")).alias("LAB_CMAD_CASE_NBR"),
					(when($"RDLGY_SCANS_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CMAD_CASE_CNT")).alias("RDLGY_SCANS_CMAD_CASE_CNT"),
					(when($"RDLGY_SCANS_CMAD_NBR".isNull, lit(0)) otherwise ($"RDLGY_SCANS_CMAD_NBR")).alias("RDLGY_SCANS_CMAD_CASE_NBR"),
					(when($"RDLGY_OTHR_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"RDLGY_OTHR_CMAD_CASE_CNT")).alias("RDLGY_OTHR_CMAD_CASE_CNT"),
					(when($"RDLGY_OTHR_CMAD_NBR".isNull, lit(0)) otherwise ($"RDLGY_OTHR_CMAD_NBR")).alias("RDLGY_OTHR_CMAD_CASE_NBR"),
					(when($"DRUG_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"DRUG_CMAD_CASE_CNT")).alias("DRUG_OTHR_CMAD_CASE_CNT"),
					(when($"DRUG_CMAD_NBR".isNull, lit(0)) otherwise ($"DRUG_CMAD_NBR")).alias("DRUG_OTHR_CMAD_CASE_NBR"),
					(when($"OUTPAT_OTHR_CMAD_CASE_CNT".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CMAD_CASE_CNT")).alias("OUTPAT_OTHR_CMAD_CASE_CNT"),
					(when($"OUTPAT_OTHR_CMAD_NBR".isNull, lit(0)) otherwise ($"OUTPAT_OTHR_CMAD_NBR")).alias("OUTPAT_OTHR_CMAD_CASE_NBR"))
    /*.filter(trim($"MBR_RATG_AREA_NM").notEqual("")).dropDuplicates(
          "PROV_ST_CD",
          "PROD_LVL3_DESC",
          "FUNDG_LVL2_DESC",
          "MBU_LVL4_DESC",
          "MBU_LVL3_DESC",
          "EXCHNG_IND_CD",
          "MBU_LVL2_DESC",
          "MBR_RATG_AREA_NM",
          "HOSP_NM",
          "HOSP_SYS_NM",
          "SUBMRKT_DESC",
          "PROV_CNTY_NM",
          "FCLTY_TYPE_DESC",
          "PAR_STTS_CD",
          "PROV_EXCLSN_CD") // Retrofit change - for Primary key constraints */ //======Jan-14-2019 :Remove drop duplicate as part of Design fix  =====//

	}
}

/* added comments*/
case class ndowrkHoppaMbrCntyInp(facilityAttributeProfileDfFil :Dataset[Row], inpatientSummaryDf: Dataset[Row], tabndowrkHoppaCmadBench: String, ndoZipSubmarketXxwalkDf: Dataset[Row], stagingHiveDB: String,rangeStartDate : String,rangeEndDate : String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
	  
	  
	    import spark.implicits._

			val ndowrkHoppaCmadBenchDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaCmadBench")
			val ndoZipSubmarketXxwalkDf1 = ndoZipSubmarketXxwalkDf.alias("ndoZipSubmarketXxwalkDf1")


			val joinndowrkOutPat = inpatientSummaryDf.withColumn("category", ((when(trim(inpatientSummaryDf("cat1")).===("Maternity") ||
			(trim(inpatientSummaryDf("MEDCR_ID")).isin("110005", "110008", "110161") &&
					trim(inpatientSummaryDf("FNL_DRG_CD")).isin("939", "940", "941", "951")), lit("MATERNITY"))
					when (trim(inpatientSummaryDf("cat1")).===("Newborn") && trim(inpatientSummaryDf("cat2")).=== ("Neonate"), lit("NICU"))
					when (trim(inpatientSummaryDf("cat1")).===("Transplant"), lit("TRANSPLANT"))
					otherwise (lit("IPOTH")))))
					
			val joinndowrk = joinndowrkOutPat
      .join(ndowrkHoppaCmadBenchDf, ndowrkHoppaCmadBenchDf("category_cd") === $"category", "inner")
			.join(facilityAttributeProfileDfFil, trim(facilityAttributeProfileDfFil("Medcr_ID")) === trim(inpatientSummaryDf("Medcr_ID")), "left_outer")
			.join(ndoZipSubmarketXxwalkDf1, trim(col("ndoZipSubmarketXxwalkDf1.Zip_Code")) === trim(inpatientSummaryDf("PROV_ZIP_CD")) 
			    && trim(col("ndoZipSubmarketXxwalkDf1.ST_CD")) === trim(inpatientSummaryDf("PROV_ST_NM")) && trim(ndoZipSubmarketXxwalkDf("CNTY_NM")) === trim(inpatientSummaryDf("PROV_County")), "left_outer")
      
      val joinndowrk2 = joinndowrk.
        filter(trim(inpatientSummaryDf("Prov_ST_NM")).isNotNull &&
					trim(inpatientSummaryDf("Prov_ST_NM")).!==("") &&
          trim(inpatientSummaryDf("MBUlvl2")).isNotNull &&
					trim(inpatientSummaryDf("prodlvl3")).isNotNull &&
          trim(inpatientSummaryDf("MCS")).!==("Y") &&
          trim(inpatientSummaryDf("liccd")) === ("01") &&
					trim(inpatientSummaryDf("MBU_CF_CD")).isin("MCKYGP", "SRLGUN", "SRLGCO", "SRLGVA", "MCOHGP", "SRLGNV", "MCINGP", "SRLGME", "SRLGNH",
							"SRLGKS", "SRLGGA", "SRLGNA", "SRNAHS", "SRLGMO", "SRLGWI", "SRLGNY", "SRLGCT",
							"SRLGBC", "MJCAMC", "SPCAMC") === false &&
          facilityAttributeProfileDfFil("factype").isin("Delete", "Long Term Care Hospital", "Snf", "Physical Rehab") === false &&
					inpatientSummaryDf("Inc_Month").between(rangeStartDate, rangeEndDate))
					
					val joinndowrkSel = joinndowrk2.
        select(
									inpatientSummaryDf("PROV_ST_NM"),
									(coalesce(inpatientSummaryDf("prodlvl3"), lit("UNK"))).alias("PROD_LVL3_DESC"),
									(coalesce(inpatientSummaryDf("fundlvl2"), lit("UNK"))).alias("FUNDG_LVL2_DESC"),
									(coalesce(inpatientSummaryDf("MBUlvl4"), lit("UNK"))).alias("MBU_LVL4_DESC"),
									(coalesce(inpatientSummaryDf("MBUlvl3"), lit("UNK"))).alias("MBU_LVL3_DESC"),
									(coalesce(inpatientSummaryDf("EXCHNG_IND_CD"), lit("N/A"))).alias("EXCHNG_IND_CD"),
									(coalesce(inpatientSummaryDf("MBUlvl2"), lit("UNK"))).alias("MBU_LVL2_DESC"),
									(when(inpatientSummaryDf("MBR_State").isNull || inpatientSummaryDf("MBR_County").isNull, lit("UNK")) 
									 when(inpatientSummaryDf("MBR_State")!==inpatientSummaryDf("PROV_ST_NM"), lit("UNK")) 
									 otherwise(inpatientSummaryDf("MBR_State"))).alias("MBR_ST_NM"),
									 (when(inpatientSummaryDf("MBR_State").isNull || inpatientSummaryDf("MBR_County").isNull, lit("UNK")) 
									 when(inpatientSummaryDf("MBR_State")!==inpatientSummaryDf("PROV_ST_NM"), lit("UNK")) 
									 otherwise(inpatientSummaryDf("MBR_County"))).alias("MBR_CNTY_NM"),
									//(coalesce(col("ndoZipSubmarketXxwalkDf_MBR.SUBMARKET_NM"), lit("UNK"))).alias("MBR_RATING_AREA"),
									inpatientSummaryDf("MEDCR_ID").alias("MEDCR_NBR"),
									(when(facilityAttributeProfileDfFil("Hospital").isNull, upper($"PROV_NM")) otherwise (upper(facilityAttributeProfileDfFil("Hospital")))).alias("HOSP_NM"),
									(coalesce(inpatientSummaryDf("System_ID"), lit("UNK"))).alias("HOSP_SYS_NM"),
		              (when(col("ndoZipSubmarketXxwalkDf1.RATING_AREA_DESC").isNotNull, col("ndoZipSubmarketXxwalkDf1.RATING_AREA_DESC")) when (facilityAttributeProfileDfFil("Rating_Area").isNotNull, facilityAttributeProfileDfFil("Rating_Area")) otherwise (lit("UNK"))).alias("SUBMRKT_DESC"),
									inpatientSummaryDf("prov_county").alias("PROV_CNTY_NM"),		              
									(coalesce(facilityAttributeProfileDfFil("factype"), lit("UNK"))).alias("FCLTY_TYPE_DESC"),
									inpatientSummaryDf("INN_CD").alias("PAR_STTS_CD"),
									(when(facilityAttributeProfileDfFil("Hospital").isNull, lit("B")) otherwise (lit("A"))).alias("PROV_EXCLSN_CD"),
									$"category",
									ndowrkHoppaCmadBenchDf("CMI_Bench"),
		            	(coalesce(inpatientSummaryDf("RPTG_NTWK_DESC"), lit("UNK"))).alias("RPTG_NTWK_DESC"),
									(when(inpatientSummaryDf("ALWD_AMT").isNull, lit(0)) otherwise (inpatientSummaryDf("ALWD_AMT"))).alias("ALWD_AMT"),
									(when(inpatientSummaryDf("BILLD_CHRG_AMT").isNull, lit(0)) otherwise (inpatientSummaryDf("BILLD_CHRG_AMT"))).alias("BILLD_CHRG_AMT"),
									(when(inpatientSummaryDf("PAID_AMT").isNull, lit(0)) otherwise (inpatientSummaryDf("PAID_AMT"))).alias("PAID_AMT"),
									(when(inpatientSummaryDf("cases").isNull, lit(0)) otherwise (inpatientSummaryDf("cases"))).alias("cases"),
									inpatientSummaryDf("ER_FLAG"),
									(when(inpatientSummaryDf("CMAD_CASES").isNull, lit(0)) otherwise (inpatientSummaryDf("CMAD_CASES"))).alias("CMAD_CASES"),
									(when(inpatientSummaryDf("CMAD_ALLOWED").isNull, lit(0)) otherwise (inpatientSummaryDf("CMAD_ALLOWED"))).alias("CMAD_ALLOWED"),
									(when(inpatientSummaryDf("CMAD").isNull, lit(0)) otherwise (inpatientSummaryDf("CMAD"))).alias("CMAD"),
									(when(inpatientSummaryDf("ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (inpatientSummaryDf("ALWD_AMT_WITH_CMS"))).alias("ALWD_AMT_WITH_CMS"),
									(when(inpatientSummaryDf("CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (inpatientSummaryDf("CMS_REIMBMNT_AMT"))).alias("CMS_REIMBMNT_AMT"))
									
									val joinndowrkGrp = joinndowrkSel.groupBy(
											$"PROV_ST_NM",
											$"PROD_LVL3_DESC",
											$"FUNDG_LVL2_DESC",
											$"MBU_LVL4_DESC",
											$"MBU_LVL3_DESC",
											$"EXCHNG_IND_CD",
											$"MBU_LVL2_DESC",
											$"MBR_ST_NM",
											$"MBR_CNTY_NM",
											$"MEDCR_NBR",
											$"HOSP_NM",
											$"HOSP_SYS_NM",
											$"SUBMRKT_DESC",
											$"PROV_CNTY_NM",
											$"FCLTY_TYPE_DESC",
											$"PAR_STTS_CD",
											$"PROV_EXCLSN_CD",
											$"category",
											ndowrkHoppaCmadBenchDf("CMI_Bench"),
	            				$"RPTG_NTWK_DESC").agg(
													sum($"ALWD_AMT").alias("ALWD_AMT"),
													sum($"BILLD_CHRG_AMT").alias("BILLD_CHRG_AMT"),
													sum($"PAID_AMT").alias("PAID_AMT"),
													(when(sum($"cases") === 0 || sum($"ALWD_AMT") === 0, lit(0)) otherwise (sum($"ALWD_AMT") / sum($"cases"))).alias("Alwd_Case_Amt"),
													(when(sum($"cases") === 0 || sum($"BILLD_CHRG_AMT") === 0, lit(0)) otherwise (sum($"BILLD_CHRG_AMT") / sum($"cases"))).alias("BILLD_CHRG_Case_AMT"),
													(when(sum($"cases") === 0 || sum($"PAID_AMT") === 0, lit(0)) otherwise (sum($"PAID_AMT") / sum($"cases"))).alias("PAID_Case_AMT"),
													sum($"CASES").alias("CASE_CNT"),
													sum(when($"ER_FLAG" === "Y", $"cases") otherwise (lit(0))).alias("ER_CASE_CNT"),
													sum($"CMAD_CASES").alias("CMAD_CASE_CNT"),
													sum($"CMAD_ALLOWED").alias("CMAD_ALLOWED"),
													(sum($"CMAD") / $"CMI_Bench").alias("CMAD"),
													sum($"ALWD_AMT_WITH_CMS").alias("ALWD_AMT_WITH_CMS"),
													sum($"CMS_REIMBMNT_AMT").alias("CMS_REIMBMNT_AMT")).withColumn("CMI", (when($"CMAD_CASE_CNT" === 0, lit(0)) otherwise (($"CMAD" / $"CMAD_CASE_CNT") / $"CMI_Bench")))
													
													 //println("The count of joinndowrkGrp is :" + joinndowrkGrp.count)

													joinndowrkGrp

	
			
	}
}

case class ndowrkHoppaMbrCntyOutp(facilityAttributeProfileDfFil :Dataset[Row], outpatientSummaryDf: Dataset[Row], tabndowrkHoppaCmadBench: String, ndoZipSubmarketXxwalkDf: Dataset[Row], stagingHiveDB: String,rangeStartDate : String,rangeEndDate : String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val ndowrkHoppaCmadBenchDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaCmadBench")
			
			val joinndowrkOutPat = outpatientSummaryDf.withColumn("category", (when(trim(outpatientSummaryDf("cat1")).===("ER / Urgent"), lit("ER"))
					when (trim(outpatientSummaryDf("cat1")).===("Lab"), lit("LAB"))
					when (trim(outpatientSummaryDf("cat1")).===("Radiology") && trim(outpatientSummaryDf("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms"), lit("RAD_SCANS"))
					when (trim(outpatientSummaryDf("cat1")).===("Radiology") && trim(outpatientSummaryDf("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms") === false, lit("RAD_OTHER"))
					when (trim(outpatientSummaryDf("cat1")).===("Surgery") && trim(outpatientSummaryDf("ER_Flag")) === "N", lit("SURGERY"))
					when (trim(outpatientSummaryDf("cat1")).===("Surgery") && trim(outpatientSummaryDf("ER_Flag")) === "Y", lit("ER_SURGERY"))
					when (trim(outpatientSummaryDf("cat1")).===("Other") && trim(outpatientSummaryDf("cat2")).===("Drugs-Infusion/Injection Therapy"), lit("DRUGS"))
					when (trim(outpatientSummaryDf("cat1")).===("Other") && trim(outpatientSummaryDf("cat2")).!==("Drugs-Infusion/Injection Therapy"), lit("OPOTH"))
					otherwise (concat(lit("OTHER: "), trim(outpatientSummaryDf("cat1")), lit(", "), trim(outpatientSummaryDf("cat2"))))))

			val joinndowrk = joinndowrkOutPat.join(ndowrkHoppaCmadBenchDf, ndowrkHoppaCmadBenchDf("category_cd") === $"category", "inner")
			.join(facilityAttributeProfileDfFil, trim(facilityAttributeProfileDfFil("Medcr_ID")) === trim(outpatientSummaryDf("Medcr_ID")), "left_outer")
			.join(ndoZipSubmarketXxwalkDf, trim(ndoZipSubmarketXxwalkDf("Zip_Code")) === trim(outpatientSummaryDf("PROV_ZIP_CD")) &&
			trim(ndoZipSubmarketXxwalkDf("ST_CD")) === trim(outpatientSummaryDf("PROV_ST_NM")) && trim(ndoZipSubmarketXxwalkDf("CNTY_NM")) === trim(outpatientSummaryDf("PROV_County")), "left_outer")
		
			.filter(trim(outpatientSummaryDf("Prov_ST_NM")).isNotNull &&
					trim(outpatientSummaryDf("Prov_ST_NM")).!==("") && trim(outpatientSummaryDf("MBUlvl2")).isNotNull &&
					trim(outpatientSummaryDf("prodlvl3")).isNotNull && trim(outpatientSummaryDf("MCS")).!==("Y") && trim(outpatientSummaryDf("liccd")) === ("01") &&
					trim(outpatientSummaryDf("MBU_CF_CD")).isin("MCKYGP", "SRLGUN", "SRLGCO", "SRLGVA", "MCOHGP", "SRLGNV", "MCINGP", "SRLGME", "SRLGNH",
							"SRLGKS", "SRLGGA", "SRLGNA", "SRNAHS", "SRLGMO", "SRLGWI", "SRLGNY", "SRLGCT",
							"SRLGBC", "MJCAMC", "SPCAMC") === false && facilityAttributeProfileDfFil("factype").isin("Delete", "Long Term Care Hospital", "Snf", "Physical Rehab") === false &&
							$"Inc_Month".between(rangeStartDate, rangeEndDate)).select(
									outpatientSummaryDf("PROV_ST_NM"),
									(coalesce(outpatientSummaryDf("prodlvl3"), lit("UNK"))).alias("PROD_LVL3_DESC"),
									(coalesce(outpatientSummaryDf("fundlvl2"), lit("UNK"))).alias("FUNDG_LVL2_DESC"),
									(coalesce(outpatientSummaryDf("MBUlvl4"), lit("UNK"))).alias("MBU_LVL4_DESC"),
									(coalesce(outpatientSummaryDf("MBUlvl3"), lit("UNK"))).alias("MBU_LVL3_DESC"),
									(coalesce(outpatientSummaryDf("EXCHNG_IND_CD"), lit("N/A"))).alias("EXCHNG_IND_CD"),
									(coalesce(outpatientSummaryDf("MBUlvl2"), lit("UNK"))).alias("MBU_LVL2_DESC"),
								(when(outpatientSummaryDf("MBR_State").isNull || outpatientSummaryDf("MBR_County").isNull, lit("UNK")) 
									 when(outpatientSummaryDf("MBR_State")!==outpatientSummaryDf("PROV_ST_NM"), lit("UNK")) 
									 otherwise(outpatientSummaryDf("MBR_State"))).alias("MBR_ST_NM"),
									 (when(outpatientSummaryDf("MBR_State").isNull || outpatientSummaryDf("MBR_County").isNull, lit("UNK")) 
									 when(outpatientSummaryDf("MBR_State")!==outpatientSummaryDf("PROV_ST_NM"), lit("UNK")) 
									 otherwise(outpatientSummaryDf("MBR_County"))).alias("MBR_CNTY_NM"),
									outpatientSummaryDf("MEDCR_ID").alias("MEDCR_NBR"),
									(when(facilityAttributeProfileDfFil("Hospital").isNull, upper($"PROV_NM")) otherwise (upper(facilityAttributeProfileDfFil("Hospital")))).alias("HOSP_NM"),
									(coalesce(outpatientSummaryDf("System_ID"), lit("UNK"))).alias("HOSP_SYS_NM"),
	            		(when(trim(ndoZipSubmarketXxwalkDf("RATING_AREA_DESC")).isNotNull, trim(ndoZipSubmarketXxwalkDf("RATING_AREA_DESC")))
											when (facilityAttributeProfileDfFil("Rating_Area").isNotNull, facilityAttributeProfileDfFil("Rating_Area")) otherwise (lit("UNK"))).alias("SUBMRKT_DESC"),
									outpatientSummaryDf("prov_county").alias("PROV_CNTY_NM"),
									(coalesce(facilityAttributeProfileDfFil("factype"), lit("UNK"))).alias("FCLTY_TYPE_DESC"),
									outpatientSummaryDf("INN_CD").alias("PAR_STTS_CD"),
									(when(facilityAttributeProfileDfFil("Hospital").isNull, lit("B")) otherwise (lit("A"))).alias("PROV_EXCLSN_CD"),
									(when(trim(outpatientSummaryDf("cat1")).===("Surgery") && trim(outpatientSummaryDf("ER_Flag")) === "Y", lit("ER_SURGERY"))
											when (trim(outpatientSummaryDf("cat1")).===("Surgery") && trim(outpatientSummaryDf("ER_Flag")) === "N", lit("SURGERY"))
											when (trim(outpatientSummaryDf("cat1")).===("ER / Urgent"), lit("ER"))
                      when (trim(outpatientSummaryDf("cat1")).===("Lab"), lit("LAB"))
											when (trim(outpatientSummaryDf("cat1")).===("Radiology") && trim(outpatientSummaryDf("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms"), lit("RAD_SCANS"))
											when (trim(outpatientSummaryDf("cat1")).===("Radiology") && trim(outpatientSummaryDf("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms") === false, lit("RAD_OTHER"))
											when (trim(outpatientSummaryDf("cat1")).===("Other") && trim(outpatientSummaryDf("cat2")).===("Drugs-Infusion/Injection Therapy"), lit("DRUGS"))
											when (trim(outpatientSummaryDf("cat1")).===("Other") && trim(outpatientSummaryDf("cat2")).!==("Drugs-Infusion/Injection Therapy"), lit("OPOTH"))
											otherwise (concat(lit("OTHER: "), trim(outpatientSummaryDf("cat1")), lit(", "), trim(outpatientSummaryDf("cat2"))))).alias("Category"),
									ndowrkHoppaCmadBenchDf("CMI_Bench"),
	            		(coalesce(outpatientSummaryDf("RPTG_NTWK_DESC"), lit("UNK"))).alias("RPTG_NTWK_DESC"),
									(when(outpatientSummaryDf("ALWD_AMT").isNull, lit(0)) otherwise (outpatientSummaryDf("ALWD_AMT"))).alias("ALWD_AMT"),
									(when(outpatientSummaryDf("BILLD_CHRG_AMT").isNull, lit(0)) otherwise (outpatientSummaryDf("BILLD_CHRG_AMT"))).alias("BILLD_CHRG_AMT"),
									(when(outpatientSummaryDf("PAID_AMT").isNull, lit(0)) otherwise (outpatientSummaryDf("PAID_AMT"))).alias("PAID_AMT"),
									(when(outpatientSummaryDf("cases").isNull, lit(0)) otherwise (outpatientSummaryDf("cases"))).alias("cases"),
									outpatientSummaryDf("ER_FLAG"),
									(when(outpatientSummaryDf("CMAD_CASES").isNull, lit(0)) otherwise (outpatientSummaryDf("CMAD_CASES"))).alias("CMAD_CASES"),
									(when(outpatientSummaryDf("CMAD_ALLOWED").isNull, lit(0)) otherwise (outpatientSummaryDf("CMAD_ALLOWED"))).alias("CMAD_ALLOWED"),
									(when(outpatientSummaryDf("CMAD").isNull, lit(0)) otherwise (outpatientSummaryDf("CMAD"))).alias("CMAD"),
										(when(outpatientSummaryDf("ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (outpatientSummaryDf("ALWD_AMT_WITH_CMS"))).alias("ALWD_AMT_WITH_CMS"),
									(when(outpatientSummaryDf("CMS_Allowed").isNull, lit(0)) otherwise (outpatientSummaryDf("CMS_Allowed"))).alias("CMS_REIMBMNT_AMT")).groupBy(
											$"PROV_ST_NM",
											$"PROD_LVL3_DESC",
											$"FUNDG_LVL2_DESC",
											$"MBU_LVL4_DESC",
											$"MBU_LVL3_DESC",
											$"EXCHNG_IND_CD",
											$"MBU_LVL2_DESC",
											$"MBR_ST_NM",
											$"MBR_CNTY_NM",
											$"MEDCR_NBR",
											$"HOSP_NM",
											$"HOSP_SYS_NM",
											$"SUBMRKT_DESC",
											$"PROV_CNTY_NM",
											$"FCLTY_TYPE_DESC",
											$"PAR_STTS_CD",
											$"PROV_EXCLSN_CD",
											$"Category",
											ndowrkHoppaCmadBenchDf("CMI_Bench"),
		            			$"RPTG_NTWK_DESC").agg(
													sum($"ALWD_AMT").alias("ALWD_AMT"),
													sum($"BILLD_CHRG_AMT").alias("BILLD_CHRG_AMT"),
													sum($"PAID_AMT").alias("PAID_AMT"),
													(when(sum($"cases") === 0 || sum($"ALWD_AMT") === 0, lit(0)) otherwise (sum($"ALWD_AMT") / sum($"cases"))).alias("Alwd_Case_Amt"),
													(when(sum($"cases") === 0 || sum($"BILLD_CHRG_AMT") === 0, lit(0)) otherwise (sum($"BILLD_CHRG_AMT") / sum($"cases"))).alias("BILLD_CHRG_Case_AMT"),
													(when(sum($"cases") === 0 || sum($"PAID_AMT") === 0, lit(0)) otherwise (sum($"PAID_AMT") / sum($"cases"))).alias("PAID_Case_AMT"),
													sum($"CASES").alias("CASE_CNT"),
													sum(when($"ER_FLAG" === "Y", $"cases") otherwise (lit(0))).alias("ER_CASE_CNT"),
													sum($"CMAD_CASES").alias("CMAD_CASE_CNT"),
													sum($"CMAD_ALLOWED").alias("CMAD_ALLOWED"),
													(sum($"CMAD") / $"CMI_Bench").alias("CMAD"),
													sum($"ALWD_AMT_WITH_CMS").alias("ALWD_AMT_WITH_CMS"),
													sum($"CMS_REIMBMNT_AMT").alias("CMS_REIMBMNT_AMT")).withColumn("CMI", (when($"CMAD_CASE_CNT" === 0, lit(0)) otherwise (($"CMAD" / $"CMAD_CASE_CNT") / $"CMI_Bench")))
													//println("The count of joinndowrk is :" + joinndowrk.count)
			joinndowrk
	}
}

case class ndowrkEhoppaMbrCntyBase(tabndowrkHoppaMbrCntyInpDf: String, tabndowrkHoppaMbrCntyOutpDf: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val ndowrkHoppaMbrCntyInpDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaMbrCntyInpDf")
			val ndowrkHoppaMbrCntyOutpDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaMbrCntyOutpDf")

					val ndowrkHoppaMbrCntyInpDfDist = ndowrkHoppaMbrCntyInpDf.select(
					$"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MBR_ST_NM",
					$"MBR_CNTY_NM",
					$"MEDCR_NBR",
				  $"HOSP_NM",
					$"HOSP_SYS_NM",
					$"SUBMRKT_DESC",
					$"PROV_CNTY_NM",
					$"FCLTY_TYPE_DESC",
					$"PAR_STTS_CD",
					$"PROV_EXCLSN_CD",
					$"RPTG_NTWK_DESC").distinct

			val ndowrkHoppaMbrCntyOutpDfDist = ndowrkHoppaMbrCntyOutpDf.select(
				$"PROV_ST_NM",
				$"PROD_LVL3_DESC",
				$"FUNDG_LVL2_DESC",
				$"MBU_LVL2_DESC",
				$"MBU_LVL4_DESC",
				$"EXCHNG_IND_CD",
				$"MBU_LVL3_DESC",
				$"MBR_ST_NM",
				$"MBR_CNTY_NM",
				$"MEDCR_NBR",
				$"HOSP_NM",
        $"HOSP_SYS_NM",
				$"SUBMRKT_DESC",
				$"PROV_CNTY_NM",
				$"FCLTY_TYPE_DESC",
				$"PAR_STTS_CD",
				$"PROV_EXCLSN_CD",
				$"RPTG_NTWK_DESC").distinct



		  val ndowrkHoppaProvListDist=ndowrkHoppaMbrCntyInpDfDist.union(ndowrkHoppaMbrCntyOutpDfDist).distinct()
		  
		   	val ndowrkHoppaMbrCntyInpDfCms = ndowrkHoppaMbrCntyInpDf.select(
					$"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MEDCR_NBR",
					$"PROV_CNTY_NM",
					$"PAR_STTS_CD",
					$"MBR_CNTY_NM",
					$"MBR_ST_NM",
					$"SUBMRKT_DESC",
					$"RPTG_NTWK_DESC",
					$"ALWD_AMT_WITH_CMS",
					$"CMS_REIMBMNT_AMT").groupBy(
					     $"PROV_ST_NM",
					     $"PROD_LVL3_DESC",
					     $"FUNDG_LVL2_DESC",
					     $"MBU_LVL2_DESC",
					     $"MBU_LVL4_DESC",
					     $"EXCHNG_IND_CD",
					     $"MBU_LVL3_DESC",
					     $"MEDCR_NBR",
					     $"PROV_CNTY_NM",
					     $"PAR_STTS_CD",
					     $"MBR_CNTY_NM",
					     $"MBR_ST_NM",
					     $"SUBMRKT_DESC",
					     $"RPTG_NTWK_DESC").agg(sum($"ALWD_AMT_WITH_CMS").as("ALWD_AMT_WITH_CMS"),
					                     sum($"CMS_REIMBMNT_AMT").as("CMS_REIMBMNT_AMT"))
					                     
					 	val ndowrkHoppaMbrCntyOutpDfCms = ndowrkHoppaMbrCntyOutpDf.select(
					$"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MEDCR_NBR",
					$"PROV_CNTY_NM",
					$"PAR_STTS_CD",
					$"MBR_CNTY_NM",
					$"MBR_ST_NM",
					$"SUBMRKT_DESC",
					$"RPTG_NTWK_DESC",
					$"ALWD_AMT_WITH_CMS",
					$"CMS_REIMBMNT_AMT"
					).groupBy(
					    $"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MEDCR_NBR",
					$"PROV_CNTY_NM",
					$"PAR_STTS_CD",
					$"MBR_CNTY_NM",
					$"MBR_ST_NM",
					$"SUBMRKT_DESC",
					$"RPTG_NTWK_DESC").agg(sum($"ALWD_AMT_WITH_CMS").as("ALWD_AMT_WITH_CMS"),
					                     sum($"CMS_REIMBMNT_AMT").as("CMS_REIMBMNT_AMT"))                    

			val ndowrkHoppaMbrCntyInpDf1 = ndowrkHoppaMbrCntyInpDf.alias("ndowrkHoppaMbrCntyInpDf1")
			val ndowrkHoppaMbrCntyInpDf2 = ndowrkHoppaMbrCntyInpDf.alias("ndowrkHoppaMbrCntyInpDf2")
			val ndowrkHoppaMbrCntyInpDf3 = ndowrkHoppaMbrCntyInpDf.alias("ndowrkHoppaMbrCntyInpDf3")
			val ndowrkHoppaMbrCntyInpDf4 = ndowrkHoppaMbrCntyInpDf.alias("ndowrkHoppaMbrCntyInpDf4")
			val ndowrkHoppaMbrCntyInpDf5 = ndowrkHoppaMbrCntyInpDf.alias("ndowrkHoppaMbrCntyInpDf5")
			val ndowrkHoppaMbrCntyOutpDf1 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf1")
			val ndowrkHoppaMbrCntyOutpDf2 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf2")
			val ndowrkHoppaMbrCntyOutpDf3 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf3")
			val ndowrkHoppaMbrCntyOutpDf4 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf4")
			val ndowrkHoppaMbrCntyOutpDf5 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf5")
			val ndowrkHoppaMbrCntyOutpDf6 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf6")
			val ndowrkHoppaMbrCntyOutpDf7 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf7")
			val ndowrkHoppaMbrCntyOutpDf8 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf8")

					val joinInpcms = ndowrkHoppaProvListDist.join(ndowrkHoppaMbrCntyInpDfCms,
			    ndowrkHoppaMbrCntyInpDfCms("PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
					ndowrkHoppaMbrCntyInpDfCms("PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
					ndowrkHoppaMbrCntyInpDfCms("FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
					ndowrkHoppaMbrCntyInpDfCms("MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
					ndowrkHoppaMbrCntyInpDfCms("MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
					ndowrkHoppaMbrCntyInpDfCms("EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
					ndowrkHoppaMbrCntyInpDfCms("MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
					ndowrkHoppaMbrCntyInpDfCms("MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
					ndowrkHoppaMbrCntyInpDfCms("PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
					ndowrkHoppaMbrCntyInpDfCms("PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
					ndowrkHoppaMbrCntyInpDfCms("MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
					ndowrkHoppaMbrCntyInpDfCms("MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
					ndowrkHoppaMbrCntyInpDfCms("SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
					ndowrkHoppaMbrCntyInpDfCms("RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC"),
					"left_outer")
				
					
					
				val joinoutpcms=joinInpcms.join(ndowrkHoppaMbrCntyOutpDfCms,
			    ndowrkHoppaMbrCntyOutpDfCms("PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
					ndowrkHoppaMbrCntyOutpDfCms("PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
					ndowrkHoppaMbrCntyOutpDfCms("FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
					ndowrkHoppaMbrCntyOutpDfCms("MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
					ndowrkHoppaMbrCntyOutpDfCms("MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
					ndowrkHoppaMbrCntyOutpDfCms("EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
					ndowrkHoppaMbrCntyOutpDfCms("MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
					ndowrkHoppaMbrCntyOutpDfCms("MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
					ndowrkHoppaMbrCntyOutpDfCms("PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
					ndowrkHoppaMbrCntyOutpDfCms("PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
					ndowrkHoppaMbrCntyOutpDfCms("MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
					ndowrkHoppaMbrCntyInpDfCms("MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
					ndowrkHoppaMbrCntyOutpDfCms("SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
					ndowrkHoppaMbrCntyOutpDfCms("RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC"),"left_outer")

					
					
			val joinInp1 = joinoutpcms.join(
					ndowrkHoppaMbrCntyInpDf1,
					col("ndowrkHoppaMbrCntyInpDf1.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
					col("ndowrkHoppaMbrCntyInpDf1.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
					col("ndowrkHoppaMbrCntyInpDf1.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
					col("ndowrkHoppaMbrCntyInpDf1.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
					col("ndowrkHoppaMbrCntyInpDf1.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
					col("ndowrkHoppaMbrCntyInpDf1.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
					col("ndowrkHoppaMbrCntyInpDf1.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
					col("ndowrkHoppaMbrCntyInpDf1.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
					col("ndowrkHoppaMbrCntyInpDf1.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
					col("ndowrkHoppaMbrCntyInpDf1.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
					col("ndowrkHoppaMbrCntyInpDf1.MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
					col("ndowrkHoppaMbrCntyInpDf1.MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
					col("ndowrkHoppaMbrCntyInpDf1.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
					//col("ndowrkHoppaMbrCntyInpDf1.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
					col("ndowrkHoppaMbrCntyInpDf1.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
					col("ndowrkHoppaMbrCntyInpDf1.category") === "MATERNITY", "left_outer")

			val joinInp2 = joinInp1.join(ndowrkHoppaMbrCntyInpDf2, col("ndowrkHoppaMbrCntyInpDf2.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyInpDf2.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf2.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf2.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf2.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf2.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyInpDf2.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf2.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyInpDf2.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyInpDf2.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyInpDf2.MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyInpDf2.MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
			col("ndowrkHoppaMbrCntyInpDf2.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			//col("ndowrkHoppaMbrCntyInpDf2.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaMbrCntyInpDf2.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf2.category") === "NICU", "left_outer")

			val joinInp5 = joinInp2.join(ndowrkHoppaMbrCntyInpDf5, col("ndowrkHoppaMbrCntyInpDf5.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyInpDf5.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf5.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf5.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf5.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf5.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyInpDf5.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf5.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyInpDf5.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyInpDf5.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyInpDf5.MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyInpDf5.MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
			col("ndowrkHoppaMbrCntyInpDf5.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			//col("ndowrkHoppaMbrCntyInpDf2.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaMbrCntyInpDf5.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf5.category") === "TRANSPLANT", "left_outer")
			
			val joinInp3 = joinInp5.join(ndowrkHoppaMbrCntyInpDf3, col("ndowrkHoppaMbrCntyInpDf3.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyInpDf3.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf3.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf3.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf3.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf3.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyInpDf3.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf3.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyInpDf3.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyInpDf3.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyInpDf3.MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyInpDf3.MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
			col("ndowrkHoppaMbrCntyInpDf3.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			//col("ndowrkHoppaMbrCntyInpDf3.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaMbrCntyInpDf3.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf3.category") === "IPOTH", "left_outer")

			val joinInp4 = joinInp3.join(ndowrkHoppaMbrCntyInpDf4, col("ndowrkHoppaMbrCntyInpDf4.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyInpDf4.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf4.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf4.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf4.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf4.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyInpDf4.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf4.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyInpDf4.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyInpDf4.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyInpDf4.MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyInpDf4.MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
			col("ndowrkHoppaMbrCntyInpDf4.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			//col("ndowrkHoppaMbrCntyInpDf4.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaMbrCntyInpDf4.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf4.category").isin("IPOTH", "NICU", "MATERNITY","TRANSPLANT"), "left_outer")

			
			
			val joinOut1 = joinInp4.join(ndowrkHoppaMbrCntyOutpDf1, col("ndowrkHoppaMbrCntyOutpDf1.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf1.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf1.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf1.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf1.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf1.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf1.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf1.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf1.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf1.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf1.MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf1.MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf1.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			//col("ndowrkHoppaMbrCntyOutpDf1.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf1.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf1.category") === "ER", "left_outer")

			val joinOut2 = joinOut1.join(ndowrkHoppaMbrCntyOutpDf2, col("ndowrkHoppaMbrCntyOutpDf2.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf2.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf2.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf2.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf2.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf2.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf2.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf2.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf2.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf2.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf2.MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf2.MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf2.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			//col("ndowrkHoppaMbrCntyOutpDf2.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf2.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf2.category") === "ER_SURGERY", "left_outer")

			val joinOut3 = joinOut2.join(ndowrkHoppaMbrCntyOutpDf3, col("ndowrkHoppaMbrCntyOutpDf3.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf3.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf3.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf3.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf3.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf3.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf3.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf3.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf3.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf3.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf3.MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf3.MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf3.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			//col("ndowrkHoppaMbrCntyOutpDf3.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf3.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf3.category") === "SURGERY", "left_outer")

			val joinOut4 = joinOut3.join(ndowrkHoppaMbrCntyOutpDf4, col("ndowrkHoppaMbrCntyOutpDf4.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf4.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf4.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf4.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf4.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf4.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf4.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf4.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf4.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf4.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf4.MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf4.MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf4.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			//col("ndowrkHoppaMbrCntyOutpDf4.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf4.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf4.category") === "LAB", "left_outer")

			val joinOut5 = joinOut4.join(ndowrkHoppaMbrCntyOutpDf5, col("ndowrkHoppaMbrCntyOutpDf5.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf5.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf5.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf5.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf5.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf5.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf5.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf5.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf5.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf5.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf5.MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf5.MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf5.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			//col("ndowrkHoppaMbrCntyOutpDf5.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf5.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf5.category") === "RAD_SCANS", "left_outer")

			val joinOut6 = joinOut5.join(ndowrkHoppaMbrCntyOutpDf6, col("ndowrkHoppaMbrCntyOutpDf6.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf6.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf6.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf6.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf6.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf6.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf6.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf6.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf6.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf6.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf6.MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf6.MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf6.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			//col("ndowrkHoppaMbrCntyOutpDf6.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf6.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf6.category") === "RAD_OTHER", "left_outer")

			val joinOut7 = joinOut6.join(ndowrkHoppaMbrCntyOutpDf7, col("ndowrkHoppaMbrCntyOutpDf7.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf7.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf7.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf7.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf7.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf7.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf7.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf7.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf7.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf7.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf7.MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf7.MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf7.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			//col("ndowrkHoppaMbrCntyOutpDf7.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf7.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf7.category") === "DRUGS", "left_outer")

			val joinOut8 = joinOut7.join(ndowrkHoppaMbrCntyOutpDf8, col("ndowrkHoppaMbrCntyOutpDf8.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf8.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf8.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf8.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf8.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf8.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf8.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf8.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf8.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf8.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf8.MBR_CNTY_NM") === ndowrkHoppaProvListDist("MBR_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf8.MBR_ST_NM") === ndowrkHoppaProvListDist("MBR_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf8.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			//col("ndowrkHoppaMbrCntyOutpDf8.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf8.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf8.category") === "OPOTH", "left_outer")

			
			
			val joinOut8Sel = joinOut8.select(
					ndowrkHoppaProvListDist("PROV_ST_NM"),
					ndowrkHoppaProvListDist("PROD_LVL3_DESC"),
					ndowrkHoppaProvListDist("FUNDG_LVL2_DESC"),
					ndowrkHoppaProvListDist("MBU_LVL4_DESC"),
					ndowrkHoppaProvListDist("MBU_LVL3_DESC"),
					ndowrkHoppaProvListDist("EXCHNG_IND_CD"),
					ndowrkHoppaProvListDist("MBU_LVL2_DESC"),
					//(when(ndowrkHoppaProvListDist("MBR_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM"), upper(ndowrkHoppaProvListDist("MBR_CNTY_NM"))) otherwise (lit("UNK"))).alias("MBR_CNTY_NM"),
          ndowrkHoppaProvListDist("MBR_CNTY_NM"),
					(concat(trim(ndowrkHoppaProvListDist("MEDCR_NBR")), lit(": "), trim(ndowrkHoppaProvListDist("HOSP_NM")))).alias("HOSP_NM"),
					ndowrkHoppaProvListDist("HOSP_SYS_NM"),
					ndowrkHoppaProvListDist("SUBMRKT_DESC"),
					ndowrkHoppaProvListDist("PROV_CNTY_NM"),
					ndowrkHoppaProvListDist("FCLTY_TYPE_DESC"),
					ndowrkHoppaProvListDist("PAR_STTS_CD"),
					ndowrkHoppaProvListDist("PROV_EXCLSN_CD"),
					ndowrkHoppaProvListDist("MEDCR_NBR"),
					ndowrkHoppaProvListDist("RPTG_NTWK_DESC"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.BILLD_CHRG_AMT"))).alias("MTRNTY_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT"))).alias("MTRNTY_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.PAID_AMT"))).alias("MTRNTY_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.BILLD_CHRG_Case_AMT"))).alias("MTRNTY_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.Alwd_Case_Amt"))).alias("MTRNTY_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.PAID_Case_AMT"))).alias("MTRNTY_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMAD_ALLOWED"))).alias("MTRNTY_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CASE_CNT"))).alias("MTRNTY_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMI"))).alias("MTRNTY_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMAD_CASE_CNT"))).alias("MTRNTY_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMAD"))).alias("MTRNTY_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS"))).alias("MTRNTY_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT"))).alias("MTRNTY_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaMbrCntyInpDf2.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.BILLD_CHRG_AMT"))).alias("NICU_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.ALWD_AMT"))).alias("NICU_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.PAID_AMT"))).alias("NICU_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.BILLD_CHRG_Case_AMT"))).alias("NICU_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.Alwd_Case_Amt"))).alias("NICU_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.PAID_Case_AMT"))).alias("NICU_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.CMAD_ALLOWED"))).alias("NICU_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.CASE_CNT"))).alias("NICU_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.CMI"))).alias("NICU_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.CMAD_CASE_CNT"))).alias("NICU_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.CMAD"))).alias("NICU_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS"))).alias("NICU_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT"))).alias("NICU_CMS_REIMBMNT_AMT"),

          (when(col("ndowrkHoppaMbrCntyInpDf5.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.BILLD_CHRG_AMT"))).alias("TRANSPLANT_BILLD_AMT"),
          (when(col("ndowrkHoppaMbrCntyInpDf5.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.ALWD_AMT"))).alias("TRANSPLANT_ALLWD_AMT"),
          (when(col("ndowrkHoppaMbrCntyInpDf5.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.PAID_AMT"))).alias("TRANSPLANT_PAID_AMT"),
          (when(col("ndowrkHoppaMbrCntyInpDf5.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.BILLD_CHRG_Case_AMT"))).alias("TRANSPLANT_BILLD_CASE_AMT"),
          (when(col("ndowrkHoppaMbrCntyInpDf5.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.Alwd_Case_Amt"))).alias("TRANSPLANT_ALLWD_CASE_AMT"),
          (when(col("ndowrkHoppaMbrCntyInpDf5.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.PAID_Case_AMT"))).alias("TRANSPLANT_PAID_CASE_AMT"),
          (when(col("ndowrkHoppaMbrCntyInpDf5.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.CMAD_ALLOWED"))).alias("TRANSPLANT_ALLWD_CMAD_AMT"),
          (when(col("ndowrkHoppaMbrCntyInpDf5.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.CASE_CNT"))).alias("TRANSPLANT_CASE_NBR"),
          (when(col("ndowrkHoppaMbrCntyInpDf5.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.CMI"))).alias("TRANSPLANT_CMI_NBR"),
          (when(col("ndowrkHoppaMbrCntyInpDf5.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.CMAD_CASE_CNT"))).alias("TRANSPLANT_CMAD_CASE_CNT"),
          (when(col("ndowrkHoppaMbrCntyInpDf5.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.CMAD"))).alias("TRANSPLANT_CMAD_NBR"),

					(when(col("ndowrkHoppaMbrCntyInpDf3.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.BILLD_CHRG_AMT"))).alias("INPAT_OTH_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.ALWD_AMT"))).alias("INPAT_OTH_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.PAID_AMT"))).alias("INPAT_OTH_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.BILLD_CHRG_Case_AMT"))).alias("INPAT_OTH_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.Alwd_Case_Amt"))).alias("INPAT_OTH_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.PAID_Case_AMT"))).alias("INPAT_OTH_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.CMAD_ALLOWED"))).alias("INPAT_OTH_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.CASE_CNT"))).alias("INPAT_OTH_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.CMI"))).alias("INPAT_OTH_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.CMAD_CASE_CNT"))).alias("INPAT_OTH_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.CMAD"))).alias("INPAT_OTH_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS"))).alias("INPAT_OTH_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT"))).alias("INPAT_OTH_CMS_REIMBMNT_AMT"),


					(when(col("ndowrkHoppaMbrCntyOutpDf1.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.BILLD_CHRG_AMT"))).alias("ER_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.ALWD_AMT"))).alias("ER_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.PAID_AMT"))).alias("ER_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.BILLD_CHRG_Case_AMT"))).alias("ER_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.Alwd_Case_Amt"))).alias("ER_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.PAID_Case_AMT"))).alias("ER_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.CMAD_ALLOWED"))).alias("ER_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.CASE_CNT"))).alias("ER_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.CMI"))).alias("ER_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.CMAD_CASE_CNT"))).alias("ER_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.CMAD"))).alias("ER_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS"))).alias("ER_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT"))).alias("ER_CMS_REIMBMNT_AMT"),


					(when(col("ndowrkHoppaMbrCntyOutpDf2.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.BILLD_CHRG_AMT"))).alias("SRGY_ER_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.ALWD_AMT"))).alias("SRGY_ER_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.PAID_AMT"))).alias("SRGY_ER_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.BILLD_CHRG_Case_AMT"))).alias("SRGY_ER_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.Alwd_Case_Amt"))).alias("SRGY_ER_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.PAID_Case_AMT"))).alias("SRGY_ER_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.CMAD_ALLOWED"))).alias("SRGY_ER_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.CASE_CNT"))).alias("SRGY_ER_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.CMI"))).alias("SRGY_ER_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.CMAD_CASE_CNT"))).alias("SRGY_ER_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.CMAD"))).alias("SRGY_ER_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS"))).alias("SRGY_ER_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT"))).alias("SRGY_ER_CMS_REIMBMNT_AMT"),


					(when(col("ndowrkHoppaMbrCntyOutpDf3.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.BILLD_CHRG_AMT"))).alias("SRGY_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.ALWD_AMT"))).alias("SRGY_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.PAID_AMT"))).alias("SRGY_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.BILLD_CHRG_Case_AMT"))).alias("SRGY_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.Alwd_Case_Amt"))).alias("SRGY_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.PAID_Case_AMT"))).alias("SRGY_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.CMAD_ALLOWED"))).alias("SRGY_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.CASE_CNT"))).alias("SRGY_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.CMI"))).alias("SRGY_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.CMAD_CASE_CNT"))).alias("SRGY_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.CMAD"))).alias("SRGY_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS"))).alias("SRGY_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT"))).alias("SRGY_CMS_REIMBMNT_AMT"),


					(when(col("ndowrkHoppaMbrCntyOutpDf4.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.BILLD_CHRG_AMT"))).alias("LAB_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.ALWD_AMT"))).alias("LAB_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.PAID_AMT"))).alias("LAB_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.BILLD_CHRG_Case_AMT"))).alias("LAB_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.Alwd_Case_Amt"))).alias("LAB_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.PAID_Case_AMT"))).alias("LAB_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.CMAD_ALLOWED"))).alias("LAB_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.CASE_CNT"))).alias("LAB_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.CMI"))).alias("LAB_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.CMAD_CASE_CNT"))).alias("LAB_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.CMAD"))).alias("LAB_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS"))).alias("LAB_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT"))).alias("LAB_CMS_REIMBMNT_AMT"),


					(when(col("ndowrkHoppaMbrCntyOutpDf5.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.BILLD_CHRG_AMT"))).alias("RDLGY_SCANS_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.ALWD_AMT"))).alias("RDLGY_SCANS_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.PAID_AMT"))).alias("RDLGY_SCANS_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.BILLD_CHRG_Case_AMT"))).alias("RDLGY_SCANS_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.Alwd_Case_Amt"))).alias("RDLGY_SCANS_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.PAID_Case_AMT"))).alias("RDLGY_SCANS_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.CMAD_ALLOWED"))).alias("RDLGY_SCANS_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.CASE_CNT"))).alias("RDLGY_SCANS_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.CMI"))).alias("RDLGY_SCANS_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.CMAD_CASE_CNT"))).alias("RDLGY_SCANS_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.CMAD"))).alias("RDLGY_SCANS_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS"))).alias("RDLGY_SCANS_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT"))).alias("RDLGY_SCANS_CMS_REIMBMNT_AMT"),


					(when(col("ndowrkHoppaMbrCntyOutpDf6.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.BILLD_CHRG_AMT"))).alias("RDLGY_OTHR_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.ALWD_AMT"))).alias("RDLGY_OTHR_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.PAID_AMT"))).alias("RDLGY_OTHR_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.BILLD_CHRG_Case_AMT"))).alias("RDLGY_OTHR_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.Alwd_Case_Amt"))).alias("RDLGY_OTHR_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.PAID_Case_AMT"))).alias("RDLGY_OTHR_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.CMAD_ALLOWED"))).alias("RDLGY_OTHR_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.CASE_CNT"))).alias("RDLGY_OTHR_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.CMI"))).alias("RDLGY_OTHR_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.CMAD_CASE_CNT"))).alias("RDLGY_OTHR_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.CMAD"))).alias("RDLGY_OTHR_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS"))).alias("RDLGY_OTHR_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT"))).alias("RDLGY_OTHR_CMS_REIMBMNT_AMT"),


					(when(col("ndowrkHoppaMbrCntyOutpDf7.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.BILLD_CHRG_AMT"))).alias("DRUG_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.ALWD_AMT"))).alias("DRUG_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.PAID_AMT"))).alias("DRUG_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.BILLD_CHRG_Case_AMT"))).alias("DRUG_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.Alwd_Case_Amt"))).alias("DRUG_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.PAID_Case_AMT"))).alias("DRUG_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.CMAD_ALLOWED"))).alias("DRUG_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.CASE_CNT"))).alias("DRUG_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.CMI"))).alias("DRUG_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.CMAD_CASE_CNT"))).alias("DRUG_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.CMAD"))).alias("DRUG_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS"))).alias("DRUG_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT"))).alias("DRUG_CMS_REIMBMNT_AMT"),


					(when(col("ndowrkHoppaMbrCntyOutpDf8.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.BILLD_CHRG_AMT"))).alias("OUTPAT_OTHR_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.ALWD_AMT"))).alias("OUTPAT_OTHR_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.PAID_AMT"))).alias("OUTPAT_OTHR_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.BILLD_CHRG_Case_AMT"))).alias("OUTPAT_OTHR_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.Alwd_Case_Amt"))).alias("OUTPAT_OTHR_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.PAID_Case_AMT"))).alias("OUTPAT_OTHR_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.CMAD_ALLOWED"))).alias("OUTPAT_OTHR_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.CASE_CNT"))).alias("OUTPAT_OTHR_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.CMI"))).alias("OUTPAT_OTHR_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.CMAD_CASE_CNT"))).alias("OUTPAT_OTHR_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.CMAD"))).alias("OUTPAT_OTHR_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS"))).alias("OUTPAT_OTHR_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT"))).alias("OUTPAT_OTHR_CMS_REIMBMNT_AMT"),


					col("ndowrkHoppaMbrCntyInpDf4.case_cnt"),
					col("ndowrkHoppaMbrCntyInpDf4.ER_Case_Cnt"),
						(when(ndowrkHoppaMbrCntyInpDfCms("ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (ndowrkHoppaMbrCntyInpDfCms("ALWD_AMT_WITH_CMS"))).alias("INP_ALWD_AMT_WITH_CMS"),
					(when(ndowrkHoppaMbrCntyOutpDfCms("ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (ndowrkHoppaMbrCntyOutpDfCms("ALWD_AMT_WITH_CMS"))).alias("OUTP_ALWD_AMT_WITH_CMS"),
					(when(ndowrkHoppaMbrCntyInpDfCms("CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (ndowrkHoppaMbrCntyInpDfCms("CMS_REIMBMNT_AMT"))).alias("INP_CMS_REIMBMNT_AMT"),
					(when(ndowrkHoppaMbrCntyOutpDfCms("CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (ndowrkHoppaMbrCntyOutpDfCms("CMS_REIMBMNT_AMT"))).alias("OUTP_CMS_REIMBMNT_AMT")
					).groupBy(
							ndowrkHoppaProvListDist("PROV_ST_NM"),
							ndowrkHoppaProvListDist("PROD_LVL3_DESC"),
							ndowrkHoppaProvListDist("FUNDG_LVL2_DESC"),
							ndowrkHoppaProvListDist("MBU_LVL4_DESC"),
							ndowrkHoppaProvListDist("MBU_LVL3_DESC"),
							ndowrkHoppaProvListDist("EXCHNG_IND_CD"),
							ndowrkHoppaProvListDist("MBU_LVL2_DESC"),
              ndowrkHoppaProvListDist("MBR_CNTY_NM"),
							$"HOSP_NM",
							ndowrkHoppaProvListDist("HOSP_SYS_NM"),
							ndowrkHoppaProvListDist("SUBMRKT_DESC"),
							ndowrkHoppaProvListDist("PROV_CNTY_NM"),
							ndowrkHoppaProvListDist("FCLTY_TYPE_DESC"),
							ndowrkHoppaProvListDist("PAR_STTS_CD"),
							ndowrkHoppaProvListDist("PROV_EXCLSN_CD"),
							ndowrkHoppaProvListDist("MEDCR_NBR"), 
							ndowrkHoppaProvListDist("RPTG_NTWK_DESC"),
							$"MTRNTY_BILLD_AMT",
							$"MTRNTY_ALLWD_AMT",
							$"MTRNTY_PAID_AMT",
							$"MTRNTY_BILLD_CASE_AMT",
							$"MTRNTY_ALLWD_CASE_AMT",
							$"MTRNTY_PAID_CASE_AMT",
							$"MTRNTY_ALLWD_CMAD_AMT",
							$"MTRNTY_CASE_NBR",
							$"MTRNTY_CMI_NBR",
							$"MTRNTY_CMAD_CASE_CNT",
							$"MTRNTY_CMAD_NBR",

							$"NICU_BILLD_AMT",
							$"NICU_ALLWD_AMT",
							$"NICU_PAID_AMT",
							$"NICU_BILLD_CASE_AMT",
							$"NICU_ALLWD_CASE_AMT",
							$"NICU_PAID_CASE_AMT",
							$"NICU_ALLWD_CMAD_AMT",
							$"NICU_CASE_NBR",
							$"NICU_CMI_NBR",
							$"NICU_CMAD_CASE_CNT",
							$"NICU_CMAD_NBR",
                            
							$"TRANSPLANT_BILLD_AMT",
							$"TRANSPLANT_ALLWD_AMT",
							$"TRANSPLANT_PAID_AMT",
							$"TRANSPLANT_BILLD_CASE_AMT",
							$"TRANSPLANT_ALLWD_CASE_AMT",
							$"TRANSPLANT_PAID_CASE_AMT",
							$"TRANSPLANT_ALLWD_CMAD_AMT",
							$"TRANSPLANT_CASE_NBR",
							$"TRANSPLANT_CMI_NBR",
							$"TRANSPLANT_CMAD_CASE_CNT",
							$"TRANSPLANT_CMAD_NBR",
							
							$"INPAT_OTH_BILLD_AMT",
							$"INPAT_OTH_ALLWD_AMT",
							$"INPAT_OTH_PAID_AMT",
							$"INPAT_OTH_BILLD_CASE_AMT",
							$"INPAT_OTH_ALLWD_CASE_AMT",
							$"INPAT_OTH_PAID_CASE_AMT",
							$"INPAT_OTH_ALLWD_CMAD_AMT",
							$"INPAT_OTH_CASE_NBR",
							$"INPAT_OTH_CMI_NBR",
							$"INPAT_OTH_CMAD_CASE_CNT",
							$"INPAT_OTH_CMAD_NBR",

							$"ER_BILLD_AMT",
							$"ER_ALLWD_AMT",
							$"ER_PAID_AMT",
							$"ER_BILLD_CASE_AMT",
							$"ER_ALLWD_CASE_AMT",
							$"ER_PAID_CASE_AMT",
							$"ER_ALLWD_CMAD_AMT",
							$"ER_CASE_NBR",
							$"ER_CMI_NBR",
							$"ER_CMAD_CASE_CNT",
							$"ER_CMAD_NBR",

							$"SRGY_ER_BILLD_AMT",
							$"SRGY_ER_ALLWD_AMT",
							$"SRGY_ER_PAID_AMT",
							$"SRGY_ER_BILLD_CASE_AMT",
							$"SRGY_ER_ALLWD_CASE_AMT",
							$"SRGY_ER_PAID_CASE_AMT",
							$"SRGY_ER_ALLWD_CMAD_AMT",
							$"SRGY_ER_CASE_NBR",
							$"SRGY_ER_CMI_NBR",
							$"SRGY_ER_CMAD_CASE_CNT",
							$"SRGY_ER_CMAD_NBR",

							$"SRGY_BILLD_AMT",
							$"SRGY_ALLWD_AMT",
							$"SRGY_PAID_AMT",
							$"SRGY_BILLD_CASE_AMT",
							$"SRGY_ALLWD_CASE_AMT",
							$"SRGY_PAID_CASE_AMT",
							$"SRGY_ALLWD_CMAD_AMT",
							$"SRGY_CASE_NBR",
							$"SRGY_CMI_NBR",
							$"SRGY_CMAD_CASE_CNT",
							$"SRGY_CMAD_NBR",

							$"LAB_BILLD_AMT",
							$"LAB_ALLWD_AMT",
							$"LAB_PAID_AMT",
							$"LAB_BILLD_CASE_AMT",
							$"LAB_ALLWD_CASE_AMT",
							$"LAB_PAID_CASE_AMT",
							$"LAB_ALLWD_CMAD_AMT",
							$"LAB_CASE_NBR",
							$"LAB_CMI_NBR",
							$"LAB_CMAD_CASE_CNT",
							$"LAB_CMAD_NBR",

							$"RDLGY_SCANS_BILLD_AMT",
							$"RDLGY_SCANS_ALLWD_AMT",
							$"RDLGY_SCANS_PAID_AMT",
							$"RDLGY_SCANS_BILLD_CASE_AMT",
							$"RDLGY_SCANS_ALLWD_CASE_AMT",
							$"RDLGY_SCANS_PAID_CASE_AMT",
							$"RDLGY_SCANS_ALLWD_CMAD_AMT",
							$"RDLGY_SCANS_CASE_NBR",
							$"RDLGY_SCANS_CMI_NBR",
							$"RDLGY_SCANS_CMAD_CASE_CNT",
							$"RDLGY_SCANS_CMAD_NBR",

							$"RDLGY_OTHR_BILLD_AMT",
							$"RDLGY_OTHR_ALLWD_AMT",
							$"RDLGY_OTHR_PAID_AMT",
							$"RDLGY_OTHR_BILLD_CASE_AMT",
							$"RDLGY_OTHR_ALLWD_CASE_AMT",
							$"RDLGY_OTHR_PAID_CASE_AMT",
							$"RDLGY_OTHR_ALLWD_CMAD_AMT",
							$"RDLGY_OTHR_CASE_NBR",
							$"RDLGY_OTHR_CMI_NBR",
							$"RDLGY_OTHR_CMAD_CASE_CNT",
							$"RDLGY_OTHR_CMAD_NBR",

							$"DRUG_BILLD_AMT",
							$"DRUG_ALLWD_AMT",
							$"DRUG_PAID_AMT",
							$"DRUG_BILLD_CASE_AMT",
							$"DRUG_ALLWD_CASE_AMT",
							$"DRUG_PAID_CASE_AMT",
							$"DRUG_ALLWD_CMAD_AMT",
							$"DRUG_CASE_NBR",
							$"DRUG_CMI_NBR",
							$"DRUG_CMAD_CASE_CNT",
							$"DRUG_CMAD_NBR",

							$"OUTPAT_OTHR_BILLD_AMT",
							$"OUTPAT_OTHR_ALLWD_AMT",
							$"OUTPAT_OTHR_PAID_AMT",
							$"OUTPAT_OTHR_BILLD_CASE_AMT",
							$"OUTPAT_OTHR_ALLWD_CASE_AMT",
							$"OUTPAT_OTHR_PAID_CASE_AMT",
							$"OUTPAT_OTHR_ALLWD_CMAD_AMT",
							$"OUTPAT_OTHR_CASE_NBR",
							$"OUTPAT_OTHR_CMI_NBR",
							$"OUTPAT_OTHR_CMAD_CASE_CNT",
							$"OUTPAT_OTHR_CMAD_NBR",
							$"INP_ALWD_AMT_WITH_CMS",
							$"OUTP_ALWD_AMT_WITH_CMS",
							$"INP_CMS_REIMBMNT_AMT",
							$"OUTP_CMS_REIMBMNT_AMT").agg(
									(when(sum(col("ndowrkHoppaMbrCntyInpDf4.case_cnt")) > 0, ((sum(col("ndowrkHoppaMbrCntyInpDf4.ER_Case_Cnt")).cast(DecimalType(18, 4)) / sum(col("ndowrkHoppaMbrCntyInpDf4.case_cnt")).cast(DecimalType(18, 4))) * 100).cast(DecimalType(18, 4)))
											otherwise (lit(0))).alias("ER_PCT"))
			.withColumn("INPAT_ALLWD_AMT", ($"MTRNTY_ALLWD_AMT" + $"NICU_ALLWD_AMT" + $"INPAT_OTH_ALLWD_AMT" + $"TRANSPLANT_ALLWD_AMT"))
			.withColumn("OUTPAT_ALLWD_AMT", ($"ER_ALLWD_AMT" + $"SRGY_ER_ALLWD_AMT" + $"SRGY_ALLWD_AMT" + $"LAB_ALLWD_AMT" + $"RDLGY_SCANS_ALLWD_AMT" + $"RDLGY_OTHR_ALLWD_AMT" + $"DRUG_ALLWD_AMT" + $"OUTPAT_OTHR_ALLWD_AMT"))
			.withColumn("TOTAL_ALLWD_AMT", ($"INPAT_ALLWD_AMT" + $"OUTPAT_ALLWD_AMT"))
			.withColumn("TOTAL_ALWD_AMT_WITH_CMS", ($"INP_ALWD_AMT_WITH_CMS".cast(DecimalType(18, 2)) + $"OUTP_ALWD_AMT_WITH_CMS".cast(DecimalType(18, 2))))
			.withColumn("TOTAL_CMS_REIMBMNT_AMT", ($"INP_CMS_REIMBMNT_AMT".cast(DecimalType(18, 2)) + $"OUTP_CMS_REIMBMNT_AMT".cast(DecimalType(18, 2))))
			.drop($"INP_ALWD_AMT_WITH_CMS")
			.drop($"OUTP_ALWD_AMT_WITH_CMS")
			.drop($"INP_CMS_REIMBMNT_AMT")
			.drop($"OUTP_CMS_REIMBMNT_AMT")
			
			
			
			val joinOut8SelFil = joinOut8Sel.filter($"TOTAL_ALLWD_AMT" > 0 )
			
		//	println("The count of joinOut8SelFil is :" + joinOut8SelFil.count)
			
			joinOut8SelFil
	}
}

case class ndowrkHoppaMbrRatgInp(facilityAttributeProfileDfFil :Dataset[Row], inpatientSummaryDf: Dataset[Row], tabndowrkHoppaCmadBench: String, ndoZipSubmarketXxwalkDf: Dataset[Row], stagingHiveDB: String,rangeStartDate : String,rangeEndDate : String) extends NDOMonad[Dataset[Row]] with Logging {
		override def run(spark: SparkSession): Dataset[Row] = {import spark.implicits._

			val ndowrkHoppaCmadBenchDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaCmadBench")
			val ndoZipSubmarketXxwalkDf1 = ndoZipSubmarketXxwalkDf.alias("ndoZipSubmarketXxwalkDf1")
      val ndoZipSubmarketXxwalkDf_MBR = ndoZipSubmarketXxwalkDf.alias("ndoZipSubmarketXxwalkDf_MBR")


			val joinndowrkOutPat = inpatientSummaryDf.withColumn("category", ((when(trim(inpatientSummaryDf("cat1")).===("Maternity") ||
			(trim(inpatientSummaryDf("MEDCR_ID")).isin("110005", "110008", "110161") &&
					trim(inpatientSummaryDf("FNL_DRG_CD")).isin("939", "940", "941", "951")), lit("MATERNITY"))
					when (trim(inpatientSummaryDf("cat1")).===("Newborn") && trim(inpatientSummaryDf("cat2")).=== ("Neonate"), lit("NICU"))
					when (trim(inpatientSummaryDf("cat1")).===("Transplant"), lit("TRANSPLANT"))
					otherwise (lit("IPOTH")))))
					
			val joinndowrk = joinndowrkOutPat
      .join(ndowrkHoppaCmadBenchDf, ndowrkHoppaCmadBenchDf("category_cd") === $"category", "inner")
			.join(facilityAttributeProfileDfFil, trim(facilityAttributeProfileDfFil("Medcr_ID")) === trim(inpatientSummaryDf("Medcr_ID")), "left_outer")
			.join(ndoZipSubmarketXxwalkDf1, trim(col("ndoZipSubmarketXxwalkDf1.Zip_Code")) === trim(inpatientSummaryDf("PROV_ZIP_CD")) 
			    && trim(col("ndoZipSubmarketXxwalkDf1.ST_CD")) === trim(inpatientSummaryDf("PROV_ST_NM")) && trim(col("ndoZipSubmarketXxwalkDf1.CNTY_NM")) === trim(inpatientSummaryDf("PROV_County")), "left_outer")
      .join(ndoZipSubmarketXxwalkDf_MBR,trim(col("ndoZipSubmarketXxwalkDf_MBR.Zip_Code")) === trim(inpatientSummaryDf("MBR_ZIP_CD")) 
          && trim(col("ndoZipSubmarketXxwalkDf_MBR.ST_CD")) === trim(inpatientSummaryDf("MBR_State")) && trim(col("ndoZipSubmarketXxwalkDf_MBR.CNTY_NM")) === trim(inpatientSummaryDf("MBR_County")), "left_outer")
      
      val joinndowrk2 = joinndowrk.
        filter(trim(inpatientSummaryDf("Prov_ST_NM")).isNotNull &&
					trim(inpatientSummaryDf("Prov_ST_NM")).!==("") &&
          trim(inpatientSummaryDf("MBUlvl2")).isNotNull &&
					trim(inpatientSummaryDf("prodlvl3")).isNotNull &&
          trim(inpatientSummaryDf("MCS")).!==("Y") &&
          trim(inpatientSummaryDf("liccd")) === ("01") &&
					trim(inpatientSummaryDf("MBU_CF_CD")).isin("MCKYGP", "SRLGUN", "SRLGCO", "SRLGVA", "MCOHGP", "SRLGNV", "MCINGP", "SRLGME", "SRLGNH",
							"SRLGKS", "SRLGGA", "SRLGNA", "SRNAHS", "SRLGMO", "SRLGWI", "SRLGNY", "SRLGCT",
							"SRLGBC", "MJCAMC", "SPCAMC") === false &&
          facilityAttributeProfileDfFil("factype").isin("Delete", "Long Term Care Hospital", "Snf", "Physical Rehab") === false &&
					inpatientSummaryDf("Inc_Month").between(rangeStartDate, rangeEndDate))
					
					val joinndowrkSel = joinndowrk2.
        select(
									inpatientSummaryDf("PROV_ST_NM"),
									(coalesce(inpatientSummaryDf("prodlvl3"), lit("UNK"))).alias("PROD_LVL3_DESC"),
									(coalesce(inpatientSummaryDf("fundlvl2"), lit("UNK"))).alias("FUNDG_LVL2_DESC"),
									(coalesce(inpatientSummaryDf("MBUlvl4"), lit("UNK"))).alias("MBU_LVL4_DESC"),
									(coalesce(inpatientSummaryDf("MBUlvl3"), lit("UNK"))).alias("MBU_LVL3_DESC"),
									(coalesce(inpatientSummaryDf("EXCHNG_IND_CD"), lit("N/A"))).alias("EXCHNG_IND_CD"),
									(coalesce(inpatientSummaryDf("MBUlvl2"), lit("UNK"))).alias("MBU_LVL2_DESC"),
//									upper((coalesce(col("ndoZipSubmarketXxwalkDf_MBR.SUBMARKET_NM"), lit("UNK")))).alias("MBR_RATING_AREA"),
            			upper((when(col("ndoZipSubmarketXxwalkDf_MBR.RATING_AREA_DESC").isNull, lit("UNK"))
									    when(inpatientSummaryDf("MBR_State")!==inpatientSummaryDf("PROV_ST_NM"), lit("UNK"))
									    otherwise (col("ndoZipSubmarketXxwalkDf_MBR.RATING_AREA_DESC")))).alias("MBR_RATING_AREA"),
									inpatientSummaryDf("MEDCR_ID").alias("MEDCR_NBR"),
									(when(facilityAttributeProfileDfFil("Hospital").isNull, upper($"PROV_NM")) otherwise (upper(facilityAttributeProfileDfFil("Hospital")))).alias("HOSP_NM"),
									(coalesce(inpatientSummaryDf("System_ID"), lit("UNK"))).alias("HOSP_SYS_NM"),
	            		(when(col("ndoZipSubmarketXxwalkDf1.RATING_AREA_DESC").isNotNull, col("ndoZipSubmarketXxwalkDf1.RATING_AREA_DESC")) when (facilityAttributeProfileDfFil("Rating_Area").isNotNull, facilityAttributeProfileDfFil("Rating_Area")) otherwise (lit("UNK"))).alias("SUBMRKT_DESC"),
									inpatientSummaryDf("prov_county").alias("PROV_CNTY_NM"),	            		
									(coalesce(facilityAttributeProfileDfFil("factype"), lit("UNK"))).alias("FCLTY_TYPE_DESC"),
									inpatientSummaryDf("INN_CD").alias("PAR_STTS_CD"),
									(when(facilityAttributeProfileDfFil("Hospital").isNull, lit("B")) otherwise (lit("A"))).alias("PROV_EXCLSN_CD"),
									$"category",
									ndowrkHoppaCmadBenchDf("CMI_Bench"),
	                (coalesce(inpatientSummaryDf("RPTG_NTWK_DESC"), lit("UNK"))).alias("RPTG_NTWK_DESC"),							
									(when(inpatientSummaryDf("ALWD_AMT").isNull, lit(0)) otherwise (inpatientSummaryDf("ALWD_AMT"))).alias("ALWD_AMT"),
									(when(inpatientSummaryDf("BILLD_CHRG_AMT").isNull, lit(0)) otherwise (inpatientSummaryDf("BILLD_CHRG_AMT"))).alias("BILLD_CHRG_AMT"),
									(when(inpatientSummaryDf("PAID_AMT").isNull, lit(0)) otherwise (inpatientSummaryDf("PAID_AMT"))).alias("PAID_AMT"),
									(when(inpatientSummaryDf("cases").isNull, lit(0)) otherwise (inpatientSummaryDf("cases"))).alias("cases"),
									inpatientSummaryDf("ER_FLAG"),
									(when(inpatientSummaryDf("CMAD_CASES").isNull, lit(0)) otherwise (inpatientSummaryDf("CMAD_CASES"))).alias("CMAD_CASES"),
									(when(inpatientSummaryDf("CMAD_ALLOWED").isNull, lit(0)) otherwise (inpatientSummaryDf("CMAD_ALLOWED"))).alias("CMAD_ALLOWED"),
									(when(inpatientSummaryDf("CMAD").isNull, lit(0)) otherwise (inpatientSummaryDf("CMAD"))).alias("CMAD"),
									(when(inpatientSummaryDf("ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (inpatientSummaryDf("ALWD_AMT_WITH_CMS"))).alias("ALWD_AMT_WITH_CMS"),
									(when(inpatientSummaryDf("CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (inpatientSummaryDf("CMS_REIMBMNT_AMT"))).alias("CMS_REIMBMNT_AMT"))
									
									val joinndowrkGrp = joinndowrkSel.groupBy(
											$"PROV_ST_NM",
											$"PROD_LVL3_DESC",
											$"FUNDG_LVL2_DESC",
											$"MBU_LVL4_DESC",
											$"MBU_LVL3_DESC",
											$"EXCHNG_IND_CD",
											$"MBU_LVL2_DESC",
											$"MBR_RATING_AREA",
											$"MEDCR_NBR",
											$"HOSP_NM",
											$"HOSP_SYS_NM",
											$"SUBMRKT_DESC",
											$"PROV_CNTY_NM",
											$"FCLTY_TYPE_DESC",
											$"PAR_STTS_CD",
											$"PROV_EXCLSN_CD",
											$"category",
											ndowrkHoppaCmadBenchDf("CMI_Bench"),
	            				$"RPTG_NTWK_DESC").agg(
													sum($"ALWD_AMT").alias("ALWD_AMT"),
													sum($"BILLD_CHRG_AMT").alias("BILLD_CHRG_AMT"),
													sum($"PAID_AMT").alias("PAID_AMT"),
													(when(sum($"cases") === 0 || sum($"ALWD_AMT") === 0, lit(0)) otherwise (sum($"ALWD_AMT") / sum($"cases"))).alias("Alwd_Case_Amt"),
													(when(sum($"cases") === 0 || sum($"BILLD_CHRG_AMT") === 0, lit(0)) otherwise (sum($"BILLD_CHRG_AMT") / sum($"cases"))).alias("BILLD_CHRG_Case_AMT"),
													(when(sum($"cases") === 0 || sum($"PAID_AMT") === 0, lit(0)) otherwise (sum($"PAID_AMT") / sum($"cases"))).alias("PAID_Case_AMT"),
													sum($"CASES").alias("CASE_CNT"),
													sum(when($"ER_FLAG" === "Y", $"cases") otherwise (lit(0))).alias("ER_CASE_CNT"),
													sum($"CMAD_CASES").alias("CMAD_CASE_CNT"),
													sum($"CMAD_ALLOWED").alias("CMAD_ALLOWED"),
													(sum($"CMAD") / $"CMI_Bench").alias("CMAD"),
													sum($"ALWD_AMT_WITH_CMS").alias("ALWD_AMT_WITH_CMS"),
													sum($"CMS_REIMBMNT_AMT").alias("CMS_REIMBMNT_AMT")).withColumn("CMI", (when($"CMAD_CASE_CNT" === 0, lit(0)) otherwise (($"CMAD" / $"CMAD_CASE_CNT") / $"CMI_Bench")))
													
													joinndowrkGrp

	}
}


case class ndowrkHoppaMbrRatgOutp(facilityAttributeProfileDfFil :Dataset[Row], outpatientSummaryDf: Dataset[Row], tabndowrkHoppaCmadBench: String, ndoZipSubmarketXxwalkDf: Dataset[Row], stagingHiveDB: String,rangeStartDate : String,rangeEndDate : String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val ndowrkHoppaCmadBenchDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaCmadBench")
			val ndoZipSubmarketXxwalkDf1 = ndoZipSubmarketXxwalkDf.alias("ndoZipSubmarketXxwalkDf1")
      
      val ndoZipSubmarketXxwalkDf_MBR_sel= ndoZipSubmarketXxwalkDf.select(
          $"st_cd",$"cnty_nm",substring($"Zip_Code",1,3).alias("zip_3"),$"RATING_AREA_DESC").distinct
          
          val ndoZipSubmarketXxwalkDf_MBR = ndoZipSubmarketXxwalkDf_MBR_sel.alias("ndoZipSubmarketXxwalkDf_MBR")

			val joinndowrkOutPat = outpatientSummaryDf.withColumn("category", (when(trim(outpatientSummaryDf("cat1")).===("ER / Urgent"), lit("ER"))
					when (trim(outpatientSummaryDf("cat1")).===("Lab"), lit("LAB"))
					when (trim(outpatientSummaryDf("cat1")).===("Radiology") && trim(outpatientSummaryDf("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms"), lit("RAD_SCANS"))
					when (trim(outpatientSummaryDf("cat1")).===("Radiology") && trim(outpatientSummaryDf("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms") === false, lit("RAD_OTHER"))
					when (trim(outpatientSummaryDf("cat1")).===("Surgery") && trim(outpatientSummaryDf("ER_Flag")) === "N", lit("SURGERY"))
					when (trim(outpatientSummaryDf("cat1")).===("Surgery") && trim(outpatientSummaryDf("ER_Flag")) === "Y", lit("ER_SURGERY"))
					when (trim(outpatientSummaryDf("cat1")).===("Other") && trim(outpatientSummaryDf("cat2")).===("Drugs-Infusion/Injection Therapy"), lit("DRUGS"))
					when (trim(outpatientSummaryDf("cat1")).===("Other") && trim(outpatientSummaryDf("cat2")).!==("Drugs-Infusion/Injection Therapy"), lit("OPOTH"))
					otherwise (concat(lit("OTHER: "), trim(outpatientSummaryDf("cat1")), lit(", "), trim(outpatientSummaryDf("cat2"))))))

			val joinndowrk = joinndowrkOutPat.join(ndowrkHoppaCmadBenchDf, ndowrkHoppaCmadBenchDf("category_cd") === $"category", "inner")
			.join(facilityAttributeProfileDfFil, trim(facilityAttributeProfileDfFil("Medcr_ID")) === trim(outpatientSummaryDf("Medcr_ID")), "left_outer")
			.join(ndoZipSubmarketXxwalkDf1, trim(col("ndoZipSubmarketXxwalkDf1.Zip_Code")) === trim(outpatientSummaryDf("PROV_ZIP_CD")) &&
			trim(col("ndoZipSubmarketXxwalkDf1.ST_CD")) === trim(outpatientSummaryDf("PROV_ST_NM")) && trim(col("ndoZipSubmarketXxwalkDf1.CNTY_NM")) === trim(outpatientSummaryDf("PROV_County")), "left_outer").
			join(ndoZipSubmarketXxwalkDf_MBR,trim(outpatientSummaryDf("MBR_ZIP3"))===trim(col("ndoZipSubmarketXxwalkDf_MBR.zip_3"))
          && trim(col("ndoZipSubmarketXxwalkDf_MBR.ST_CD")) === trim(outpatientSummaryDf("MBR_State")) && trim(col("ndoZipSubmarketXxwalkDf_MBR.CNTY_NM")) === trim(outpatientSummaryDf("MBR_County")), "left_outer")
     .filter(trim(outpatientSummaryDf("Prov_ST_NM")).isNotNull &&
					trim(outpatientSummaryDf("Prov_ST_NM")).!==("") && trim(outpatientSummaryDf("MBUlvl2")).isNotNull &&
					trim(outpatientSummaryDf("prodlvl3")).isNotNull && trim(outpatientSummaryDf("MCS")).!==("Y") && trim(outpatientSummaryDf("liccd")) === ("01") &&
					trim(outpatientSummaryDf("MBU_CF_CD")).isin("MCKYGP", "SRLGUN", "SRLGCO", "SRLGVA", "MCOHGP", "SRLGNV", "MCINGP", "SRLGME", "SRLGNH",
							"SRLGKS", "SRLGGA", "SRLGNA", "SRNAHS", "SRLGMO", "SRLGWI", "SRLGNY", "SRLGCT",
							"SRLGBC", "MJCAMC", "SPCAMC") === false && facilityAttributeProfileDfFil("factype").isin("Delete", "Long Term Care Hospital", "Snf", "Physical Rehab") === false &&
							$"Inc_Month".between(rangeStartDate, rangeEndDate)).select(
									trim(outpatientSummaryDf("PROV_ST_NM")).alias("PROV_ST_NM"),
									(coalesce(trim(outpatientSummaryDf("prodlvl3")), lit("UNK"))).alias("PROD_LVL3_DESC"),
									(coalesce(trim(outpatientSummaryDf("fundlvl2")), lit("UNK"))).alias("FUNDG_LVL2_DESC"),
									(coalesce(trim(outpatientSummaryDf("MBUlvl4")), lit("UNK"))).alias("MBU_LVL4_DESC"),
									(coalesce(trim(outpatientSummaryDf("MBUlvl3")), lit("UNK"))).alias("MBU_LVL3_DESC"),
									(coalesce(trim(outpatientSummaryDf("EXCHNG_IND_CD")), lit("N/A"))).alias("EXCHNG_IND_CD"),
									(coalesce(trim(outpatientSummaryDf("MBUlvl2")), lit("UNK"))).alias("MBU_LVL2_DESC"),
            			upper((when(trim(col("ndoZipSubmarketXxwalkDf_MBR.RATING_AREA_DESC")).isNull, lit("UNK"))
									    when(trim(outpatientSummaryDf("MBR_State"))!==trim(outpatientSummaryDf("PROV_ST_NM")), lit("UNK"))
									    otherwise (trim(col("ndoZipSubmarketXxwalkDf_MBR.RATING_AREA_DESC"))))).alias("MBR_RATING_AREA"),
									trim(outpatientSummaryDf("MEDCR_ID")).alias("MEDCR_NBR"),
									(when(trim(facilityAttributeProfileDfFil("Hospital")).isNull, upper($"PROV_NM")) otherwise (upper(trim(facilityAttributeProfileDfFil("Hospital"))))).alias("HOSP_NM"),
									(coalesce(trim(outpatientSummaryDf("System_ID")), lit("UNK"))).alias("HOSP_SYS_NM"),
            			(when(trim(col("ndoZipSubmarketXxwalkDf1.RATING_AREA_DESC")).isNotNull, trim(ndoZipSubmarketXxwalkDf("RATING_AREA_DESC")))
											when (trim(facilityAttributeProfileDfFil("Rating_Area")).isNotNull, trim(facilityAttributeProfileDfFil("Rating_Area"))) otherwise (lit("UNK"))).alias("SUBMRKT_DESC"),
									trim(outpatientSummaryDf("prov_county")).alias("PROV_CNTY_NM"),
									(coalesce(trim(facilityAttributeProfileDfFil("factype")), lit("UNK"))).alias("FCLTY_TYPE_DESC"),
									trim(outpatientSummaryDf("INN_CD")).alias("PAR_STTS_CD"),
									(when(trim(facilityAttributeProfileDfFil("Hospital")).isNull, lit("B")) otherwise (lit("A"))).alias("PROV_EXCLSN_CD"),
									(when(trim(outpatientSummaryDf("cat1")).===("Surgery") && trim(outpatientSummaryDf("ER_Flag")) === "Y", lit("ER_SURGERY"))
											when (trim(outpatientSummaryDf("cat1")).===("Surgery") && trim(outpatientSummaryDf("ER_Flag")) === "N", lit("SURGERY"))
											when (trim(outpatientSummaryDf("cat1")).===("ER / Urgent"), lit("ER"))
                      when (trim(outpatientSummaryDf("cat1")).===("Lab"), lit("LAB"))
											when (trim(outpatientSummaryDf("cat1")).===("Radiology") && trim(outpatientSummaryDf("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms"), lit("RAD_SCANS"))
											when (trim(outpatientSummaryDf("cat1")).===("Radiology") && trim(outpatientSummaryDf("cat2")).isin("CT scans", "High Tech MRI", "PET Scans", "Ultrasounds", "Mammograms") === false, lit("RAD_OTHER"))
											when (trim(outpatientSummaryDf("cat1")).===("Other") && trim(outpatientSummaryDf("cat2")).===("Drugs-Infusion/Injection Therapy"), lit("DRUGS"))
											when (trim(outpatientSummaryDf("cat1")).===("Other") && trim(outpatientSummaryDf("cat2")).!==("Drugs-Infusion/Injection Therapy"), lit("OPOTH"))
											otherwise (concat(lit("OTHER: "), trim(outpatientSummaryDf("cat1")), lit(", "), trim(outpatientSummaryDf("cat2"))))).alias("Category"),
									ndowrkHoppaCmadBenchDf("CMI_Bench"),
                  (coalesce(trim(outpatientSummaryDf("RPTG_NTWK_DESC")), lit("UNK"))).alias("RPTG_NTWK_DESC"),							
									(when(trim(outpatientSummaryDf("ALWD_AMT")).isNull, lit(0)) otherwise (trim(outpatientSummaryDf("ALWD_AMT")))).alias("ALWD_AMT"),
									(when(trim(outpatientSummaryDf("BILLD_CHRG_AMT")).isNull, lit(0)) otherwise (trim(outpatientSummaryDf("BILLD_CHRG_AMT")))).alias("BILLD_CHRG_AMT"),
									(when(trim(outpatientSummaryDf("PAID_AMT")).isNull, lit(0)) otherwise (trim(outpatientSummaryDf("PAID_AMT")))).alias("PAID_AMT"),
									(when(trim(outpatientSummaryDf("cases")).isNull, lit(0)) otherwise (trim(outpatientSummaryDf("cases")))).alias("cases"),
									trim(outpatientSummaryDf("ER_FLAG")).alias("ER_FLAG"),
									(when(trim(outpatientSummaryDf("CMAD_CASES")).isNull, lit(0)) otherwise (trim(outpatientSummaryDf("CMAD_CASES")))).alias("CMAD_CASES"),
									(when(trim(outpatientSummaryDf("CMAD_ALLOWED")).isNull, lit(0)) otherwise (trim(outpatientSummaryDf("CMAD_ALLOWED")))).alias("CMAD_ALLOWED"),
									(when(trim(outpatientSummaryDf("CMAD")).isNull, lit(0)) otherwise (trim(outpatientSummaryDf("CMAD")))).alias("CMAD"),
									(when(outpatientSummaryDf("ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (outpatientSummaryDf("ALWD_AMT_WITH_CMS"))).alias("ALWD_AMT_WITH_CMS"),
									(when(outpatientSummaryDf("CMS_Allowed").isNull, lit(0)) otherwise (outpatientSummaryDf("CMS_Allowed"))).alias("CMS_REIMBMNT_AMT")).groupBy(
											trim($"PROV_ST_NM"),
                      trim($"PROD_LVL3_DESC"),
                      trim($"FUNDG_LVL2_DESC"),
                      trim($"MBU_LVL4_DESC"),
                      trim($"MBU_LVL3_DESC"),
                      trim($"EXCHNG_IND_CD"),
                      trim($"MBU_LVL2_DESC"),
                      trim($"MBR_RATING_AREA"),
                      trim($"MEDCR_NBR"),
                      trim($"HOSP_NM"),
                      trim($"HOSP_SYS_NM"),
                      trim($"SUBMRKT_DESC"),
                      trim($"PROV_CNTY_NM"),
                      trim($"FCLTY_TYPE_DESC"),
                      trim($"PAR_STTS_CD"),
                      trim($"PROV_EXCLSN_CD"),
                      trim($"Category"),
                      ndowrkHoppaCmadBenchDf("CMI_Bench"),
                      trim($"RPTG_NTWK_DESC")
											/*trim(ndowrkHoppaCmadBenchDf("CMI_Bench")).alias("CMI_Bench")*/).agg(
													sum($"ALWD_AMT").alias("ALWD_AMT"),
													sum($"BILLD_CHRG_AMT").alias("BILLD_CHRG_AMT"),
													sum($"PAID_AMT").alias("PAID_AMT"),
													(when(sum($"cases") === 0 || sum($"ALWD_AMT") === 0, lit(0)) otherwise (sum($"ALWD_AMT") / sum($"cases"))).alias("Alwd_Case_Amt"),
													(when(sum($"cases") === 0 || sum($"BILLD_CHRG_AMT") === 0, lit(0)) otherwise (sum($"BILLD_CHRG_AMT") / sum($"cases"))).alias("BILLD_CHRG_Case_AMT"),
													(when(sum($"cases") === 0 || sum($"PAID_AMT") === 0, lit(0)) otherwise (sum($"PAID_AMT") / sum($"cases"))).alias("PAID_Case_AMT"),
													sum($"CASES").alias("CASE_CNT"),
													sum(when($"ER_FLAG" === "Y", $"cases") otherwise (lit(0))).alias("ER_CASE_CNT"),
													sum($"CMAD_CASES").alias("CMAD_CASE_CNT"),
													sum($"CMAD_ALLOWED").alias("CMAD_ALLOWED"),
													(sum($"CMAD") / $"CMI_Bench").alias("CMAD"),
													sum($"ALWD_AMT_WITH_CMS").alias("ALWD_AMT_WITH_CMS"),
													sum($"CMS_REIMBMNT_AMT").alias("CMS_REIMBMNT_AMT")).withColumn("CMI", (when($"CMAD_CASE_CNT" === 0, lit(0)) otherwise (($"CMAD" / $"CMAD_CASE_CNT") / $"CMI_Bench")))

			joinndowrk
	}
}

case class ndowrkEhoppaMbrRatgBase(tabndowrkHoppaMbrCntyInpDf: String, tabndowrkHoppaMbrCntyOutpDf: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			//val ndowrkHoppaProvListDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaProvList")
			val ndowrkHoppaMbrCntyInpDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaMbrCntyInpDf")
			val ndowrkHoppaMbrCntyOutpDf = spark.sql(s"select * from $stagingHiveDB.$tabndowrkHoppaMbrCntyOutpDf")

			val ndowrkHoppaMbrCntyInpDfDist = ndowrkHoppaMbrCntyInpDf.select(
					$"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MBR_RATING_AREA",
					$"MEDCR_NBR",
				  $"HOSP_NM",
					$"HOSP_SYS_NM",
					$"SUBMRKT_DESC",
					$"PROV_CNTY_NM",
					$"FCLTY_TYPE_DESC",
					$"PAR_STTS_CD",
					$"PROV_EXCLSN_CD",
					$"RPTG_NTWK_DESC").distinct

			val ndowrkHoppaMbrCntyOutpDfDist = ndowrkHoppaMbrCntyOutpDf.select(
				$"PROV_ST_NM",
				$"PROD_LVL3_DESC",
				$"FUNDG_LVL2_DESC",
				$"MBU_LVL2_DESC",
				$"MBU_LVL4_DESC",
				$"EXCHNG_IND_CD",
				$"MBU_LVL3_DESC",
				$"MBR_RATING_AREA",
				$"MEDCR_NBR",
				$"HOSP_NM",
        $"HOSP_SYS_NM",
				$"SUBMRKT_DESC",
				$"PROV_CNTY_NM",
				$"FCLTY_TYPE_DESC",
				$"PAR_STTS_CD",
				$"PROV_EXCLSN_CD",
				$"RPTG_NTWK_DESC").distinct



		  val ndowrkHoppaProvListDist=ndowrkHoppaMbrCntyInpDfDist.union(ndowrkHoppaMbrCntyOutpDfDist).distinct()
			
		  	val ndowrkHoppaMbrCntyInpDfCms = ndowrkHoppaMbrCntyInpDf.select(
					$"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MEDCR_NBR",
					$"PROV_CNTY_NM",
					$"PAR_STTS_CD",
					$"MBR_RATING_AREA",
					$"SUBMRKT_DESC",
					$"RPTG_NTWK_DESC",
					$"ALWD_AMT_WITH_CMS",
					$"CMS_REIMBMNT_AMT"
					).groupBy(
					    $"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MEDCR_NBR",
					$"PROV_CNTY_NM",
					$"PAR_STTS_CD",
					$"MBR_RATING_AREA",
					$"SUBMRKT_DESC",
					$"RPTG_NTWK_DESC").agg(sum($"ALWD_AMT_WITH_CMS").as("ALWD_AMT_WITH_CMS"),
					                     sum($"CMS_REIMBMNT_AMT").as("CMS_REIMBMNT_AMT"))

			val ndowrkHoppaMbrCntyOutpDfCms = ndowrkHoppaMbrCntyOutpDf.select(
					$"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MEDCR_NBR",
					$"PROV_CNTY_NM",
					$"PAR_STTS_CD",
					$"MBR_RATING_AREA",
					$"SUBMRKT_DESC",
					$"RPTG_NTWK_DESC",
					$"ALWD_AMT_WITH_CMS",
					$"CMS_REIMBMNT_AMT"
					).groupBy(
					    $"PROV_ST_NM",
					$"PROD_LVL3_DESC",
					$"FUNDG_LVL2_DESC",
					$"MBU_LVL2_DESC",
					$"MBU_LVL4_DESC",
					$"EXCHNG_IND_CD",
					$"MBU_LVL3_DESC",
					$"MEDCR_NBR",
					$"PROV_CNTY_NM",
					$"PAR_STTS_CD",
					$"MBR_RATING_AREA",
					$"SUBMRKT_DESC",
					$"RPTG_NTWK_DESC").agg(sum($"ALWD_AMT_WITH_CMS").as("ALWD_AMT_WITH_CMS"),
					                     sum($"CMS_REIMBMNT_AMT").as("CMS_REIMBMNT_AMT"))
					                     
			val ndowrkHoppaMbrCntyInpDf1 = ndowrkHoppaMbrCntyInpDf.alias("ndowrkHoppaMbrCntyInpDf1")
			val ndowrkHoppaMbrCntyInpDf2 = ndowrkHoppaMbrCntyInpDf.alias("ndowrkHoppaMbrCntyInpDf2")
			val ndowrkHoppaMbrCntyInpDf3 = ndowrkHoppaMbrCntyInpDf.alias("ndowrkHoppaMbrCntyInpDf3")
			val ndowrkHoppaMbrCntyInpDf4 = ndowrkHoppaMbrCntyInpDf.alias("ndowrkHoppaMbrCntyInpDf4")
			val ndowrkHoppaMbrCntyInpDf5 = ndowrkHoppaMbrCntyInpDf.alias("ndowrkHoppaMbrCntyInpDf5")
			val ndowrkHoppaMbrCntyOutpDf1 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf1")
			val ndowrkHoppaMbrCntyOutpDf2 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf2")
			val ndowrkHoppaMbrCntyOutpDf3 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf3")
			val ndowrkHoppaMbrCntyOutpDf4 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf4")
			val ndowrkHoppaMbrCntyOutpDf5 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf5")
			val ndowrkHoppaMbrCntyOutpDf6 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf6")
			val ndowrkHoppaMbrCntyOutpDf7 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf7")
			val ndowrkHoppaMbrCntyOutpDf8 = ndowrkHoppaMbrCntyOutpDf.alias("ndowrkHoppaMbrCntyOutpDf8")
			
				val joinInpcms = ndowrkHoppaProvListDist.join(ndowrkHoppaMbrCntyInpDfCms,
			    ndowrkHoppaMbrCntyInpDfCms("PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
					ndowrkHoppaMbrCntyInpDfCms("PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
					ndowrkHoppaMbrCntyInpDfCms("FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
					ndowrkHoppaMbrCntyInpDfCms("MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
					ndowrkHoppaMbrCntyInpDfCms("MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
					ndowrkHoppaMbrCntyInpDfCms("EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
					ndowrkHoppaMbrCntyInpDfCms("MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
					ndowrkHoppaMbrCntyInpDfCms("MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
					ndowrkHoppaMbrCntyInpDfCms("PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
					ndowrkHoppaMbrCntyInpDfCms("PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
					ndowrkHoppaMbrCntyInpDfCms("MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
					ndowrkHoppaMbrCntyInpDfCms("SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
					ndowrkHoppaMbrCntyInpDfCms("RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC"), "left_outer")
					
				val joinoutpcms=joinInpcms.join(ndowrkHoppaMbrCntyOutpDfCms,
			    ndowrkHoppaMbrCntyOutpDfCms("PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
					ndowrkHoppaMbrCntyOutpDfCms("PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
					ndowrkHoppaMbrCntyOutpDfCms("FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
					ndowrkHoppaMbrCntyOutpDfCms("MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
					ndowrkHoppaMbrCntyOutpDfCms("MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
					ndowrkHoppaMbrCntyOutpDfCms("EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
					ndowrkHoppaMbrCntyOutpDfCms("MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
					ndowrkHoppaMbrCntyOutpDfCms("MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
					ndowrkHoppaMbrCntyOutpDfCms("PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
					ndowrkHoppaMbrCntyOutpDfCms("PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
					ndowrkHoppaMbrCntyOutpDfCms("MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
					ndowrkHoppaMbrCntyOutpDfCms("SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
					ndowrkHoppaMbrCntyOutpDfCms("RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") , "left_outer")

			val joinInp1 = joinoutpcms.join(
					ndowrkHoppaMbrCntyInpDf1,
					col("ndowrkHoppaMbrCntyInpDf1.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
					col("ndowrkHoppaMbrCntyInpDf1.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
					col("ndowrkHoppaMbrCntyInpDf1.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
					col("ndowrkHoppaMbrCntyInpDf1.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
					col("ndowrkHoppaMbrCntyInpDf1.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
					col("ndowrkHoppaMbrCntyInpDf1.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
					col("ndowrkHoppaMbrCntyInpDf1.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
					col("ndowrkHoppaMbrCntyInpDf1.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
					col("ndowrkHoppaMbrCntyInpDf1.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
					col("ndowrkHoppaMbrCntyInpDf1.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
					col("ndowrkHoppaMbrCntyInpDf1.MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
					col("ndowrkHoppaMbrCntyInpDf1.category") === "MATERNITY" &&
					col("ndowrkHoppaMbrCntyInpDf1.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
					col("ndowrkHoppaMbrCntyInpDf1.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC")/*&&
					col("ndowrkHoppaMbrCntyInpDf1.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM")*/, "left_outer")

			val joinInp2 = joinInp1.join(ndowrkHoppaMbrCntyInpDf2, col("ndowrkHoppaMbrCntyInpDf2.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyInpDf2.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf2.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf2.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf2.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf2.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyInpDf2.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf2.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyInpDf2.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyInpDf2.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyInpDf2.MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
			col("ndowrkHoppaMbrCntyInpDf2.category") === "NICU" &&
			col("ndowrkHoppaMbrCntyInpDf2.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf2.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") /*&&
			col("ndowrkHoppaMbrCntyInpDf2.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM")*/, "left_outer")
			
			
			val joinInp5 = joinInp2.join(ndowrkHoppaMbrCntyInpDf5, col("ndowrkHoppaMbrCntyInpDf5.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyInpDf5.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf5.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf5.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf5.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf5.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyInpDf5.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf5.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyInpDf5.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyInpDf5.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyInpDf5.MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
			col("ndowrkHoppaMbrCntyInpDf5.category") === "TRANSPLANT" &&
			col("ndowrkHoppaMbrCntyInpDf5.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf5.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") /*&&
			col("ndowrkHoppaMbrCntyInpDf2.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM")*/, "left_outer")

			val joinInp3 = joinInp5.join(ndowrkHoppaMbrCntyInpDf3, col("ndowrkHoppaMbrCntyInpDf3.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyInpDf3.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf3.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf3.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf3.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf3.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyInpDf3.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf3.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyInpDf3.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyInpDf3.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyInpDf3.MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
			col("ndowrkHoppaMbrCntyInpDf3.category") === "IPOTH" &&
			col("ndowrkHoppaMbrCntyInpDf3.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf3.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC")/*&&
			col("ndowrkHoppaMbrCntyInpDf3.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM") */, "left_outer")

			val joinInp4 = joinInp3.join(ndowrkHoppaMbrCntyInpDf4, col("ndowrkHoppaMbrCntyInpDf4.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyInpDf4.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf4.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf4.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf4.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf4.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyInpDf4.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf4.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyInpDf4.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyInpDf4.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyInpDf4.MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
			col("ndowrkHoppaMbrCntyInpDf4.category").isin("IPOTH", "NICU", "MATERNITY","TRANSPLANT") &&
			col("ndowrkHoppaMbrCntyInpDf4.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			col("ndowrkHoppaMbrCntyInpDf4.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC")/* &&
			col("ndowrkHoppaMbrCntyInpDf4.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM")*/, "left_outer")

			val joinOut1 = joinInp4.join(ndowrkHoppaMbrCntyOutpDf1, col("ndowrkHoppaMbrCntyOutpDf1.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf1.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf1.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf1.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf1.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf1.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf1.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf1.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf1.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf1.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf1.MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
			col("ndowrkHoppaMbrCntyOutpDf1.category") === "ER" &&
			col("ndowrkHoppaMbrCntyOutpDf1.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") && 
			col("ndowrkHoppaMbrCntyOutpDf1.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC")/*&&
			col("ndowrkHoppaMbrCntyOutpDf1.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM")*/, "left_outer")

			val joinOut2 = joinOut1.join(ndowrkHoppaMbrCntyOutpDf2, col("ndowrkHoppaMbrCntyOutpDf2.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf2.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf2.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf2.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf2.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf2.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf2.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf2.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf2.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf2.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf2.MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
			col("ndowrkHoppaMbrCntyOutpDf2.category") === "ER_SURGERY" &&
			col("ndowrkHoppaMbrCntyOutpDf2.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") && 
			col("ndowrkHoppaMbrCntyOutpDf2.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC") /*&&
			col("ndowrkHoppaMbrCntyOutpDf2.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM")*/, "left_outer")

			val joinOut3 = joinOut2.join(ndowrkHoppaMbrCntyOutpDf3, col("ndowrkHoppaMbrCntyOutpDf3.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf3.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf3.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf3.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf3.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf3.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf3.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf3.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf3.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf3.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf3.MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
			col("ndowrkHoppaMbrCntyOutpDf3.category") === "SURGERY" &&
			col("ndowrkHoppaMbrCntyOutpDf3.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf3.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC")/*&&
			col("ndowrkHoppaMbrCntyOutpDf3.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM")*/, "left_outer")

			val joinOut4 = joinOut3.join(ndowrkHoppaMbrCntyOutpDf4, col("ndowrkHoppaMbrCntyOutpDf4.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf4.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf4.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf4.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf4.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf4.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf4.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf4.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf4.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf4.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf4.MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
			col("ndowrkHoppaMbrCntyOutpDf4.category") === "LAB" &&
			col("ndowrkHoppaMbrCntyOutpDf4.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf4.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC")/*&&
			col("ndowrkHoppaMbrCntyOutpDf4.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM")*/, "left_outer")

			val joinOut5 = joinOut4.join(ndowrkHoppaMbrCntyOutpDf5, col("ndowrkHoppaMbrCntyOutpDf5.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf5.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf5.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf5.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf5.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf5.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf5.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf5.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf5.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf5.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf5.MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
			col("ndowrkHoppaMbrCntyOutpDf5.category") === "RAD_SCANS" &&
			col("ndowrkHoppaMbrCntyOutpDf5.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf5.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC")/*&&
			col("ndowrkHoppaMbrCntyOutpDf5.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM")*/, "left_outer")

			val joinOut6 = joinOut5.join(ndowrkHoppaMbrCntyOutpDf6, col("ndowrkHoppaMbrCntyOutpDf6.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf6.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf6.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf6.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf6.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf6.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf6.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf6.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf6.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf6.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf6.MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
			col("ndowrkHoppaMbrCntyOutpDf6.category") === "RAD_OTHER" &&
			col("ndowrkHoppaMbrCntyOutpDf6.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf6.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC")/*&&
			col("ndowrkHoppaMbrCntyOutpDf6.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM")*/, "left_outer")

			val joinOut7 = joinOut6.join(ndowrkHoppaMbrCntyOutpDf7, col("ndowrkHoppaMbrCntyOutpDf7.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf7.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf7.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf7.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf7.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf7.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf7.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf7.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf7.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf7.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf7.MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
			col("ndowrkHoppaMbrCntyOutpDf7.category") === "DRUGS" &&
			col("ndowrkHoppaMbrCntyOutpDf7.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf7.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC")/*&&
			col("ndowrkHoppaMbrCntyOutpDf7.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM")*/, "left_outer")

			val joinOut8 = joinOut7.join(ndowrkHoppaMbrCntyOutpDf8, col("ndowrkHoppaMbrCntyOutpDf8.PROV_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf8.PROD_LVL3_DESC") === ndowrkHoppaProvListDist("PROD_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf8.FUNDG_LVL2_DESC") === ndowrkHoppaProvListDist("FUNDG_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf8.MBU_LVL2_DESC") === ndowrkHoppaProvListDist("MBU_LVL2_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf8.MBU_LVL4_DESC") === ndowrkHoppaProvListDist("MBU_LVL4_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf8.EXCHNG_IND_CD") === ndowrkHoppaProvListDist("EXCHNG_IND_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf8.MBU_LVL3_DESC") === ndowrkHoppaProvListDist("MBU_LVL3_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf8.MEDCR_NBR") === ndowrkHoppaProvListDist("MEDCR_NBR") &&
			col("ndowrkHoppaMbrCntyOutpDf8.PROV_CNTY_NM") === ndowrkHoppaProvListDist("PROV_CNTY_NM") &&
			col("ndowrkHoppaMbrCntyOutpDf8.PAR_STTS_CD") === ndowrkHoppaProvListDist("PAR_STTS_CD") &&
			col("ndowrkHoppaMbrCntyOutpDf8.MBR_RATING_AREA") === ndowrkHoppaProvListDist("MBR_RATING_AREA") &&
			col("ndowrkHoppaMbrCntyOutpDf8.category") === "OPOTH" &&
			col("ndowrkHoppaMbrCntyOutpDf8.SUBMRKT_DESC") === ndowrkHoppaProvListDist("SUBMRKT_DESC") &&
			col("ndowrkHoppaMbrCntyOutpDf8.RPTG_NTWK_DESC") === ndowrkHoppaProvListDist("RPTG_NTWK_DESC")/*&&
			col("ndowrkHoppaMbrCntyOutpDf8.HOSP_SYS_NM") === ndowrkHoppaProvListDist("HOSP_SYS_NM")*/, "left_outer")

			val joinOut8Sel = joinOut8.select(
					ndowrkHoppaProvListDist("PROV_ST_NM"),
					ndowrkHoppaProvListDist("PROD_LVL3_DESC"),
					ndowrkHoppaProvListDist("FUNDG_LVL2_DESC"),
					ndowrkHoppaProvListDist("MBU_LVL4_DESC"),
					ndowrkHoppaProvListDist("MBU_LVL3_DESC"),
					ndowrkHoppaProvListDist("EXCHNG_IND_CD"),
					ndowrkHoppaProvListDist("MBU_LVL2_DESC"),
					//(when(ndowrkHoppaProvListDist("MBR_ST_NM") === ndowrkHoppaProvListDist("PROV_ST_NM"), ndowrkHoppaProvListDist("MBR_RATING_AREA"))
							(when (trim(ndowrkHoppaProvListDist("SUBMRKT_DESC")) === trim(ndowrkHoppaProvListDist("MBR_RATING_AREA")), ndowrkHoppaProvListDist("MBR_RATING_AREA")) otherwise (ndowrkHoppaProvListDist("MBR_RATING_AREA"))).alias("MBR_RATG_AREA_NM"),
					(concat(trim(ndowrkHoppaProvListDist("MEDCR_NBR")), lit(": "), trim(ndowrkHoppaProvListDist("HOSP_NM")))).alias("HOSP_NM"),
					ndowrkHoppaProvListDist("HOSP_SYS_NM"),
					ndowrkHoppaProvListDist("SUBMRKT_DESC"),
					ndowrkHoppaProvListDist("PROV_CNTY_NM"),
					ndowrkHoppaProvListDist("FCLTY_TYPE_DESC"),
					ndowrkHoppaProvListDist("PAR_STTS_CD"),
					ndowrkHoppaProvListDist("PROV_EXCLSN_CD"),
					ndowrkHoppaProvListDist("MEDCR_NBR"),
          ndowrkHoppaProvListDist("RPTG_NTWK_DESC"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.BILLD_CHRG_AMT"))).alias("MTRNTY_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT"))).alias("MTRNTY_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.PAID_AMT"))).alias("MTRNTY_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.BILLD_CHRG_Case_AMT"))).alias("MTRNTY_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.Alwd_Case_Amt"))).alias("MTRNTY_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.PAID_Case_AMT"))).alias("MTRNTY_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMAD_ALLOWED"))).alias("MTRNTY_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CASE_CNT"))).alias("MTRNTY_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMI"))).alias("MTRNTY_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMAD_CASE_CNT"))).alias("MTRNTY_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyInpDf1.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMAD"))).alias("MTRNTY_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.ALWD_AMT_WITH_CMS"))).alias("MTRNTY_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf1.CMS_REIMBMNT_AMT"))).alias("MTRNTY_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaMbrCntyInpDf2.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.BILLD_CHRG_AMT"))).alias("NICU_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.ALWD_AMT"))).alias("NICU_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.PAID_AMT"))).alias("NICU_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.BILLD_CHRG_Case_AMT"))).alias("NICU_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.Alwd_Case_Amt"))).alias("NICU_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.PAID_Case_AMT"))).alias("NICU_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.CMAD_ALLOWED"))).alias("NICU_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.CASE_CNT"))).alias("NICU_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.CMI"))).alias("NICU_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.CMAD_CASE_CNT"))).alias("NICU_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyInpDf2.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.CMAD"))).alias("NICU_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf2.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.ALWD_AMT_WITH_CMS"))).alias("NICU_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf2.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf2.CMS_REIMBMNT_AMT"))).alias("NICU_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaMbrCntyInpDf5.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.BILLD_CHRG_AMT"))).alias("TRANSPLANT_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf5.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.ALWD_AMT"))).alias("TRANSPLANT_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf5.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.PAID_AMT"))).alias("TRANSPLANT_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf5.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.BILLD_CHRG_Case_AMT"))).alias("TRANSPLANT_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf5.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.Alwd_Case_Amt"))).alias("TRANSPLANT_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf5.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.PAID_Case_AMT"))).alias("TRANSPLANT_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf5.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.CMAD_ALLOWED"))).alias("TRANSPLANT_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf5.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.CASE_CNT"))).alias("TRANSPLANT_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf5.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.CMI"))).alias("TRANSPLANT_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf5.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.CMAD_CASE_CNT"))).alias("TRANSPLANT_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyInpDf5.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf5.CMAD"))).alias("TRANSPLANT_CMAD_NBR"),
					
					
					(when(col("ndowrkHoppaMbrCntyInpDf3.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.BILLD_CHRG_AMT"))).alias("INPAT_OTH_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.ALWD_AMT"))).alias("INPAT_OTH_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.PAID_AMT"))).alias("INPAT_OTH_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.BILLD_CHRG_Case_AMT"))).alias("INPAT_OTH_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.Alwd_Case_Amt"))).alias("INPAT_OTH_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.PAID_Case_AMT"))).alias("INPAT_OTH_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.CMAD_ALLOWED"))).alias("INPAT_OTH_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.CASE_CNT"))).alias("INPAT_OTH_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.CMI"))).alias("INPAT_OTH_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.CMAD_CASE_CNT"))).alias("INPAT_OTH_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyInpDf3.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.CMAD"))).alias("INPAT_OTH_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyInpDf3.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.ALWD_AMT_WITH_CMS"))).alias("INPAT_OTH_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyInpDf3.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyInpDf3.CMS_REIMBMNT_AMT"))).alias("INPAT_OTH_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaMbrCntyOutpDf1.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.BILLD_CHRG_AMT"))).alias("ER_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.ALWD_AMT"))).alias("ER_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.PAID_AMT"))).alias("ER_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.BILLD_CHRG_Case_AMT"))).alias("ER_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.Alwd_Case_Amt"))).alias("ER_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.PAID_Case_AMT"))).alias("ER_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.CMAD_ALLOWED"))).alias("ER_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.CASE_CNT"))).alias("ER_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.CMI"))).alias("ER_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.CMAD_CASE_CNT"))).alias("ER_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf1.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.CMAD"))).alias("ER_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf1.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.ALWD_AMT_WITH_CMS"))).alias("ER_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf1.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf1.CMS_REIMBMNT_AMT"))).alias("ER_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaMbrCntyOutpDf2.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.BILLD_CHRG_AMT"))).alias("SRGY_ER_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.ALWD_AMT"))).alias("SRGY_ER_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.PAID_AMT"))).alias("SRGY_ER_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.BILLD_CHRG_Case_AMT"))).alias("SRGY_ER_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.Alwd_Case_Amt"))).alias("SRGY_ER_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.PAID_Case_AMT"))).alias("SRGY_ER_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.CMAD_ALLOWED"))).alias("SRGY_ER_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.CASE_CNT"))).alias("SRGY_ER_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.CMI"))).alias("SRGY_ER_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.CMAD_CASE_CNT"))).alias("SRGY_ER_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf2.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.CMAD"))).alias("SRGY_ER_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf2.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.ALWD_AMT_WITH_CMS"))).alias("SRGY_ER_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf2.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf2.CMS_REIMBMNT_AMT"))).alias("SRGY_ER_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaMbrCntyOutpDf3.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.BILLD_CHRG_AMT"))).alias("SRGY_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.ALWD_AMT"))).alias("SRGY_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.PAID_AMT"))).alias("SRGY_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.BILLD_CHRG_Case_AMT"))).alias("SRGY_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.Alwd_Case_Amt"))).alias("SRGY_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.PAID_Case_AMT"))).alias("SRGY_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.CMAD_ALLOWED"))).alias("SRGY_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.CASE_CNT"))).alias("SRGY_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.CMI"))).alias("SRGY_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.CMAD_CASE_CNT"))).alias("SRGY_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf3.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.CMAD"))).alias("SRGY_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf3.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.ALWD_AMT_WITH_CMS"))).alias("SRGY_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf3.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf3.CMS_REIMBMNT_AMT"))).alias("SRGY_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaMbrCntyOutpDf4.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.BILLD_CHRG_AMT"))).alias("LAB_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.ALWD_AMT"))).alias("LAB_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.PAID_AMT"))).alias("LAB_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.BILLD_CHRG_Case_AMT"))).alias("LAB_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.Alwd_Case_Amt"))).alias("LAB_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.PAID_Case_AMT"))).alias("LAB_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.CMAD_ALLOWED"))).alias("LAB_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.CASE_CNT"))).alias("LAB_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.CMI"))).alias("LAB_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.CMAD_CASE_CNT"))).alias("LAB_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf4.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.CMAD"))).alias("LAB_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf4.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.ALWD_AMT_WITH_CMS"))).alias("LAB_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf4.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf4.CMS_REIMBMNT_AMT"))).alias("LAB_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaMbrCntyOutpDf5.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.BILLD_CHRG_AMT"))).alias("RDLGY_SCANS_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.ALWD_AMT"))).alias("RDLGY_SCANS_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.PAID_AMT"))).alias("RDLGY_SCANS_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.BILLD_CHRG_Case_AMT"))).alias("RDLGY_SCANS_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.Alwd_Case_Amt"))).alias("RDLGY_SCANS_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.PAID_Case_AMT"))).alias("RDLGY_SCANS_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.CMAD_ALLOWED"))).alias("RDLGY_SCANS_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.CASE_CNT"))).alias("RDLGY_SCANS_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.CMI"))).alias("RDLGY_SCANS_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.CMAD_CASE_CNT"))).alias("RDLGY_SCANS_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf5.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.CMAD"))).alias("RDLGY_SCANS_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf5.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.ALWD_AMT_WITH_CMS"))).alias("RDLGY_SCANS_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf5.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf5.CMS_REIMBMNT_AMT"))).alias("RDLGY_SCANS_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaMbrCntyOutpDf6.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.BILLD_CHRG_AMT"))).alias("RDLGY_OTHR_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.ALWD_AMT"))).alias("RDLGY_OTHR_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.PAID_AMT"))).alias("RDLGY_OTHR_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.BILLD_CHRG_Case_AMT"))).alias("RDLGY_OTHR_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.Alwd_Case_Amt"))).alias("RDLGY_OTHR_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.PAID_Case_AMT"))).alias("RDLGY_OTHR_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.CMAD_ALLOWED"))).alias("RDLGY_OTHR_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.CASE_CNT"))).alias("RDLGY_OTHR_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.CMI"))).alias("RDLGY_OTHR_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.CMAD_CASE_CNT"))).alias("RDLGY_OTHR_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf6.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.CMAD"))).alias("RDLGY_OTHR_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf6.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.ALWD_AMT_WITH_CMS"))).alias("RDLGY_OTHR_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf6.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf6.CMS_REIMBMNT_AMT"))).alias("RDLGY_OTHR_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaMbrCntyOutpDf7.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.BILLD_CHRG_AMT"))).alias("DRUG_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.ALWD_AMT"))).alias("DRUG_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.PAID_AMT"))).alias("DRUG_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.BILLD_CHRG_Case_AMT"))).alias("DRUG_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.Alwd_Case_Amt"))).alias("DRUG_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.PAID_Case_AMT"))).alias("DRUG_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.CMAD_ALLOWED"))).alias("DRUG_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.CASE_CNT"))).alias("DRUG_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.CMI"))).alias("DRUG_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.CMAD_CASE_CNT"))).alias("DRUG_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf7.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.CMAD"))).alias("DRUG_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf7.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.ALWD_AMT_WITH_CMS"))).alias("DRUG_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf7.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf7.CMS_REIMBMNT_AMT"))).alias("DRUG_CMS_REIMBMNT_AMT"),

					(when(col("ndowrkHoppaMbrCntyOutpDf8.BILLD_CHRG_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.BILLD_CHRG_AMT"))).alias("OUTPAT_OTHR_BILLD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.ALWD_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.ALWD_AMT"))).alias("OUTPAT_OTHR_ALLWD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.PAID_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.PAID_AMT"))).alias("OUTPAT_OTHR_PAID_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.BILLD_CHRG_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.BILLD_CHRG_Case_AMT"))).alias("OUTPAT_OTHR_BILLD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.Alwd_Case_Amt").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.Alwd_Case_Amt"))).alias("OUTPAT_OTHR_ALLWD_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.PAID_Case_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.PAID_Case_AMT"))).alias("OUTPAT_OTHR_PAID_CASE_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.CMAD_ALLOWED").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.CMAD_ALLOWED"))).alias("OUTPAT_OTHR_ALLWD_CMAD_AMT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.CASE_CNT"))).alias("OUTPAT_OTHR_CASE_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.CMI").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.CMI"))).alias("OUTPAT_OTHR_CMI_NBR"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.CMAD_CASE_CNT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.CMAD_CASE_CNT"))).alias("OUTPAT_OTHR_CMAD_CASE_CNT"),
					(when(col("ndowrkHoppaMbrCntyOutpDf8.CMAD").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.CMAD"))).alias("OUTPAT_OTHR_CMAD_NBR"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf8.ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.ALWD_AMT_WITH_CMS"))).alias("OUTPAT_OTHR_ALWD_AMT_WITH_CMS"),
//					(when(col("ndowrkHoppaMbrCntyOutpDf8.CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (col("ndowrkHoppaMbrCntyOutpDf8.CMS_REIMBMNT_AMT"))).alias("OUTPAT_OTHR_CMS_REIMBMNT_AMT"),

					col("ndowrkHoppaMbrCntyInpDf4.case_cnt"),
					col("ndowrkHoppaMbrCntyInpDf4.ER_Case_Cnt"),
					(when(ndowrkHoppaMbrCntyInpDfCms("ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (ndowrkHoppaMbrCntyInpDfCms("ALWD_AMT_WITH_CMS"))).alias("INP_ALWD_AMT_WITH_CMS"),
					(when(ndowrkHoppaMbrCntyOutpDfCms("ALWD_AMT_WITH_CMS").isNull, lit(0)) otherwise (ndowrkHoppaMbrCntyOutpDfCms("ALWD_AMT_WITH_CMS"))).alias("OUTP_ALWD_AMT_WITH_CMS"),
					(when(ndowrkHoppaMbrCntyInpDfCms("CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (ndowrkHoppaMbrCntyInpDfCms("CMS_REIMBMNT_AMT"))).alias("INP_CMS_REIMBMNT_AMT"),
					(when(ndowrkHoppaMbrCntyOutpDfCms("CMS_REIMBMNT_AMT").isNull, lit(0)) otherwise (ndowrkHoppaMbrCntyOutpDfCms("CMS_REIMBMNT_AMT"))).alias("OUTP_CMS_REIMBMNT_AMT")
					).groupBy(
							ndowrkHoppaProvListDist("PROV_ST_NM"),
							ndowrkHoppaProvListDist("PROD_LVL3_DESC"),
							ndowrkHoppaProvListDist("FUNDG_LVL2_DESC"),
							ndowrkHoppaProvListDist("MBU_LVL4_DESC"),
							ndowrkHoppaProvListDist("MBU_LVL3_DESC"),
							ndowrkHoppaProvListDist("EXCHNG_IND_CD"),
							ndowrkHoppaProvListDist("MBU_LVL2_DESC"),
							$"MBR_RATG_AREA_NM",
							$"HOSP_NM",
							ndowrkHoppaProvListDist("HOSP_SYS_NM"),
							ndowrkHoppaProvListDist("SUBMRKT_DESC"),
							ndowrkHoppaProvListDist("PROV_CNTY_NM"),
							ndowrkHoppaProvListDist("FCLTY_TYPE_DESC"),
							ndowrkHoppaProvListDist("PAR_STTS_CD"),
							ndowrkHoppaProvListDist("PROV_EXCLSN_CD"),
							ndowrkHoppaProvListDist("MEDCR_NBR"),
							ndowrkHoppaProvListDist("RPTG_NTWK_DESC"),
							$"MTRNTY_BILLD_AMT",
							$"MTRNTY_ALLWD_AMT",
							$"MTRNTY_PAID_AMT",
							$"MTRNTY_BILLD_CASE_AMT",
							$"MTRNTY_ALLWD_CASE_AMT",
							$"MTRNTY_PAID_CASE_AMT",
							$"MTRNTY_ALLWD_CMAD_AMT",
							$"MTRNTY_CASE_NBR",
							$"MTRNTY_CMI_NBR",
							$"MTRNTY_CMAD_CASE_CNT",
							$"MTRNTY_CMAD_NBR",
//							$"MTRNTY_ALWD_AMT_WITH_CMS",
//							$"MTRNTY_CMS_REIMBMNT_AMT",

							$"NICU_BILLD_AMT",
							$"NICU_ALLWD_AMT",
							$"NICU_PAID_AMT",
							$"NICU_BILLD_CASE_AMT",
							$"NICU_ALLWD_CASE_AMT",
							$"NICU_PAID_CASE_AMT",
							$"NICU_ALLWD_CMAD_AMT",
							$"NICU_CASE_NBR",
							$"NICU_CMI_NBR",
							$"NICU_CMAD_CASE_CNT",
							$"NICU_CMAD_NBR",
//							$"NICU_ALWD_AMT_WITH_CMS",
//							$"NICU_CMS_REIMBMNT_AMT",

							
							$"TRANSPLANT_BILLD_AMT",
							$"TRANSPLANT_ALLWD_AMT",
							$"TRANSPLANT_PAID_AMT",
							$"TRANSPLANT_BILLD_CASE_AMT",
							$"TRANSPLANT_ALLWD_CASE_AMT",
							$"TRANSPLANT_PAID_CASE_AMT",
							$"TRANSPLANT_ALLWD_CMAD_AMT",
							$"TRANSPLANT_CASE_NBR",
							$"TRANSPLANT_CMI_NBR",
							$"TRANSPLANT_CMAD_CASE_CNT",
							$"TRANSPLANT_CMAD_NBR",
							
							
							$"INPAT_OTH_BILLD_AMT",
							$"INPAT_OTH_ALLWD_AMT",
							$"INPAT_OTH_PAID_AMT",
							$"INPAT_OTH_BILLD_CASE_AMT",
							$"INPAT_OTH_ALLWD_CASE_AMT",
							$"INPAT_OTH_PAID_CASE_AMT",
							$"INPAT_OTH_ALLWD_CMAD_AMT",
							$"INPAT_OTH_CASE_NBR",
							$"INPAT_OTH_CMI_NBR",
							$"INPAT_OTH_CMAD_CASE_CNT",
							$"INPAT_OTH_CMAD_NBR",
//							$"INPAT_OTH_ALWD_AMT_WITH_CMS",
//							$"INPAT_OTH_CMS_REIMBMNT_AMT",

							$"ER_BILLD_AMT",
							$"ER_ALLWD_AMT",
							$"ER_PAID_AMT",
							$"ER_BILLD_CASE_AMT",
							$"ER_ALLWD_CASE_AMT",
							$"ER_PAID_CASE_AMT",
							$"ER_ALLWD_CMAD_AMT",
							$"ER_CASE_NBR",
							$"ER_CMI_NBR",
							$"ER_CMAD_CASE_CNT",
							$"ER_CMAD_NBR",
//							$"ER_ALWD_AMT_WITH_CMS",
//							$"ER_CMS_REIMBMNT_AMT",

							$"SRGY_ER_BILLD_AMT",
							$"SRGY_ER_ALLWD_AMT",
							$"SRGY_ER_PAID_AMT",
							$"SRGY_ER_BILLD_CASE_AMT",
							$"SRGY_ER_ALLWD_CASE_AMT",
							$"SRGY_ER_PAID_CASE_AMT",
							$"SRGY_ER_ALLWD_CMAD_AMT",
							$"SRGY_ER_CASE_NBR",
							$"SRGY_ER_CMI_NBR",
							$"SRGY_ER_CMAD_CASE_CNT",
							$"SRGY_ER_CMAD_NBR",
//							$"SRGY_ER_ALWD_AMT_WITH_CMS",
//							$"SRGY_ER_CMS_REIMBMNT_AMT",

							$"SRGY_BILLD_AMT",
							$"SRGY_ALLWD_AMT",
							$"SRGY_PAID_AMT",
							$"SRGY_BILLD_CASE_AMT",
							$"SRGY_ALLWD_CASE_AMT",
							$"SRGY_PAID_CASE_AMT",
							$"SRGY_ALLWD_CMAD_AMT",
							$"SRGY_CASE_NBR",
							$"SRGY_CMI_NBR",
							$"SRGY_CMAD_CASE_CNT",
							$"SRGY_CMAD_NBR",
//							$"SRGY_ALWD_AMT_WITH_CMS",
//							$"SRGY_CMS_REIMBMNT_AMT",

							$"LAB_BILLD_AMT",
							$"LAB_ALLWD_AMT",
							$"LAB_PAID_AMT",
							$"LAB_BILLD_CASE_AMT",
							$"LAB_ALLWD_CASE_AMT",
							$"LAB_PAID_CASE_AMT",
							$"LAB_ALLWD_CMAD_AMT",
							$"LAB_CASE_NBR",
							$"LAB_CMI_NBR",
							$"LAB_CMAD_CASE_CNT",
							$"LAB_CMAD_NBR",
//							$"LAB_ALWD_AMT_WITH_CMS",
//							$"LAB_CMS_REIMBMNT_AMT",

							$"RDLGY_SCANS_BILLD_AMT",
							$"RDLGY_SCANS_ALLWD_AMT",
							$"RDLGY_SCANS_PAID_AMT",
							$"RDLGY_SCANS_BILLD_CASE_AMT",
							$"RDLGY_SCANS_ALLWD_CASE_AMT",
							$"RDLGY_SCANS_PAID_CASE_AMT",
							$"RDLGY_SCANS_ALLWD_CMAD_AMT",
							$"RDLGY_SCANS_CASE_NBR",
							$"RDLGY_SCANS_CMI_NBR",
							$"RDLGY_SCANS_CMAD_CASE_CNT",
							$"RDLGY_SCANS_CMAD_NBR",
//							$"RDLGY_SCANS_ALWD_AMT_WITH_CMS",
//							$"RDLGY_SCANS_CMS_REIMBMNT_AMT",

							$"RDLGY_OTHR_BILLD_AMT",
							$"RDLGY_OTHR_ALLWD_AMT",
							$"RDLGY_OTHR_PAID_AMT",
							$"RDLGY_OTHR_BILLD_CASE_AMT",
							$"RDLGY_OTHR_ALLWD_CASE_AMT",
							$"RDLGY_OTHR_PAID_CASE_AMT",
							$"RDLGY_OTHR_ALLWD_CMAD_AMT",
							$"RDLGY_OTHR_CASE_NBR",
							$"RDLGY_OTHR_CMI_NBR",
							$"RDLGY_OTHR_CMAD_CASE_CNT",
							$"RDLGY_OTHR_CMAD_NBR",
//							$"RDLGY_OTHR_ALWD_AMT_WITH_CMS",
//							$"RDLGY_OTHR_CMS_REIMBMNT_AMT",

							$"DRUG_BILLD_AMT",
							$"DRUG_ALLWD_AMT",
							$"DRUG_PAID_AMT",
							$"DRUG_BILLD_CASE_AMT",
							$"DRUG_ALLWD_CASE_AMT",
							$"DRUG_PAID_CASE_AMT",
							$"DRUG_ALLWD_CMAD_AMT",
							$"DRUG_CASE_NBR",
							$"DRUG_CMI_NBR",
							$"DRUG_CMAD_CASE_CNT",
							$"DRUG_CMAD_NBR",
//							$"DRUG_ALWD_AMT_WITH_CMS",
//							$"DRUG_CMS_REIMBMNT_AMT",

							$"OUTPAT_OTHR_BILLD_AMT",
							$"OUTPAT_OTHR_ALLWD_AMT",
							$"OUTPAT_OTHR_PAID_AMT",
							$"OUTPAT_OTHR_BILLD_CASE_AMT",
							$"OUTPAT_OTHR_ALLWD_CASE_AMT",
							$"OUTPAT_OTHR_PAID_CASE_AMT",
							$"OUTPAT_OTHR_ALLWD_CMAD_AMT",
							$"OUTPAT_OTHR_CASE_NBR",
							$"OUTPAT_OTHR_CMI_NBR",
							$"OUTPAT_OTHR_CMAD_CASE_CNT",
							$"OUTPAT_OTHR_CMAD_NBR",
//							$"OUTPAT_OTHR_ALWD_AMT_WITH_CMS",
//							$"OUTPAT_OTHR_CMS_REIMBMNT_AMT",
							$"INP_ALWD_AMT_WITH_CMS",
							$"OUTP_ALWD_AMT_WITH_CMS",
							$"INP_CMS_REIMBMNT_AMT",
							$"OUTP_CMS_REIMBMNT_AMT").agg(
									(when(sum(col("ndowrkHoppaMbrCntyInpDf4.case_cnt")) > 0, ((sum(col("ndowrkHoppaMbrCntyInpDf4.ER_Case_Cnt")).cast(DecimalType(18, 4)) / sum(col("ndowrkHoppaMbrCntyInpDf4.case_cnt")).cast(DecimalType(18, 4))) * 100).cast(DecimalType(18, 4)))
											otherwise (lit(0))).alias("ER_PCT"))
			.withColumn("INPAT_ALLWD_AMT", ($"MTRNTY_ALLWD_AMT" + $"NICU_ALLWD_AMT" + $"INPAT_OTH_ALLWD_AMT" + $"TRANSPLANT_ALLWD_AMT"))
			.withColumn("OUTPAT_ALLWD_AMT", ($"ER_ALLWD_AMT" + $"SRGY_ER_ALLWD_AMT" + $"SRGY_ALLWD_AMT" + $"LAB_ALLWD_AMT" + $"RDLGY_SCANS_ALLWD_AMT" + $"RDLGY_OTHR_ALLWD_AMT" + $"DRUG_ALLWD_AMT" + $"OUTPAT_OTHR_ALLWD_AMT"))
			.withColumn("TOTAL_ALLWD_AMT", ($"INPAT_ALLWD_AMT" + $"OUTPAT_ALLWD_AMT"))
			.withColumn("TOTAL_ALWD_AMT_WITH_CMS", ($"INP_ALWD_AMT_WITH_CMS".cast(DecimalType(18, 2)) + $"OUTP_ALWD_AMT_WITH_CMS".cast(DecimalType(18, 2))))
			.withColumn("TOTAL_CMS_REIMBMNT_AMT", ($"INP_CMS_REIMBMNT_AMT".cast(DecimalType(18, 2)) + $"OUTP_CMS_REIMBMNT_AMT".cast(DecimalType(18, 2))))
			.drop($"INP_ALWD_AMT_WITH_CMS")
			.drop($"OUTP_ALWD_AMT_WITH_CMS")
			.drop($"INP_CMS_REIMBMNT_AMT")
			.drop($"OUTP_CMS_REIMBMNT_AMT")

			val joinOut8SelFil = joinOut8Sel.filter($"TOTAL_ALLWD_AMT" > 0)

			joinOut8SelFil
	}
}