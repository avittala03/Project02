package com.am.ndo.medcrRltvty

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
import org.apache.spark.sql.catalog.Column
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.{ GregorianCalendar, Calendar, Date }
import org.joda.time.DateTime
import org.joda.time.Minutes
import java.lang.Character
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.Date
import org.joda.time.format.DateTimeFormat
import org.joda.time
import java.util.{ GregorianCalendar, Calendar, Date }
import java.text.SimpleDateFormat

case class ndoMedcrRltvtyWrk1(facilityAttributeProfileDfFil: Dataset[Row], inpatientSummaryFil: Dataset[Row], outpatientSummaryFil: Dataset[Row], ndtRatingAreaAcaZip3DfFil: Dataset[Row], stagingHiveDB: String, rangeStartDate: String, rangeEndDate: String) extends NDOMonad[Dataset[Row]] with Logging {
  override def run(spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    
    val joinInpatFac = inpatientSummaryFil.join(
      facilityAttributeProfileDfFil,
      trim(facilityAttributeProfileDfFil("Medcr_ID")) === trim(inpatientSummaryFil("Medcr_ID")) &&
        trim(facilityAttributeProfileDfFil("factype")).isin("Acute Hospital", "Childrens", "Veterans Hospital", "Critical Access") === true, "inner")
      .join(ndtRatingAreaAcaZip3DfFil, trim(inpatientSummaryFil("mbr_state")) === trim(ndtRatingAreaAcaZip3DfFil("state")) &&
        trim(inpatientSummaryFil("mbr_county")) === trim(ndtRatingAreaAcaZip3DfFil("cnty_nm")) &&
        trim(inpatientSummaryFil("MBR_ZIP3")) === trim(ndtRatingAreaAcaZip3DfFil("zip3")), "left_outer")
        .select(
        inpatientSummaryFil("mbr_state").alias("MBR_ST_NM"),
        inpatientSummaryFil("MEDCR_ID").alias("MEDCR_ID"),
        facilityAttributeProfileDfFil("Hospital").alias("HOSP_NM"),
        facilityAttributeProfileDfFil("Hosp_System").alias("HOSP_SYS_NM"),
        inpatientSummaryFil("MBUlvl2").alias("MBU_LVL2_DESC"),
        inpatientSummaryFil("MBUlvl4").alias("MBU_LVL4_DESC"),
        //inpatientSummaryFil("prodlvl3").alias("PROD_LVL3_DESC"),
        (coalesce(inpatientSummaryFil("prodlvl3"), lit("UNK"))).alias("PROD_LVL3_DESC"),
        inpatientSummaryFil("RPTG_NTWK_DESC").alias("RPTG_NTWK_DESC"),
        lit("Inpatient").alias("CLM_TYP_DESC"),
        (when(trim(inpatientSummaryFil("cat1")).isin("Maternity", "Newborn"), lit("Maternity/Newborn")) otherwise (lit("OTHER"))).alias("MTRNTY_IND"),
        substring(inpatientSummaryFil("Inc_Month"), 1, 4).alias("INCRD_YEAR"),
        (when(trim(inpatientSummaryFil("EXCHNG_IND_CD")) === "PB", lit("HIX")) otherwise (lit("Non-HIX"))).alias("HIX_IND"),
        inpatientSummaryFil("prov_st_nm"),
        //inpatientSummaryFil("prov_county"),
        (coalesce(inpatientSummaryFil("prov_county"), lit("UNK"))).alias("PROV_CNTY_NM"),
        inpatientSummaryFil("prov_zip_cd"),
        coalesce(ndtRatingAreaAcaZip3DfFil("rating_area_desc"), lit("UNK")).alias("MBR_RATING_AREA"),
        inpatientSummaryFil("INN_CD"),
        (when(trim(inpatientSummaryFil("er_flag")) === "Y", lit("ER"))
          when (trim(inpatientSummaryFil("er_flag")) === "N", lit("Non-ER"))
          otherwise (lit("UNK"))).alias("ER_IND"),
        inpatientSummaryFil("BILLD_CHRG_AMT"),
         inpatientSummaryFil("CVRD_EXPNS_AMT"),
         inpatientSummaryFil("PAID_AMT"),
        inpatientSummaryFil("ALWD_AMT"),
        inpatientSummaryFil("ALWD_AMT_WITH_CMS"),
        inpatientSummaryFil("CMS_REIMBMNT_AMT"))
      .groupBy(
        $"MBR_ST_NM",
        $"MEDCR_ID",
        $"HOSP_NM",
        $"HOSP_SYS_NM",
        $"MBU_LVL2_DESC",
        $"MBU_LVL4_DESC",
        $"PROD_LVL3_DESC",
        $"RPTG_NTWK_DESC",
        $"CLM_TYP_DESC",
        $"MTRNTY_IND",
        $"INCRD_YEAR",
        $"HIX_IND",
        $"prov_st_nm",
        $"PROV_CNTY_NM",
        $"prov_zip_cd",
        $"MBR_RATING_AREA",
        $"INN_CD",
        $"ER_IND").agg( 
          sum($"BILLD_CHRG_AMT").cast(DecimalType(18, 2)).alias("BILLD_CHRG_AMT"),
          sum($"CVRD_EXPNS_AMT").cast(DecimalType(18, 2)).alias("CVRD_EXPNS_AMT"),
          sum($"PAID_AMT").cast(DecimalType(18, 2)).alias("PAID_AMT"),
          sum($"ALWD_AMT").cast(DecimalType(18, 2)).alias("ALWD_AMT"),
          sum($"ALWD_AMT_WITH_CMS").cast(DecimalType(18, 2)).alias("ALWD_AMT_WITH_CMS"),
          sum($"CMS_REIMBMNT_AMT").cast(DecimalType(18, 2)).alias("CMS_REIMBMNT_AMT_MCARE"),
          count("*").alias("rw_cntr"))

   

  val joinOutpatFac = outpatientSummaryFil.join(
      facilityAttributeProfileDfFil,
      trim(facilityAttributeProfileDfFil("Medcr_ID")) === trim(outpatientSummaryFil("Medcr_ID")) &&
        trim(facilityAttributeProfileDfFil("factype")).isin("Acute Hospital", "Childrens", "Veterans Hospital", "Critical Access") === true, "inner")
      .join(ndtRatingAreaAcaZip3DfFil, trim(outpatientSummaryFil("mbr_state")) === trim(ndtRatingAreaAcaZip3DfFil("state")) &&
        trim(outpatientSummaryFil("mbr_county")) === trim(ndtRatingAreaAcaZip3DfFil("cnty_nm")) &&
        trim(outpatientSummaryFil("mbr_zip3")) === trim(ndtRatingAreaAcaZip3DfFil("zip3")), "left_outer")

        .select(
        outpatientSummaryFil("mbr_state").alias("MBR_ST_NM"),
        outpatientSummaryFil("MEDCR_ID"),
        facilityAttributeProfileDfFil("Hospital").alias("HOSP_NM"),
        facilityAttributeProfileDfFil("Hosp_System").alias("HOSP_SYS_NM"),
        outpatientSummaryFil("MBUlvl2").alias("MBU_LVL2_DESC"),
        outpatientSummaryFil("MBUlvl4").alias("MBU_LVL4_DESC"),
        //outpatientSummaryFil("prodlvl3").alias("PROD_LVL3_DESC"),
        (coalesce(trim(outpatientSummaryFil("prodlvl3")), lit("UNK"))).alias("PROD_LVL3_DESC"),
        outpatientSummaryFil("RPTG_NTWK_DESC").alias("RPTG_NTWK_DESC"),
        lit("Outpatient").alias("CLM_TYP_DESC"),
        (when(trim(outpatientSummaryFil("cat1")).isin("Maternity", "Newborn"), lit("Maternity/Newborn")) otherwise (lit("OTHER"))).alias("MTRNTY_IND"),
        substring(col("Inc_Month"), 1, 4).alias("INCRD_YEAR"),
        (when(trim(outpatientSummaryFil("EXCHNG_IND_CD")) === "PB", lit("HIX")) otherwise (lit("Non-HIX"))).alias("HIX_IND"),
        outpatientSummaryFil("prov_st_nm"),
        //outpatientSummaryFil("prov_county"),
        (coalesce(outpatientSummaryFil("prov_county"), lit("UNK"))).alias("PROV_CNTY_NM"),
        outpatientSummaryFil("prov_zip_cd"),
        (coalesce(trim(ndtRatingAreaAcaZip3DfFil("rating_area_desc")), lit("UNK"))).alias("MBR_RATING_AREA"),
        outpatientSummaryFil("INN_CD"),
        (when(trim(outpatientSummaryFil("er_flag")) === "Y", lit("ER"))
          when (trim(outpatientSummaryFil("er_flag")) === "N", lit("Non-ER"))
          otherwise (lit("UNK"))).alias("ER_IND"),
        outpatientSummaryFil("BILLD_CHRG_AMT"),
        outpatientSummaryFil("CVRD_EXPNS_AMT"),
        outpatientSummaryFil("PAID_AMT"),
        outpatientSummaryFil("ALWD_AMT"),
        outpatientSummaryFil("ALWD_AMT_WITH_CMS"),
        outpatientSummaryFil("CMS_Allowed"))
      .groupBy(
        $"MBR_ST_NM",
        $"MEDCR_ID",
        $"HOSP_NM",
        $"HOSP_SYS_NM",
        $"MBU_LVL2_DESC",
        $"MBU_LVL4_DESC",
        $"PROD_LVL3_DESC",
        $"RPTG_NTWK_DESC",
        $"CLM_TYP_DESC",
        $"MTRNTY_IND",
        $"INCRD_YEAR",
        $"HIX_IND",
        $"prov_st_nm",
        $"PROV_CNTY_NM",
        $"prov_zip_cd",
        $"MBR_RATING_AREA",
        $"INN_CD",
        $"ER_IND").agg(
          sum($"BILLD_CHRG_AMT").cast(DecimalType(18, 2)).alias("BILLD_CHRG_AMT"),
          sum($"CVRD_EXPNS_AMT").cast(DecimalType(18, 2)).alias("CVRD_EXPNS_AMT"),
          sum($"PAID_AMT").cast(DecimalType(18, 2)).alias("PAID_AMT"),
          sum($"ALWD_AMT").cast(DecimalType(18, 2)).alias("ALWD_AMT"),
          sum($"ALWD_AMT_WITH_CMS").cast(DecimalType(18, 2)).alias("ALWD_AMT_WITH_CMS"),
          sum($"CMS_Allowed").cast(DecimalType(18, 2)).alias("CMS_REIMBMNT_AMT_MCARE"),
          count("*").alias("rw_cntr"))



    val finalDf = joinOutpatFac.union(joinInpatFac).distinct()

   finalDf

  }
}

case class ndoMedcrRltvtyNtwk(ndoMedcrRltvtyWrk1Df : Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {

  override def run(spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    import java.util.Calendar
    import java.util.Properties

    
    val MedcrRltvtyWrk1DistDf = ndoMedcrRltvtyWrk1Df.select(
      ndoMedcrRltvtyWrk1Df("MEDCR_ID").alias("MEDCR_ID"),
      ndoMedcrRltvtyWrk1Df("INCRD_YEAR").alias("INCRD_YEAR"),
      ndoMedcrRltvtyWrk1Df("MBR_ST_NM").alias("MBR_ST_NM"),
      ndoMedcrRltvtyWrk1Df("MBU_LVL4_DESC").alias("MBU_LVL4_DESC"),
      (when(trim(ndoMedcrRltvtyWrk1Df("PROD_LVL3_DESC")) === "CONSUMER DRIVEN HEALTH PRODUCTS", lit("CDHP-PPO"))
        when (trim(ndoMedcrRltvtyWrk1Df("PROD_LVL3_DESC")) === "PPO PRODUCTS", lit("CDHP-PPO"))
        otherwise (trim(ndoMedcrRltvtyWrk1Df("PROD_LVL3_DESC")))).alias("PROD_LVL3_DESC2"),
      ndoMedcrRltvtyWrk1Df("RPTG_NTWK_DESC").alias("RPTG_NTWK_DESC"),
      ndoMedcrRltvtyWrk1Df("INN_CD").alias("INN_CD"),
      ndoMedcrRltvtyWrk1Df("ALWD_AMT")
      )
      .groupBy(
        $"MEDCR_ID",
        $"INCRD_YEAR",
        $"MBR_ST_NM",
        $"MBU_LVL4_DESC",
        $"PROD_LVL3_DESC2",
        $"RPTG_NTWK_DESC",
        $"INN_CD").agg(
          sum("ALWD_AMT").alias("ALWD_AMT2"))



    val windowFunc = Window.partitionBy(MedcrRltvtyWrk1DistDf("MEDCR_ID"), MedcrRltvtyWrk1DistDf("INCRD_YEAR"), MedcrRltvtyWrk1DistDf("MBR_ST_NM"), MedcrRltvtyWrk1DistDf("MBU_LVL4_DESC"), MedcrRltvtyWrk1DistDf("PROD_LVL3_DESC2"), MedcrRltvtyWrk1DistDf("RPTG_NTWK_DESC")).orderBy(
      MedcrRltvtyWrk1DistDf("ALWD_AMT2").desc,
      MedcrRltvtyWrk1DistDf("INN_CD").asc)



    val ndoMedcrRltvtyWindow = MedcrRltvtyWrk1DistDf.withColumn("ROW_NUMBER", row_number() over (windowFunc)).filter($"ROW_NUMBER" === 1).drop($"ROW_NUMBER")

    ndoMedcrRltvtyWindow

  }
}

case class ndoMedcrRltvty(ndoMedcrRltvtyWrk1Df : Dataset[Row],ndoMedcrRltvtyNtwk : Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {

  override def run(spark: SparkSession): Dataset[Row] = {
     import spark.implicits._
 val ndoMedcrRltvtyNtwk1 = ndoMedcrRltvtyNtwk.alias("ndoMedcrRltvtyNtwk1")
 val ndoMedcrRltvtyNtwk2 = ndoMedcrRltvtyNtwk.alias("ndoMedcrRltvtyNtwk2")

    val RltvtyJoinNtwk = ndoMedcrRltvtyWrk1Df.join(ndoMedcrRltvtyNtwk1, ndoMedcrRltvtyWrk1Df("MEDCR_ID") === col("ndoMedcrRltvtyNtwk1.MEDCR_ID") &&
      ndoMedcrRltvtyWrk1Df("INCRD_YEAR") === col("ndoMedcrRltvtyNtwk1.INCRD_YEAR") &&
      ndoMedcrRltvtyWrk1Df("MBR_ST_NM") === col("ndoMedcrRltvtyNtwk1.MBR_ST_NM") &&
      ndoMedcrRltvtyWrk1Df("MBU_LVL4_DESC") === col("ndoMedcrRltvtyNtwk1.MBU_LVL4_DESC") &&
      ndoMedcrRltvtyWrk1Df("RPTG_NTWK_DESC") === col("ndoMedcrRltvtyNtwk1.RPTG_NTWK_DESC") &&
      coalesce(ndoMedcrRltvtyWrk1Df("PROD_LVL3_DESC"), lit("")) === coalesce(col("ndoMedcrRltvtyNtwk1.PROD_LVL3_DESC2"), lit("")), "left_outer")
      .join(ndoMedcrRltvtyNtwk2, ndoMedcrRltvtyWrk1Df("MEDCR_ID") === col("ndoMedcrRltvtyNtwk2.MEDCR_ID") &&
        ndoMedcrRltvtyWrk1Df("INCRD_YEAR") === col("ndoMedcrRltvtyNtwk2.INCRD_YEAR") &&
        ndoMedcrRltvtyWrk1Df("MBR_ST_NM") === col("ndoMedcrRltvtyNtwk2.MBR_ST_NM") &&
        ndoMedcrRltvtyWrk1Df("MBU_LVL4_DESC") === col("ndoMedcrRltvtyNtwk2.MBU_LVL4_DESC") &&
        ndoMedcrRltvtyWrk1Df("RPTG_NTWK_DESC") === col("ndoMedcrRltvtyNtwk2.RPTG_NTWK_DESC") &&
        col("ndoMedcrRltvtyNtwk2.PROD_LVL3_DESC2") === "CDHP-PPO", "left_outer")
      .select(
        ndoMedcrRltvtyWrk1Df("MBR_ST_NM"),
        trim(ndoMedcrRltvtyWrk1Df("MEDCR_ID")).alias("MEDCR_ID"),
        ndoMedcrRltvtyWrk1Df("HOSP_NM"),
        ndoMedcrRltvtyWrk1Df("HOSP_SYS_NM"),
        ndoMedcrRltvtyWrk1Df("MBU_LVL2_DESC"),
        ndoMedcrRltvtyWrk1Df("MBU_LVL4_DESC"),
        ndoMedcrRltvtyWrk1Df("PROD_LVL3_DESC"),
        (coalesce(ndoMedcrRltvtyWrk1Df("RPTG_NTWK_DESC"), lit("UNK"))).alias("RPTG_NTWK_DESC"),
        ndoMedcrRltvtyWrk1Df("CLM_TYP_DESC"),
        ndoMedcrRltvtyWrk1Df("MTRNTY_IND"),
        ndoMedcrRltvtyWrk1Df("INCRD_YEAR"),
        ndoMedcrRltvtyWrk1Df("HIX_IND"),
        ndoMedcrRltvtyWrk1Df("prov_st_nm"),
        ndoMedcrRltvtyWrk1Df("PROV_CNTY_NM"),
        ndoMedcrRltvtyWrk1Df("prov_zip_cd"),
        (when(ndoMedcrRltvtyWrk1Df("MBR_ST_NM") === ndoMedcrRltvtyWrk1Df("prov_st_nm"), lit("In-State"))
          otherwise lit("Out-of-State")).alias("PROV_LOC_IND"),
        ndoMedcrRltvtyWrk1Df("MBR_RATING_AREA"),
        (when((ndoMedcrRltvtyWrk1Df("PROD_LVL3_DESC").isin("CONSUMER DRIVEN HEALTH PRODUCTS", "PPO PRODUCTS")) &&
          (col("ndoMedcrRltvtyNtwk2.INN_CD").isNotNull), col("ndoMedcrRltvtyNtwk2.INN_CD"))
          otherwise (coalesce(col("ndoMedcrRltvtyNtwk1.INN_CD"), lit("UNK")))).alias("INN_CD"),
        ndoMedcrRltvtyWrk1Df("ER_IND"),
        ndoMedcrRltvtyWrk1Df("BILLD_CHRG_AMT"),
        ndoMedcrRltvtyWrk1Df("CVRD_EXPNS_AMT"),
        ndoMedcrRltvtyWrk1Df("PAID_AMT"),
        ndoMedcrRltvtyWrk1Df("ALWD_AMT"),
        ndoMedcrRltvtyWrk1Df("ALWD_AMT_WITH_CMS"),
        coalesce(ndoMedcrRltvtyWrk1Df("CMS_REIMBMNT_AMT_MCARE"),lit(0)).alias("CMS_REIMBMNT_AMT_MCARE")
        )
        .groupBy(
            $"MBR_ST_NM",
            $"MEDCR_ID",
            $"HOSP_NM",
            $"HOSP_SYS_NM",
            $"MBU_LVL2_DESC",
            $"MBU_LVL4_DESC",
            $"PROD_LVL3_DESC",
            $"RPTG_NTWK_DESC",
            $"CLM_TYP_DESC",
            $"MTRNTY_IND",
            $"INCRD_YEAR",
            $"HIX_IND",
            $"prov_st_nm",
            $"PROV_CNTY_NM",
            $"prov_zip_cd",
            $"PROV_LOC_IND",
            $"MBR_RATING_AREA",
            $"INN_CD",
            $"ER_IND" )
        .agg(
          sum("BILLD_CHRG_AMT").alias("BILLD_CHRG_AMT"),
          sum("CVRD_EXPNS_AMT").alias("CVRD_EXPNS_AMT"),
          sum("PAID_AMT").alias("PAID_AMT"),
          sum("ALWD_AMT").alias("ALWD_AMT"),
          sum("ALWD_AMT_WITH_CMS").alias("ALWD_AMT_WITH_CMS"),
          sum("CMS_REIMBMNT_AMT_MCARE").alias("CMS_REIMBMNT_AMT_MCARE"))

    println("[NDO-ETL] After RltvtyJoinNtwk")

    RltvtyJoinNtwk

  }
}