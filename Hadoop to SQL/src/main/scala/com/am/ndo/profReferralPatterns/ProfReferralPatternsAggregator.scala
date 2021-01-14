package com.am.ndo.profReferralPatterns

import org.apache.spark.sql.Dataset
import com.am.ndo.helper.NDOMonad
import org.apache.spark.sql.SparkSession
import grizzled.slf4j.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.upper
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.catalyst.expressions.Concat
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.udf

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Locale

case class yue2017Prof(clmMaxRvsn: Dataset[Row], clmLineFilter: Dataset[Row], clmFilter: Dataset[Row],
		clmLineCoa: Dataset[Row], botPcrFnclMbuCf: Dataset[Row], botPcrFnclProdCf: Dataset[Row],
		mbr: Dataset[Row], mdmPpltnXwalk: Dataset[Row], mbrAdrsHist: Dataset[Row], adjdctnStrtDt: String,
		adjdctnEndDt: String, clmLineSrvcStrtDt: String, clmLineSrvcEndDt: String) extends NDOMonad[Dataset[Row]] with Logging {

	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val yueProf0 = clmMaxRvsn.repartition(clmMaxRvsn("CLM_ADJSTMNT_KEY")).join(clmFilter, clmMaxRvsn("CLM_ADJSTMNT_KEY") === clmFilter("CLM_ADJSTMNT_KEY"), "left").
			join(clmLineFilter, clmFilter("CLM_ADJSTMNT_KEY") === clmLineFilter("CLM_ADJSTMNT_KEY"), "left").
			filter(clmLineFilter("ADJDCTN_DT") >= adjdctnStrtDt && clmLineFilter("ADJDCTN_DT") <= adjdctnEndDt &&
			clmLineFilter("CLM_LINE_SRVC_STRT_DT") >= clmLineSrvcStrtDt && clmLineFilter("CLM_LINE_SRVC_STRT_DT") <= clmLineSrvcEndDt &&
			clmLineFilter("CLM_LINE_STTS_CD").isin("APRVD", "PD") && clmLineFilter("alwd_amt") > 0).
			filter(!clmFilter("CLM_ITS_HOST_CD").isin("HOST", "JAACL") && clmFilter("SRVC_RNDRG_TYPE_CD").isin("PHYSN", "PANCL"))

			val yueProf1 = yueProf0.select(
					clmMaxRvsn("MBR_KEY").as("MBR_KEY"),
					clmMaxRvsn("CLM_ADJSTMNT_KEY").as("CLM_ADJSTMNT_KEY"),
					clmFilter("SRC_BILLG_TAX_ID").as("SRC_BILLG_TAX_ID"),
					clmFilter("DIAG_1_CD").as("DIAG_1_CD"),
					clmLineFilter("BILLD_CHRG_AMT").as("BILLD_CHRG_AMT"),
					clmLineFilter("ALWD_AMT").as("ALWD_AMT"),
					clmLineFilter("PAID_AMT").as("PAID_AMT"))
			.groupBy(
					$"MBR_KEY",
					$"CLM_ADJSTMNT_KEY",
					$"SRC_BILLG_TAX_ID",
					$"DIAG_1_CD").agg(
							sum($"BILLD_CHRG_AMT").as("tot_bil"),
							sum($"ALWD_AMT").as("tot_alow"),
							sum($"PAID_AMT").as("tot_paid"))

			println(s"Volatile table 1 parsed ")
			val yueProf1_per = yueProf1.persist(StorageLevel.DISK_ONLY)
			//yueProf1_per.write.mode("overwrite").saveAsTable("ts_pdppndoph_nogbd_r000_sg.tabyueProf1_per")
			yueProf1_per

	}
}
//-------------- Volatile Table 2 (YUE_2017_PROF_line) --------------------------

case class yueProfLine(yueProf1: Dataset[Row], clmLine: Dataset[Row], clmLineCoa: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val yueProfLineDF = yueProf1.join(clmLine, yueProf1("CLM_ADJSTMNT_KEY") === clmLine("CLM_ADJSTMNT_KEY"), "left").
			join(clmLineCoa, yueProf1("CLM_ADJSTMNT_KEY") === clmLineCoa("CLM_ADJSTMNT_KEY"), "left")
			.select(
					yueProf1("CLM_ADJSTMNT_KEY"),
					clmLine("clm_line_nbr"),
					clmLine("CLM_LINE_SRVC_STRT_DT"),
					clmLine("CLM_LINE_SRVC_END_DT"),
					clmLineCoa("mbu_cf_cd"),
					clmLineCoa("prod_cf_cd"))

			println(s"Volatile table 2 parsed ")
			val yueProfLineDF_per = yueProfLineDF.persist(StorageLevel.DISK_ONLY)

			yueProfLineDF_per
	}
}

case class yueProfLob(yueProfLineDF: Dataset[Row], botPcrFnclMbuCf: Dataset[Row], botPcrFnclProdCf: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			//-------------- Volatile Table 3 (YUE_2017_PROF_lob) --------------------------
			val yueProfLobAllDF = yueProfLineDF.join(broadcast(botPcrFnclMbuCf), yueProfLineDF("MBU_CF_CD") === botPcrFnclMbuCf("MBU_CF_CD"), "left")
			.join(botPcrFnclProdCf, yueProfLineDF("PROD_CF_CD") === botPcrFnclProdCf("PROD_CF_CD"), "left")

			//val windowFunc = Window.partitionBy(yueProfLineDF("CLM_ADJSTMNT_KEY")).orderBy(yueProfLineDF("CLM_LINE_SRVC_STRT_DT").asc)
			val windowFunc = Window.partitionBy(yueProfLineDF("CLM_ADJSTMNT_KEY")).orderBy(yueProfLineDF("CLM_LINE_SRVC_STRT_DT").asc,botPcrFnclMbuCf("PCR_CNTRCTD_ST_DESC").asc_nulls_last,botPcrFnclMbuCf("PCR_LOB_DESC").asc_nulls_last
				,botPcrFnclProdCf("PCR_PROD_DESC").asc_nulls_last)

			val yueProfLobDF = yueProfLobAllDF.withColumn("row_number", row_number().over(windowFunc))
			.filter($"row_number" === 1)
			.select(
					yueProfLineDF("CLM_ADJSTMNT_KEY").as("lob_CLM_ADJSTMNT_KEY"),
					botPcrFnclMbuCf("PCR_CNTRCTD_ST_DESC"),
					botPcrFnclMbuCf("PCR_LOB_DESC"),
					botPcrFnclProdCf("PCR_PROD_DESC"))

			println(s"Volatile table 3 parsed ")
			//yueProfLobDF.write.mode("overwrite").saveAsTable("ts_pdppndoph_nogbd_r000_sg.tabyueProfLobDF")
			yueProfLobDF
	}
}

case class yueProfFirstDF(yueProfLineDF: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			//---------------  Volatile Table 4 (YUE_2017_PROF_FIRST) --------------------------
			val yueProfFirstAllDF = yueProfLineDF.
			select($"CLM_ADJSTMNT_KEY", $"CLM_LINE_SRVC_STRT_DT".as("CLM_SRVC_FIRST_DT"))

			val windowFunc1 = Window.partitionBy($"CLM_ADJSTMNT_KEY").orderBy($"CLM_SRVC_FIRST_DT".asc)
			val yueProfFirstDF = yueProfFirstAllDF.withColumn("row_number", row_number().over(windowFunc1))
			.filter($"row_number" === 1)
			.drop($"row_number").select(
					yueProfFirstAllDF("CLM_ADJSTMNT_KEY").as("first_CLM_ADJSTMNT_KEY"),
					yueProfFirstAllDF("CLM_SRVC_FIRST_DT"))

			println(s"Volatile table 4 parsed ")
			yueProfFirstDF
	}
}

case class yueProfLastDF(yueProfLineDF: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			//---------------	Volatile Table 5 (YUE_2017_PROF_LAST) --------------------------
			val yueProfLastAllDF = yueProfLineDF.
			select($"CLM_ADJSTMNT_KEY", $"CLM_LINE_SRVC_END_DT".as("CLM_SRVC_END_DT"))

			val windowFunc2 = Window.partitionBy($"CLM_ADJSTMNT_KEY").orderBy($"CLM_SRVC_END_DT".desc)
			val yueProfLastDF = yueProfLastAllDF.withColumn("row_number", row_number().over(windowFunc2))
			.filter($"row_number" === 1)
			.drop($"row_number").select(
					yueProfLastAllDF("CLM_ADJSTMNT_KEY").as("last_CLM_ADJSTMNT_KEY"),
					yueProfLastAllDF("CLM_SRVC_END_DT"))

			println(s"Volatile table 5 parsed ")
			yueProfLastDF
	}
}

case class yueProfClm(yueProf1: Dataset[Row], yueProfFirstDF: Dataset[Row], yueProfLastDF: Dataset[Row], yueProfLobDF: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			//----------------- Volatile Table 6 (YUE_2017_PROF_CLM)  --------------------------
			val yueProfClmDF = yueProf1.repartition(yueProf1("CLM_ADJSTMNT_KEY")).join(yueProfFirstDF, yueProf1("CLM_ADJSTMNT_KEY") === yueProfFirstDF("first_CLM_ADJSTMNT_KEY"), "left").
			repartition(yueProf1("CLM_ADJSTMNT_KEY"))
			.join(yueProfLastDF, yueProf1("CLM_ADJSTMNT_KEY") === yueProfLastDF("last_CLM_ADJSTMNT_KEY"), "left")
			.repartition(yueProf1("CLM_ADJSTMNT_KEY"))
			.join(yueProfLobDF, yueProf1("CLM_ADJSTMNT_KEY") === yueProfLobDF("lob_CLM_ADJSTMNT_KEY"), "left")
			.select(
					yueProf1("MBR_KEY"),
					yueProf1("CLM_ADJSTMNT_KEY"),
					yueProf1("DIAG_1_CD"),
					yueProf1("tot_bil"),
					yueProf1("tot_alow"),
					yueProf1("tot_paid"),
					yueProfFirstDF("CLM_SRVC_FIRST_DT"),
					yueProfLastDF("CLM_SRVC_END_DT"),
					yueProfLobDF("PCR_CNTRCTD_ST_DESC"),
					yueProfLobDF("PCR_LOB_DESC"),
					yueProfLobDF("PCR_PROD_DESC"))

			println(s"Volatile table 6 parsed ")
			yueProfClmDF
	}
}

case class yueProfMcid(mdmPpltnXwalk: Dataset[Row], yueProfClmDF: Dataset[Row], mbr: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			//----------------- Volatile Table 7 (YUE_2017_PROF_MCID)  --------------------------

			val yueProfMcidAllDF = yueProfClmDF.
			join(mbr, yueProfClmDF("MBR_KEY") === mbr("MBR_KEY"), "left")
			.withColumn("trim_concat", concat(trim(mbr("MBRSHP_SOR_CD")), lit("-"), trim(mbr("SRC_GRP_NBR")), lit("-"), trim(mbr("SBSCRBR_ID")), lit("-"), trim(mbr("MBR_SQNC_NBR"))))
			.join(
					mdmPpltnXwalk,
					$"trim_concat" === mdmPpltnXwalk("tknzd_src_key") &&  to_date(mdmPpltnXwalk("XWALK_TRMNTN_DT")) === to_date(lit("8888-12-31")), "left")

			val windowFunc3 = Window.partitionBy(yueProfClmDF("CLM_ADJSTMNT_KEY"), mdmPpltnXwalk("ALT_KEY"))
			.orderBy($"XWALK_TRMNTN_DT".desc, $"XWALK_EFCTV_DT".desc)

			val yueProfMcidDF = yueProfMcidAllDF.withColumn("row_number", row_number().over(windowFunc3))
			.filter($"row_number" === 1)
			.select(
					yueProfClmDF("MBR_KEY"),
					yueProfClmDF("CLM_ADJSTMNT_KEY"),
					yueProfClmDF("DIAG_1_CD"),
					yueProfClmDF("tot_bil"),
					yueProfClmDF("tot_alow"),
					yueProfClmDF("tot_paid"),
					yueProfClmDF("CLM_SRVC_FIRST_DT"),
					yueProfClmDF("CLM_SRVC_END_DT"),
					yueProfClmDF("PCR_CNTRCTD_ST_DESC"),
					yueProfClmDF("PCR_LOB_DESC"),
					yueProfClmDF("PCR_PROD_DESC"),
					mdmPpltnXwalk("MCID"))
			.distinct().filter(mdmPpltnXwalk("MCID").isNotNull)

			println(s"Volatile table 7 parsed ")
			yueProfMcidDF
	}
}

case class yueProfDF(yueProfMcidDF: Dataset[Row], mbrAdrsHist: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._



			//----------------- final table  (YUE_2017_PROF) -- SQL 1 --------------------------
			val yueProfFinalAll = yueProfMcidDF.repartition(yueProfMcidDF("CLM_ADJSTMNT_KEY")).join(
					mbrAdrsHist,
					yueProfMcidDF("MBR_KEY") === mbrAdrsHist("MBR_KEY") && yueProfMcidDF("CLM_SRVC_FIRST_DT").between(mbrAdrsHist("VRSN_OPEN_DT"), mbrAdrsHist("VRSN_CLOS_DT")), "left")
			// && yueProfMcidDF("CLM_SRVC_FIRST_DT").between(mbrAdrsHist("VRSN_OPEN_DT"), mbrAdrsHist("VRSN_CLOS_DT"))
			.
			select(
					yueProfMcidDF("MBR_KEY"),
					yueProfMcidDF("CLM_ADJSTMNT_KEY"),
					yueProfMcidDF("DIAG_1_CD"),
					yueProfMcidDF("tot_bil"),
					yueProfMcidDF("tot_alow"),
					yueProfMcidDF("tot_paid"),
					yueProfMcidDF("CLM_SRVC_FIRST_DT"),
					yueProfMcidDF("CLM_SRVC_END_DT"),
					yueProfMcidDF("PCR_CNTRCTD_ST_DESC"),
					yueProfMcidDF("PCR_LOB_DESC"),
					yueProfMcidDF("PCR_PROD_DESC"),
					yueProfMcidDF("mcid"),
					mbrAdrsHist("ZIP_CD").as("mem_zip"),
					(when($"ADRS_TYPE_CD" === "13", 1)
							.when($"ADRS_TYPE_CD" === "2", 2)
							.when($"ADRS_TYPE_CD" === "28", 3)
							.when($"ADRS_TYPE_CD" === "3", 4)
							.when($"ADRS_TYPE_CD" === "12", 5)
							.when($"ADRS_TYPE_CD" === "22", 6)
							.when($"ADRS_TYPE_CD" === "21", 7)
							.when($"ADRS_TYPE_CD" === "19", 8)
							.when($"ADRS_TYPE_CD" === "27", 9)
							.when($"ADRS_TYPE_CD" === "29", 10)
							.when($"ADRS_TYPE_CD" === "UNK", 11)).alias("lvl")).distinct

			val windowFunc4 = Window.partitionBy(yueProfMcidDF("CLM_ADJSTMNT_KEY")).orderBy($"lvl".desc, $"lvl".asc)

			val yueProfDF =
			yueProfFinalAll.withColumn("row_number", row_number().over(windowFunc4))
			.filter($"row_number" === 1)
			.drop($"row_number")

			yueProfDF

	}
}

case class ProfNppes(srcCmsNppes: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val yueProfNppesFil = srcCmsNppes.filter($"ENTY_TYPE_CD".isin("1", "2"))

			val yueProfNppes = yueProfNppesFil.select(
					srcCmsNppes("NPI"),
					srcCmsNppes("ENTY_TYPE_CD"),
					srcCmsNppes("PROV_PRCTC_ADRS_LINE_1_TXT").as("NPPES_ADDR1"),
					srcCmsNppes("PROV_PRCTC_ADRS_LINE_2_TXT").as("NPPES_ADDR2"),
					srcCmsNppes("PROV_PRCTC_CITY_NM").as("NPPES_CITY"),
					srcCmsNppes("PROV_PRCTC_ST_PRVNC_NM").as("NPPES_ST"),
					substring(srcCmsNppes("PROV_PRCTC_ZIP_CD"), 1, 5).as("NPPES_ZIP"),
					(when(srcCmsNppes("PRMRY_TXNMY_1_IND") === "Y", $"TXNMY_1_CD").
							when(srcCmsNppes("PRMRY_TXNMY_2_IND") === "Y", $"TXNMY_2_CD").
							when(srcCmsNppes("PRMRY_TXNMY_3_IND") === "Y", $"TXNMY_3_CD").
							when(srcCmsNppes("PRMRY_TXNMY_4_IND") === "Y", $"TXNMY_4_CD").
							when(srcCmsNppes("PRMRY_TXNMY_5_IND") === "Y", $"TXNMY_5_CD").
							when($"PRMRY_TXNMY_6_IND" === "Y", $"TXNMY_6_CD").
							when($"PRMRY_TXNMY_7_IND" === "Y", $"TXNMY_7_CD").
							when($"PRMRY_TXNMY_8_IND" === "Y", $"TXNMY_8_CD").
							when($"PRMRY_TXNMY_9_IND" === "Y", $"TXNMY_9_CD").
							when($"PRMRY_TXNMY_10_IND" === "Y", $"TXNMY_10_CD").
							when($"PRMRY_TXNMY_11_IND" === "Y", $"TXNMY_11_CD").
							when($"PRMRY_TXNMY_12_IND" === "Y", $"TXNMY_12_CD").
							when($"PRMRY_TXNMY_13_IND" === "Y", $"TXNMY_13_CD").
							when($"PRMRY_TXNMY_14_IND" === "Y", $"TXNMY_14_CD").
							when($"PRMRY_TXNMY_15_IND" === "Y", $"TXNMY_15_CD").
							when($"PRMRY_TXNMY_1_IND" === "X", $"TXNMY_1_CD").
							when($"PRMRY_TXNMY_2_IND" === "X", $"TXNMY_2_CD").
							when($"PRMRY_TXNMY_3_IND" === "X", $"TXNMY_3_CD").
							when($"PRMRY_TXNMY_4_IND" === "X", $"TXNMY_4_CD").
							when($"PRMRY_TXNMY_5_IND" === "X", $"TXNMY_5_CD").
							when($"PRMRY_TXNMY_6_IND" === "X", $"TXNMY_6_CD").
							when($"PRMRY_TXNMY_7_IND" === "X", $"TXNMY_7_CD").
							when($"PRMRY_TXNMY_8_IND" === "X", $"TXNMY_8_CD").
							when($"PRMRY_TXNMY_9_IND" === "X", $"TXNMY_9_CD").
							when($"PRMRY_TXNMY_10_IND" === "X", $"TXNMY_10_CD").
							when($"PRMRY_TXNMY_11_IND" === "X", $"TXNMY_11_CD").
							when($"PRMRY_TXNMY_12_IND" === "X", $"TXNMY_12_CD").
							when($"PRMRY_TXNMY_13_IND" === "X", $"TXNMY_13_CD").
							when($"PRMRY_TXNMY_14_IND" === "X", $"TXNMY_14_CD").
							when($"PRMRY_TXNMY_15_IND" === "X", $"TXNMY_15_CD").
							when($"PRMRY_TXNMY_1_IND" === "N", $"TXNMY_1_CD").
							when($"PRMRY_TXNMY_2_IND" === "N", $"TXNMY_2_CD").
							when($"PRMRY_TXNMY_3_IND" === "N", $"TXNMY_3_CD").
							when($"PRMRY_TXNMY_4_IND" === "N", $"TXNMY_4_CD").
							when($"PRMRY_TXNMY_5_IND" === "N", $"TXNMY_5_CD").
							when($"PRMRY_TXNMY_6_IND" === "N", $"TXNMY_6_CD").
							when($"PRMRY_TXNMY_7_IND" === "N", $"TXNMY_7_CD").
							when($"PRMRY_TXNMY_8_IND" === "N", $"TXNMY_8_CD").
							when($"PRMRY_TXNMY_9_IND" === "N", $"TXNMY_9_CD").
							when($"PRMRY_TXNMY_10_IND" === "N", $"TXNMY_10_CD").
							when($"PRMRY_TXNMY_11_IND" === "N", $"TXNMY_11_CD").
							when($"PRMRY_TXNMY_12_IND" === "N", $"TXNMY_12_CD").
							when($"PRMRY_TXNMY_13_IND" === "N", $"TXNMY_13_CD").
							when($"PRMRY_TXNMY_14_IND" === "N", $"TXNMY_14_CD").
							when($"PRMRY_TXNMY_15_IND" === "N", $"TXNMY_15_CD").
							when($"PRMRY_TXNMY_1_IND" === " ", $"TXNMY_1_CD").
							when($"PRMRY_TXNMY_2_IND" === " ", $"TXNMY_2_CD").
							when($"PRMRY_TXNMY_3_IND" === " ", $"TXNMY_3_CD").
							when($"PRMRY_TXNMY_4_IND" === " ", $"TXNMY_4_CD").
							when($"PRMRY_TXNMY_5_IND" === " ", $"TXNMY_5_CD").
							when($"PRMRY_TXNMY_6_IND" === " ", $"TXNMY_6_CD").
							when($"PRMRY_TXNMY_7_IND" === " ", $"TXNMY_7_CD").
							when($"PRMRY_TXNMY_8_IND" === " ", $"TXNMY_8_CD").
							when($"PRMRY_TXNMY_9_IND" === " ", $"TXNMY_9_CD").
							when($"PRMRY_TXNMY_10_IND" === " ", $"TXNMY_10_CD").
							when($"PRMRY_TXNMY_11_IND" === " ", $"TXNMY_11_CD").
							when($"PRMRY_TXNMY_12_IND" === " ", $"TXNMY_12_CD").
							when($"PRMRY_TXNMY_13_IND" === " ", $"TXNMY_13_CD").
							when($"PRMRY_TXNMY_14_IND" === " ", $"TXNMY_14_CD").
							when($"PRMRY_TXNMY_15_IND" === " ", $"TXNMY_15_CD")).as("TXNMY_CD"),
					(when(srcCmsNppes("ENTY_TYPE_CD").===(1), concat(trim(srcCmsNppes("INDIV_PROV_FRST_NM")), lit("-"), trim(srcCmsNppes("INDIV_PROV_LAST_NM"))))
							when (srcCmsNppes("ENTY_TYPE_CD").===(2), srcCmsNppes("PROV_ORG_FULL_NM"))).as("NPPES_PROVIDER_NAME"))

			yueProfNppes

	}

}

case class yueProfPdl(bkbnIpBv: Dataset[Row],snapNbr : Long) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val bkbnIpBvFil = bkbnIpBv.filter($"SNAP_YEAR_MNTH_NBR" === snapNbr)
			bkbnIpBvFil.select(
					$"NPI",
					$"EP_TAX_ID",
					$"EP_TAX_ID_1099_NM",
					$"IP_LGL_FRST_NM",
					$"IP_LGL_LAST_NM",
					$"SPCLTY_PRMRY_CD",
					$"SPCLTY_PRMRY_DESC",
					$"PADRS_NPI_ZIP_CD").distinct().dropDuplicates("NPI")

	}
}

case class yueProfPdlNpi(bkbnIpBv: Dataset[Row],snapNbr : Long) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val bkbnIpBvFil = bkbnIpBv.filter($"SNAP_YEAR_MNTH_NBR" === snapNbr)

			bkbnIpBvFil.select(
					$"NPI",
					$"SPCLTY_PRMRY_CD".as("NPI_EP_SPCLTY_CD"),
					$"SPCLTY_PRMRY_DESC".as("NPI_EP_SPCLTY_DESC")).distinct().dropDuplicates("NPI")
	}
}

case class yueProfPdlTax(bkbnIpBv: Dataset[Row],snapNbr : Long) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val bkbnIpBvFil = bkbnIpBv.filter($"SNAP_YEAR_MNTH_NBR" === snapNbr)

			bkbnIpBvFil.select(
					$"EP_TAX_ID",
					$"EP_TAX_ID_1099_NM").distinct().dropDuplicates("EP_TAX_ID")
	}
}

case class ProfRefNpi(clmProvFil: Dataset[Row], clmLineProvFil: Dataset[Row], tabyueProf: String, tabyueProfPdlNpi: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val yueProf = spark.sql(s"select * from $stagingHiveDB.$tabyueProf")
			val yueProfPdlNpi = spark.sql(s"select * from $stagingHiveDB.$tabyueProfPdlNpi")
      
      val clmProv = clmProvFil.filter(clmProvFil("CLM_PROV_ROLE_CD").isin("02") &&
					upper(clmProvFil("CLM_PROV_ID_TYPE_CD")).isin("NPI") &&
					length(clmProvFil("SRC_CLM_PROV_ID")).isin("10") &&
					substring(clmProvFil("SRC_CLM_PROV_ID"), 0, 1).isin("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
					&& clmProvFil("SRC_CLM_PROV_ID").!==("0000000000"))
			
			val yueProfRefNpiVolatile = yueProf.join(clmProv, yueProf("CLM_ADJSTMNT_KEY") === clmProv("CLM_ADJSTMNT_KEY"), "left")
			.select(
					yueProf("CLM_ADJSTMNT_KEY"),
					clmProv("CLM_PROV_ROLE_CD"),
					clmProv("SRC_CLM_PROV_ID"),
					clmProv("CLM_PROV_ID_TYPE_CD"),
					clmProv("RPTG_PROV_ZIP_CD").as("REF_NPI_PROV_ZIP"),
					lit(1).as("lvl"),
					when(clmProv("SRC_CLM_PROV_ID").isNotNull, clmProv("SRC_CLM_PROV_ID"))
					.otherwise(null).as("REF_NPI"),
					when(upper(clmProv("CLM_PROV_ID_TYPE_CD")) === "NPI", "A").otherwise("B").as("NPI_ID_TYPE")).distinct.persist(StorageLevel.DISK_ONLY)

      val clmLineProv = clmLineProvFil.filter(clmLineProvFil("CLM_LINE_PROV_ROLE_CD").isin("02") &&
					upper(clmLineProvFil("CLM_LINE_PROV_ID_TYPE_CD")).isin("NPI") &&
					length(clmLineProvFil("SRC_CLM_LINE_PROV_ID")).isin("10") &&
					substring(clmLineProvFil("SRC_CLM_LINE_PROV_ID"), 0, 1).isin("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
					&& clmLineProvFil("SRC_CLM_LINE_PROV_ID").!==("0000000000"))
      val yueProfRefNpiLine = yueProf.join(clmLineProv, yueProf("CLM_ADJSTMNT_KEY") === clmLineProv("CLM_ADJSTMNT_KEY"), "left")
			.select(
					yueProf("CLM_ADJSTMNT_KEY"),
					clmLineProv("CLM_LINE_PROV_ROLE_CD").as("CLM_PROV_ROLE_CD"),
					clmLineProv("SRC_CLM_LINE_PROV_ID").as("SRC_CLM_PROV_ID"),
					clmLineProv("CLM_LINE_PROV_ID_TYPE_CD").as("CLM_PROV_ID_TYPE_CD"),
					clmLineProv("RPTG_PROV_ZIP_CD").as("REF_NPI_PROV_ZIP"),
					lit(2).as("lvl"),
					when(clmLineProv("SRC_CLM_LINE_PROV_ID").isNotNull, clmLineProv("SRC_CLM_LINE_PROV_ID"))
					.otherwise(null).as("REF_NPI"),
					when(upper(clmLineProv("CLM_LINE_PROV_ID_TYPE_CD")) === "NPI", "A").otherwise("B").as("NPI_ID_TYPE")).distinct.persist(StorageLevel.DISK_ONLY)

			val yueProfRefNpiVolatile1 = yueProfRefNpiVolatile.filter($"REF_NPI".isNotNull).union(yueProfRefNpiLine.filter($"REF_NPI".isNotNull))
			.select(
					$"CLM_ADJSTMNT_KEY",
					$"REF_NPI",
					$"REF_NPI_PROV_ZIP",
					$"NPI_ID_TYPE",
					$"LVL")
			val yueProfRefNpiAll = yueProfRefNpiVolatile1.join(yueProfPdlNpi, yueProfRefNpiVolatile1("REF_NPI") === yueProfPdlNpi("NPI"), "left")
			val windowFunc = Window.partitionBy($"clm_adjstmnt_key").orderBy($"npi_id_type", $"lvl")
			
			val yueProfRefNpi = yueProfRefNpiAll.withColumn("row_number", row_number().over(windowFunc))
			.filter($"row_number" === 1)
			.select(
					yueProfRefNpiVolatile1("CLM_ADJSTMNT_KEY"),
					yueProfRefNpiVolatile1("REF_NPI"),
					yueProfPdlNpi("NPI_EP_SPCLTY_CD").as("REF_NPI_EP_SPCLTY_CD"),
					yueProfPdlNpi("NPI_EP_SPCLTY_DESC").as("REF_NPI_EP_SPCLTY_DESC"),
					yueProfRefNpiVolatile1("REF_NPI_PROV_ZIP"))

			yueProfRefNpi
	}
}


case class ProfRefTax(clmProvFil: Dataset[Row], clmLineProvFil: Dataset[Row], tabyueProf: String, tabyueProfPdlTax: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val yueProf = spark.sql(s"select * from $stagingHiveDB.$tabyueProf")
			val yueProfPdlTax = spark.sql(s"select * from $stagingHiveDB.$tabyueProfPdlTax")
			
			val clmProv=clmProvFil.filter(trim(clmProvFil("CLM_PROV_ROLE_CD")).isin("02") &&
					upper(trim(clmProvFil("CLM_PROV_ID_TYPE_CD"))).isin("TAX") &&
					length(trim(clmProvFil("SRC_CLM_PROV_ID"))).isin("9") &&
					substring(trim(clmProvFil("SRC_CLM_PROV_ID")), 0, 1).isin("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
					&& trim(clmProvFil("SRC_CLM_PROV_ID")).!==("0000000000"))
			val yueProfRefTaxVolatile = yueProf.join(clmProv, yueProf("CLM_ADJSTMNT_KEY") === clmProv("CLM_ADJSTMNT_KEY"), "left")
			
			.select(
					yueProf("CLM_ADJSTMNT_KEY"),
					clmProv("RPTG_PROV_ZIP_CD").as("REF_TAX_PROV_ZIP"),
					lit(1).as("lvl"),
					when(trim(clmProv("SRC_CLM_PROV_ID")).isNotNull, trim(clmProv("SRC_CLM_PROV_ID")))
					.otherwise(lit(null)).as("REF_TAX"),
					when(upper(trim(clmProv("CLM_PROV_ID_TYPE_CD"))) === "TAX", lit("A")).otherwise(lit("B")).as("TAX_ID_TYPE")).distinct.persist(StorageLevel.DISK_ONLY)
			val clmLineProv=clmLineProvFil.filter(clmLineProvFil("CLM_LINE_PROV_ROLE_CD").isin("02") &&
					upper(clmLineProvFil("CLM_LINE_PROV_ID_TYPE_CD")).isin("TAX") &&
					length(clmLineProvFil("SRC_CLM_LINE_PROV_ID")).isin("9") &&
					substring(clmLineProvFil("SRC_CLM_LINE_PROV_ID"), 0, 1).isin("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
					&& clmLineProvFil("SRC_CLM_LINE_PROV_ID").!==("0000000000"))
			
					val yueProfRefTaxLine = yueProf.join(clmLineProv, yueProf("CLM_ADJSTMNT_KEY") === clmLineProv("CLM_ADJSTMNT_KEY"), "left")
			
			.select(
					yueProf("CLM_ADJSTMNT_KEY"),
					clmLineProv("RPTG_PROV_ZIP_CD").as("REF_TAX_PROV_ZIP"),
					lit(2).as("lvl"),
					when(clmLineProv("SRC_CLM_LINE_PROV_ID").isNotNull, clmLineProv("SRC_CLM_LINE_PROV_ID"))
					.otherwise(null).as("REF_TAX"),
					when(upper(clmLineProv("CLM_LINE_PROV_ID_TYPE_CD")) === "TAX", "A").otherwise("B").as("TAX_ID_TYPE")).distinct.persist(StorageLevel.DISK_ONLY)


			val yueProfRefTaxVolatile1 = yueProfRefTaxVolatile.filter($"REF_TAX".isNotNull).union(yueProfRefTaxLine.filter($"REF_TAX".isNotNull))
			.select(
					$"CLM_ADJSTMNT_KEY",
					$"REF_TAX",
					$"REF_TAX_PROV_ZIP",
					$"TAX_ID_TYPE",
					$"LVL")

			
			val yueProfRefTaxAll = yueProfRefTaxVolatile1.join(yueProfPdlTax, yueProfRefTaxVolatile1("REF_TAX") === yueProfPdlTax("EP_TAX_ID"), "left")

			val windowFunc = Window.partitionBy($"clm_adjstmnt_key").orderBy($"tax_id_type", $"lvl")

			val yueProfRefTax = yueProfRefTaxAll.withColumn("row_number", row_number().over(windowFunc))
			.filter($"row_number" === 1)
			.select(
					yueProfRefTaxVolatile1("CLM_ADJSTMNT_KEY"),
					yueProfRefTaxVolatile1("REF_TAX"),
					yueProfPdlTax("EP_TAX_ID_1099_NM").as("REF_EP_PROV_NAME"),
					yueProfRefTaxVolatile1("REF_TAX_PROV_ZIP"))

			yueProfRefTax

	}
}

case class yueProfBilNpi(clmProvFil: Dataset[Row], clmLineProvFil: Dataset[Row], tabyueProf: String, tabyueProfPdlNpi: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val yueProf = spark.sql(s"select * from $stagingHiveDB.$tabyueProf")
			val yueProfPdlNpi = spark.sql(s"select * from $stagingHiveDB.$tabyueProfPdlNpi")
			
			val clmProv= clmProvFil.filter(clmProvFil("CLM_PROV_ROLE_CD").isin("04", "10") &&
					upper(clmProvFil("CLM_PROV_ID_TYPE_CD")).isin("NPI") &&
					length(clmProvFil("SRC_CLM_PROV_ID")).isin("10") &&
					substring(clmProvFil("SRC_CLM_PROV_ID"), 1, 1).isin("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
					&& clmProvFil("SRC_CLM_PROV_ID").!==("0000000000"))
			val yueProfBilNpiVolatile = yueProf.join(clmProv, yueProf("CLM_ADJSTMNT_KEY") === clmProv("CLM_ADJSTMNT_KEY"), "left")
			
			.select(
					yueProf("CLM_ADJSTMNT_KEY"),
					clmProv("CLM_PROV_ROLE_CD"),
					clmProv("SRC_CLM_PROV_ID"),
					clmProv("CLM_PROV_ID_TYPE_CD"),
					clmProv("RPTG_PROV_ZIP_CD").as("BIL_NPI_PROV_ZIP"),
					lit(1).as("lvl"),
					when(clmProv("SRC_CLM_PROV_ID").isNotNull, clmProv("SRC_CLM_PROV_ID"))
					.otherwise(null).as("BIL_NPI"),
					when(upper(clmProv("CLM_PROV_ID_TYPE_CD")) === "NPI", "A").otherwise("B").as("NPI_ID_TYPE")).distinct().persist(StorageLevel.DISK_ONLY)
					
		  val clmLineProv=clmLineProvFil.filter(clmLineProvFil("CLM_LINE_PROV_ROLE_CD").isin("04", "10") &&
					upper(clmLineProvFil("CLM_LINE_PROV_ID_TYPE_CD")).isin("NPI") &&
					length(clmLineProvFil("SRC_CLM_LINE_PROV_ID")).isin("10") &&
					substring(clmLineProvFil("SRC_CLM_LINE_PROV_ID"), 1, 1).isin("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
					&& clmLineProvFil("SRC_CLM_LINE_PROV_ID").!==("0000000000"))
					
			val yueProfBilNpiLine = yueProf.join(clmLineProv, yueProf("CLM_ADJSTMNT_KEY") === clmLineProv("CLM_ADJSTMNT_KEY"), "left")
			
			.select(
					yueProf("CLM_ADJSTMNT_KEY"),
					clmLineProv("CLM_LINE_PROV_ROLE_CD").as("CLM_PROV_ROLE_CD"),
					clmLineProv("SRC_CLM_LINE_PROV_ID").as("SRC_CLM_PROV_ID"),
					clmLineProv("CLM_LINE_PROV_ID_TYPE_CD").as("CLM_PROV_ID_TYPE_CD"),
					clmLineProv("RPTG_PROV_ZIP_CD").as("BIL_NPI_PROV_ZIP"),
					lit(2).as("lvl"),
					when(clmLineProv("SRC_CLM_LINE_PROV_ID").isNotNull, clmLineProv("SRC_CLM_LINE_PROV_ID"))
					.otherwise(null).as("BIL_NPI"),
					when(upper(clmLineProv("CLM_LINE_PROV_ID_TYPE_CD")) === "NPI", "A").otherwise("B").as("NPI_ID_TYPE")).distinct().persist(StorageLevel.DISK_ONLY)

			val yueProfBilNpiVolatile1 = yueProfBilNpiVolatile.filter($"BIL_NPI".isNotNull).union(yueProfBilNpiLine.filter($"BIL_NPI".isNotNull))
			.select(
					$"CLM_ADJSTMNT_KEY",
					$"BIL_NPI",
					$"BIL_NPI_PROV_ZIP",
					$"NPI_ID_TYPE",
					$"LVL")

		
			val yueProfBilNpiAll = yueProfBilNpiVolatile1.join(yueProfPdlNpi, yueProfBilNpiVolatile1("BIL_NPI") === yueProfPdlNpi("NPI"), "left")

			val windowFunc = Window.partitionBy($"clm_adjstmnt_key").orderBy($"npi_id_type", $"lvl")

			val yueProfRefNpi = yueProfBilNpiAll.withColumn("row_number", row_number().over(windowFunc))
			.filter($"row_number" === 1)
			.select(
					yueProfBilNpiVolatile1("CLM_ADJSTMNT_KEY"),
					yueProfBilNpiVolatile1("BIL_NPI"),
					yueProfPdlNpi("NPI_EP_SPCLTY_CD").as("BIL_NPI_EP_SPCLTY_CD"),
					yueProfPdlNpi("NPI_EP_SPCLTY_DESC").as("BIL_NPI_EP_SPCLTY_DESC"),
					yueProfBilNpiVolatile1("BIL_NPI_PROV_ZIP"))

			yueProfRefNpi

	}
}

case class yueProfBilTax(clmProvFil: Dataset[Row], clmLineProvFil: Dataset[Row], tabyueProf: String, tabyueProfPdlTax: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val yueProf = spark.sql(s"select * from $stagingHiveDB.$tabyueProf")
			val yueProfPdlTax = spark.sql(s"select * from $stagingHiveDB.$tabyueProfPdlTax")
			
			val clmProv=clmProvFil.filter(clmProvFil("CLM_PROV_ROLE_CD").isin("04", "10") &&
					upper(clmProvFil("CLM_PROV_ID_TYPE_CD")).isin("TAX") &&
					length(clmProvFil("SRC_CLM_PROV_ID")).isin("9") &&
					substring(clmProvFil("SRC_CLM_PROV_ID"), 1, 1).isin("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
					&& clmProvFil("SRC_CLM_PROV_ID").!==("0000000000"))
					
			val yueProfBilTaxVolatile = yueProf.join(clmProv, yueProf("CLM_ADJSTMNT_KEY") === clmProv("CLM_ADJSTMNT_KEY"), "left")
			
			.select(
					yueProf("CLM_ADJSTMNT_KEY"),
					clmProv("RPTG_PROV_ZIP_CD").as("BIL_TAX_PROV_ZIP"),
					lit(1).as("lvl"),
					when(clmProv("SRC_CLM_PROV_ID").isNotNull, clmProv("SRC_CLM_PROV_ID"))
					.otherwise(null).as("BIL_TAX"),
					when(upper(clmProv("CLM_PROV_ID_TYPE_CD")) === "TAX", "A").otherwise("B").as("TAX_ID_TYPE")).distinct().persist(StorageLevel.DISK_ONLY)
					
			val clmLineProv =clmLineProvFil.filter(clmLineProvFil("CLM_LINE_PROV_ROLE_CD").isin("04", "10") &&
					upper(clmLineProvFil("CLM_LINE_PROV_ID_TYPE_CD")).isin("TAX") &&
					length(clmLineProvFil("SRC_CLM_LINE_PROV_ID")).isin("9") &&
					substring(clmLineProvFil("SRC_CLM_LINE_PROV_ID"), 1, 1).isin("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
					&& clmLineProvFil("SRC_CLM_LINE_PROV_ID").!==("0000000000"))
					
			val yueProfBilTaxLine = yueProf.join(clmLineProv, yueProf("CLM_ADJSTMNT_KEY") === clmLineProv("CLM_ADJSTMNT_KEY"), "left")
			
			.select(
					yueProf("CLM_ADJSTMNT_KEY"),
					clmLineProv("RPTG_PROV_ZIP_CD").as("BIL_TAX_PROV_ZIP"),
					lit(2).as("lvl"),
					when(clmLineProv("SRC_CLM_LINE_PROV_ID").isNotNull, clmLineProv("SRC_CLM_LINE_PROV_ID"))
					.otherwise(null).as("BIL_TAX"),
					when(upper(clmLineProv("CLM_LINE_PROV_ID_TYPE_CD")) === "TAX", "A").otherwise("B").as("TAX_ID_TYPE")).distinct().persist(StorageLevel.DISK_ONLY)

			val yueProfBilTaxVolatile1 = yueProfBilTaxVolatile.filter($"BIL_TAX".isNotNull).union(yueProfBilTaxLine.filter($"BIL_TAX".isNotNull))
			.select(
					$"CLM_ADJSTMNT_KEY",
					$"BIL_TAX",
					$"BIL_TAX_PROV_ZIP",
					$"TAX_ID_TYPE",
					$"LVL")

			val yueProfBilTaxAll = yueProfBilTaxVolatile1.join(yueProfPdlTax, yueProfBilTaxVolatile1("BIL_TAX") === yueProfPdlTax("EP_TAX_ID"), "left")

			val windowFunc = Window.partitionBy($"clm_adjstmnt_key").orderBy($"tax_id_type", $"lvl")

			val yueProfBilTax = yueProfBilTaxAll.withColumn("row_number", row_number().over(windowFunc))
			.filter($"row_number" === 1)
			.select(
					yueProfBilTaxVolatile1("CLM_ADJSTMNT_KEY"),
					yueProfBilTaxVolatile1("BIL_TAX"),
					yueProfPdlTax("EP_TAX_ID_1099_NM").as("BIL_EP_PROV_NAME"),
					yueProfBilTaxVolatile1("BIL_TAX_PROV_ZIP"))

			yueProfBilTax

	}
}
case class yueProfRenNpi(clmProvFil: Dataset[Row], clmLineProvFil: Dataset[Row], tabyueProf: String, tabyueProfPdlNpi: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val yueProf = spark.sql(s"select * from $stagingHiveDB.$tabyueProf")
			val yueProfPdlNpi = spark.sql(s"select * from $stagingHiveDB.$tabyueProfPdlNpi")
			
			val clmProv = clmProvFil.filter(clmProvFil("CLM_PROV_ROLE_CD").isin("09") &&
					upper(clmProvFil("CLM_PROV_ID_TYPE_CD")).isin("NPI") &&
					length(clmProvFil("SRC_CLM_PROV_ID")).isin("10") &&
					substring(clmProvFil("SRC_CLM_PROV_ID"), 1, 1).isin("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
					&& clmProvFil("SRC_CLM_PROV_ID").!==("0000000000"))
					
			val yueProfBilNpiVolatile = yueProf.join(clmProv, yueProf("CLM_ADJSTMNT_KEY") === clmProv("CLM_ADJSTMNT_KEY"), "left")
			.select(
					yueProf("CLM_ADJSTMNT_KEY"),
					clmProv("CLM_PROV_ROLE_CD"),
					clmProv("SRC_CLM_PROV_ID"),
					clmProv("CLM_PROV_ID_TYPE_CD"),
					clmProv("RPTG_PROV_ZIP_CD").as("REN_NPI_PROV_ZIP"),
					lit(1).as("lvl"),
					when(clmProv("SRC_CLM_PROV_ID").isNotNull, clmProv("SRC_CLM_PROV_ID"))
					.otherwise(null).as("REN_NPI"),
					when(upper(clmProv("CLM_PROV_ID_TYPE_CD")) === "NPI", "A").otherwise("B").as("NPI_ID_TYPE")).distinct().persist(StorageLevel.DISK_ONLY)
					
			val clmLineProv =clmLineProvFil.filter(clmLineProvFil("CLM_LINE_PROV_ROLE_CD").isin("09") &&
					upper(clmLineProvFil("CLM_LINE_PROV_ID_TYPE_CD")).isin("NPI") &&
					length(clmLineProvFil("SRC_CLM_LINE_PROV_ID")).isin("10") &&
					substring(clmLineProvFil("SRC_CLM_LINE_PROV_ID"), 1, 1).isin("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
					&& clmLineProvFil("SRC_CLM_LINE_PROV_ID").!==("0000000000"))
					
					
			val yueProfBilNpiLine = yueProf.join(clmLineProv, yueProf("CLM_ADJSTMNT_KEY") === clmLineProv("CLM_ADJSTMNT_KEY"), "left")
			
			.select(
					yueProf("CLM_ADJSTMNT_KEY"),
					clmLineProv("CLM_LINE_PROV_ROLE_CD").as("CLM_PROV_ROLE_CD"),
					clmLineProv("SRC_CLM_LINE_PROV_ID").as("SRC_CLM_PROV_ID"),
					clmLineProv("CLM_LINE_PROV_ID_TYPE_CD").as("CLM_PROV_ID_TYPE_CD"),
					clmLineProv("RPTG_PROV_ZIP_CD").as("REN_NPI_PROV_ZIP"),
					lit(2).as("lvl"),
					when(clmLineProv("SRC_CLM_LINE_PROV_ID").isNotNull, clmLineProv("SRC_CLM_LINE_PROV_ID"))
					.otherwise(null).as("REN_NPI"),
					when(upper(clmLineProv("CLM_LINE_PROV_ID_TYPE_CD")) === "NPI", "A").otherwise("B").as("NPI_ID_TYPE")).distinct().persist(StorageLevel.DISK_ONLY)

			val yueProfBilNpiVolatile1 = yueProfBilNpiVolatile.filter($"REN_NPI".isNotNull).union(yueProfBilNpiLine.filter($"REN_NPI".isNotNull))
			.select(
					$"CLM_ADJSTMNT_KEY",
					$"REN_NPI",
					$"REN_NPI_PROV_ZIP",
					$"NPI_ID_TYPE",
					$"LVL")


			val yueProfBilNpiAll = yueProfBilNpiVolatile1.join(yueProfPdlNpi, yueProfBilNpiVolatile1("REN_NPI") === yueProfPdlNpi("NPI"), "left")

			val windowFunc = Window.partitionBy($"clm_adjstmnt_key").orderBy($"npi_id_type", $"lvl")

			val yueProfRefNpi = yueProfBilNpiAll.withColumn("row_number", row_number().over(windowFunc))
			.filter($"row_number" === 1)
			.select(
					yueProfBilNpiVolatile1("CLM_ADJSTMNT_KEY"),
					yueProfBilNpiVolatile1("REN_NPI"),
					yueProfPdlNpi("NPI_EP_SPCLTY_CD").as("REN_NPI_EP_SPCLTY_CD"),
					yueProfPdlNpi("NPI_EP_SPCLTY_DESC").as("REN_NPI_EP_SPCLTY_DESC"),
					yueProfBilNpiVolatile1("REN_NPI_PROV_ZIP"))

			yueProfRefNpi

	}
}

case class yueProfRenTax(clmProvFil: Dataset[Row], clmLineProvFil: Dataset[Row], tabyueProf: String, tabyueProfPdlTax: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val yueProf = spark.sql(s"select * from $stagingHiveDB.$tabyueProf")
			val yueProfPdlTax = spark.sql(s"select * from $stagingHiveDB.$tabyueProfPdlTax")
			
			val clmProv=clmProvFil.filter(clmProvFil("CLM_PROV_ROLE_CD").isin("09") &&
					upper(clmProvFil("CLM_PROV_ID_TYPE_CD")).isin("TAX") &&
					length(clmProvFil("SRC_CLM_PROV_ID")).isin("9") &&
					substring(clmProvFil("SRC_CLM_PROV_ID"), 1, 1).isin("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
					&& clmProvFil("SRC_CLM_PROV_ID").!==("0000000000"))
			val yueProfBilTaxVolatile = yueProf.join(clmProv, yueProf("CLM_ADJSTMNT_KEY") === clmProv("CLM_ADJSTMNT_KEY"), "left")
			
			.select(
					yueProf("CLM_ADJSTMNT_KEY"),
					clmProv("RPTG_PROV_ZIP_CD").as("REN_TAX_PROV_ZIP"),
					lit(1).as("lvl"),
					when(clmProv("SRC_CLM_PROV_ID").isNotNull, clmProv("SRC_CLM_PROV_ID"))
					.otherwise(null).as("REN_TAX"),
					when(upper(clmProv("CLM_PROV_ID_TYPE_CD")) === "TAX", "A").otherwise("B").as("TAX_ID_TYPE")).distinct().persist(StorageLevel.DISK_ONLY)
					
			val clmLineProv= clmLineProvFil.filter(clmLineProvFil("CLM_LINE_PROV_ROLE_CD").isin("09") &&
					upper(clmLineProvFil("CLM_LINE_PROV_ID_TYPE_CD")).isin("TAX") &&
					length(clmLineProvFil("SRC_CLM_LINE_PROV_ID")).isin("9") &&
					substring(clmLineProvFil("SRC_CLM_LINE_PROV_ID"), 1, 1).isin("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
					&& clmLineProvFil("SRC_CLM_LINE_PROV_ID").!==("0000000000"))
					
			val yueProfBilTaxLine = yueProf.join(clmLineProv, yueProf("CLM_ADJSTMNT_KEY") === clmLineProv("CLM_ADJSTMNT_KEY"), "left")
			
			.select(
					yueProf("CLM_ADJSTMNT_KEY"),
					clmLineProv("RPTG_PROV_ZIP_CD").as("REN_TAX_PROV_ZIP"),
					lit(2).as("lvl"),
					when(clmLineProv("SRC_CLM_LINE_PROV_ID").isNotNull, clmLineProv("SRC_CLM_LINE_PROV_ID"))
					.otherwise(null).as("REN_TAX"),
					when(upper(clmLineProv("CLM_LINE_PROV_ID_TYPE_CD")) === "TAX", "A").otherwise("B").as("TAX_ID_TYPE")).distinct().persist(StorageLevel.DISK_ONLY)

			val yueProfBilTaxVolatile1 = yueProfBilTaxVolatile.filter($"REN_TAX".isNotNull).union(yueProfBilTaxLine.filter($"REN_TAX".isNotNull))
			.select(
					$"CLM_ADJSTMNT_KEY",
					$"REN_TAX",
					$"REN_TAX_PROV_ZIP",
					$"TAX_ID_TYPE",
					$"LVL")

			val yueProfBilTaxAll = yueProfBilTaxVolatile1.join(yueProfPdlTax, yueProfBilTaxVolatile1("REN_TAX") === yueProfPdlTax("EP_TAX_ID"), "left")

			val windowFunc = Window.partitionBy($"clm_adjstmnt_key").orderBy($"tax_id_type", $"lvl")

			val yueProfBilTax = yueProfBilTaxAll.withColumn("row_number", row_number().over(windowFunc))
			.filter($"row_number" === 1)
			.select(
					yueProfBilTaxVolatile1("CLM_ADJSTMNT_KEY"),
					yueProfBilTaxVolatile1("REN_TAX"),
					yueProfPdlTax("EP_TAX_ID_1099_NM").as("REN_EP_PROV_NAME"),
					yueProfBilTaxVolatile1("REN_TAX_PROV_ZIP"))

			yueProfBilTax

	}
}

case class profFinalReport( tabyueProf: String, tabyueProfRefNpi: String, tabyueProfRefTax: String, tabyueProfBilNpi: String, tabyueProfBilTax: String,
		tabyueProfRenNpi: String, tabyueProfRenTax: String, tabyueProfNppes: String, diagDf: Dataset[Row], stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {

			val yueProf = spark.sql(s"select * from $stagingHiveDB.$tabyueProf")
					val yueProfRefNpi = spark.sql(s"select * from $stagingHiveDB.$tabyueProfRefNpi")
					val yueProfRefTax = spark.sql(s"select * from $stagingHiveDB.$tabyueProfRefTax")
					val yueProfBilNpi = spark.sql(s"select * from $stagingHiveDB.$tabyueProfBilNpi")
					val yueProfBilTax = spark.sql(s"select * from $stagingHiveDB.$tabyueProfBilTax")
					val yueProfRenNpi = spark.sql(s"select * from $stagingHiveDB.$tabyueProfRenNpi")
					val yueProfRenTax = spark.sql(s"select * from $stagingHiveDB.$tabyueProfRenTax")
					val yueProfNppes = spark.sql(s"select * from $stagingHiveDB.$tabyueProfNppes")
					yueProfNppes.repartition(yueProfNppes("NPI"))
					/*H*/ val yueProfNppes1 = yueProfNppes.alias("yueProfNppes1")
					/*I*/ val yueProfNppes2 = yueProfNppes.alias("yueProfNppes2")
					/*J*/ val yueProfNppes3 = yueProfNppes.alias("yueProfNppes3")
			    
					val profFinalJoin_1 = yueProf.repartition(yueProf("clm_adjstmnt_key")).
					join(yueProfRefNpi, yueProf("clm_adjstmnt_key") === yueProfRefNpi("clm_adjstmnt_key"), "left").
					join(yueProfRefTax, yueProf("clm_adjstmnt_key") === yueProfRefTax("clm_adjstmnt_key"), "left").
					join(yueProfBilNpi, yueProf("clm_adjstmnt_key") === yueProfBilNpi("clm_adjstmnt_key"), "left").
					join(yueProfBilTax, yueProf("clm_adjstmnt_key") === yueProfBilTax("clm_adjstmnt_key"), "left").
					join(yueProfRenNpi, yueProf("clm_adjstmnt_key") === yueProfRenNpi("clm_adjstmnt_key"), "left").
					join(yueProfRenTax, yueProf("clm_adjstmnt_key") === yueProfRenTax("clm_adjstmnt_key"), "left").persist(StorageLevel.DISK_ONLY)

					println("Persist compleed")
					val profFinalJoin_2=profFinalJoin_1.join(yueProfNppes1, profFinalJoin_1("REF_NPI") === col("yueProfNppes1.NPI"), "left").
					join(yueProfNppes2, profFinalJoin_1("BIL_NPI") === col("yueProfNppes2.NPI"), "left").
					join(yueProfNppes3, profFinalJoin_1("REN_NPI") === col("yueProfNppes3.NPI"), "left").
					join(diagDf, yueProf("diag_1_cd") === diagDf("diag_cd"), "left").filter(diagDf("DIAG_VRSN_NBR") === "10")

					val profFinal = profFinalJoin_2.select(
							yueProf("clm_adjstmnt_key").alias("clm_adjstmnt_key"),
							(when(yueProf("diag_1_cd").isNull, lit("null")) otherwise (yueProf("diag_1_cd"))).alias("diag_1_cd"),
							(when(yueProf("clm_srvc_frst_dt").isNull, lit("null")) otherwise (to_date(yueProf("clm_srvc_frst_dt")))).alias("clm_srvc_frst_dt"),
							(when(yueProf("clm_srvc_end_dt").isNull, lit("null")) otherwise (to_date(yueProf("clm_srvc_end_dt")))).alias("clm_srvc_end_dt"),
							yueProf("tot_bil").alias("TOTL_BILLD_AMT"),
							yueProf("tot_alow").alias("TOTL_ALWD_AMT"),
							yueProf("tot_paid").alias("TOTL_PAID_AMT"),
							yueProf("pcr_cntrctd_st_desc").alias("st_cd"),
							yueProf("pcr_lob_desc").alias("lob_id"),
							yueProf("pcr_prod_desc").alias("prod_id"),
							yueProf("MCID"),
							(when(yueProf("mem_zip").isNull, lit("null")) otherwise (yueProf("mem_zip"))).alias("mbr_zip_cd"),

							(when(profFinalJoin_1("REF_NPI").isNull, lit("null")) otherwise (profFinalJoin_1("REF_NPI"))).alias("RFRL_NPI"),
							(when(profFinalJoin_1("REF_NPI_EP_SPCLTY_CD").isNull, lit("null")) otherwise (profFinalJoin_1("REF_NPI_EP_SPCLTY_CD"))).alias("RFRL_PROV_SPCLTY_CD"),
							(when(profFinalJoin_1("REF_NPI_EP_SPCLTY_DESC").isNull, lit("null")) otherwise (profFinalJoin_1("REF_NPI_EP_SPCLTY_DESC"))).alias("RFRL_PROV_SPCLTY_DESC"),
							(when(profFinalJoin_1("REF_NPI_PROV_ZIP").isNull, lit("null")) otherwise (profFinalJoin_1("REF_NPI_PROV_ZIP"))).alias("RFRL_PROV_ZIP_CD"),

							(when(col("yueProfNppes1.TXNMY_CD").isNull, lit("null")) otherwise (col("yueProfNppes1.TXNMY_CD"))).alias("RFRL_PROV_TXNMY_CD"),
							(when(col("yueProfNppes1.NPPES_PROVIDER_NAME").isNull, lit("null")) otherwise (col("yueProfNppes1.NPPES_PROVIDER_NAME"))).alias("RFRL_PROV_NPPES_PROV_NM"),

							(when(profFinalJoin_1("REF_TAX").isNull, lit("null")) otherwise (profFinalJoin_1("REF_TAX"))).alias("RFRL_PROV_TAX_ID"),
							(when(profFinalJoin_1("REF_EP_PROV_NAME").isNull, lit("null")) otherwise (profFinalJoin_1("REF_EP_PROV_NAME"))).alias("RFRL_PROV_TAX_NM"),
							(when(profFinalJoin_1("REF_TAX_PROV_ZIP").isNull, lit("null")) otherwise (profFinalJoin_1("REF_TAX_PROV_ZIP"))).alias("RFRL_PROV_TAX_ZIP_CD"),

							(when(profFinalJoin_1("BIL_NPI").isNull, lit("null")) otherwise (profFinalJoin_1("BIL_NPI"))).alias("BILLG_NPI"),
							(when(profFinalJoin_1("BIL_NPI_EP_SPCLTY_CD").isNull, lit("null")) otherwise (profFinalJoin_1("BIL_NPI_EP_SPCLTY_CD"))).alias("BILLG_PROV_SPCLTY_CD"),
							(when(profFinalJoin_1("BIL_NPI_EP_SPCLTY_DESC").isNull, lit("null")) otherwise (profFinalJoin_1("BIL_NPI_EP_SPCLTY_DESC"))).alias("BILLG_PROV_SPCLTY_DESC"),
							(when(profFinalJoin_1("BIL_NPI_PROV_ZIP").isNull, lit("null")) otherwise (profFinalJoin_1("BIL_NPI_PROV_ZIP"))).alias("BILLG_PROV_ZIP_CD"),

							(when(col("yueProfNppes2.TXNMY_CD").isNull, lit("null")) otherwise (col("yueProfNppes2.TXNMY_CD"))).alias("BILLG_PROV_TXNMY_CD"),
							(when(col("yueProfNppes2.NPPES_PROVIDER_NAME").isNull, lit("null")) otherwise (col("yueProfNppes2.NPPES_PROVIDER_NAME"))).alias("BILLG_PROV_NPPES_PROV_NM"),

							(when(profFinalJoin_1("BIL_TAX").isNull, lit("null")) otherwise (profFinalJoin_1("BIL_TAX"))).alias("BILLG_PROV_TAX_ID"),
							(when(profFinalJoin_1("BIL_EP_PROV_NAME").isNull, lit("null")) otherwise (profFinalJoin_1("BIL_EP_PROV_NAME"))).alias("BILLG_PROV_TAX_NM"),
							(when(profFinalJoin_1("BIL_TAX_PROV_ZIP").isNull, lit("null")) otherwise (profFinalJoin_1("BIL_TAX_PROV_ZIP"))).alias("BILLG_PROV_TAX_ZIP_CD"),

							(when(profFinalJoin_1("REN_NPI").isNull, lit("null")) otherwise (profFinalJoin_1("REN_NPI"))).alias("RNDRG_NPI"),
							(when(profFinalJoin_1("REN_NPI_EP_SPCLTY_CD").isNull, lit("null")) otherwise (profFinalJoin_1("REN_NPI_EP_SPCLTY_CD"))).alias("RNDRG_PROV_SPCLTY_CD"),
							(when(profFinalJoin_1("REN_NPI_EP_SPCLTY_DESC").isNull, lit("null")) otherwise (profFinalJoin_1("REN_NPI_EP_SPCLTY_DESC"))).alias("RNDRG_PROV_SPCLTY_DESC"),
							(when(profFinalJoin_1("REN_NPI_PROV_ZIP").isNull, lit("null")) otherwise (profFinalJoin_1("REN_NPI_PROV_ZIP"))).alias("RNDRG_PROV_ZIP_CD"),

							(when(col("yueProfNppes3.TXNMY_CD").isNull, lit("null")) otherwise (col("yueProfNppes3.TXNMY_CD"))).alias("RNDRG_PROV_TXNMY_CD"),
							(when(col("yueProfNppes3.NPPES_PROVIDER_NAME").isNull, lit("null")) otherwise (col("yueProfNppes3.NPPES_PROVIDER_NAME"))).alias("RNDRG_PROV_NPPES_PROV_NM"),

							(when(profFinalJoin_1("REN_TAX").isNull, lit("null")) otherwise (profFinalJoin_1("REN_TAX"))).alias("RNDRG_PROV_TAX_ID"),
							(when(profFinalJoin_1("REN_EP_PROV_NAME").isNull, lit("null")) otherwise (profFinalJoin_1("REN_EP_PROV_NAME"))).alias("RNDRG_PROV_TAX_NM"),
							(when(profFinalJoin_1("REN_TAX_PROV_ZIP").isNull, lit("null")) otherwise (profFinalJoin_1("REN_TAX_PROV_ZIP"))).alias("RNDRG_PROV_TAX_ZIP_CD"),

							(when(diagDf("ICD_CTGRY_1_TXT").isNull, lit("null")) otherwise (diagDf("ICD_CTGRY_1_TXT"))).alias("ICD_CTGRY_1_TXT"),
							(when(diagDf("ICD_CTGRY_2_TXT").isNull, lit("null")) otherwise (diagDf("ICD_CTGRY_2_TXT"))).alias("ICD_CTGRY_2_TXT"),
							(when(diagDf("ICD_CTGRY_3_TXT").isNull, lit("null")) otherwise (diagDf("ICD_CTGRY_3_TXT"))).alias("ICD_CTGRY_3_TXT"),
							(when(diagDf("ICD_CTGRY_4_TXT").isNull, lit("null")) otherwise (diagDf("ICD_CTGRY_4_TXT"))).alias("ICD_CTGRY_4_TXT")
								)


					val profFinalOut=profFinal.filter(
											profFinal("st_cd").isNotNull
									 && profFinal("lob_id").isNotNull
									 && profFinal("prod_id").isNotNull
									 && profFinal("prod_id").!==("EXCLUSION")
									)
					profFinalOut

	}
}
