package com.am.ndo.api.apiProv

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

case class apiProvWrk1(pcrElgblProvDf: Dataset[Row], bkbnTaxIdDf: Dataset[Row], bkbnIpDf: Dataset[Row], pcrGrpEfcncyRatioFil: Dataset[Row], botSubmrktDistDf: Dataset[Row], snapNbr: Long, columnfilter: String) extends NDOMonad[Dataset[Row]] with Logging {

	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val pcrElgblJoinGrpDf = pcrElgblProvDf.join(bkbnTaxIdDf, pcrElgblProvDf("TAX_ID") === bkbnTaxIdDf("EP_TAX_ID") &&
			bkbnTaxIdDf("SNAP_YEAR_MNTH_NBR") === pcrElgblProvDf("BKBN_SNAP_YEAR_MNTH_NBR"), "inner")
			.join(bkbnIpDf, bkbnIpDf("SNAP_YEAR_MNTH_NBR") === pcrElgblProvDf("BKBN_SNAP_YEAR_MNTH_NBR") &&
			bkbnIpDf("NPI") === pcrElgblProvDf("NPI") &&
			bkbnIpDf("SPCLTY_PRMRY_CD") === pcrElgblProvDf("SPCLTY_PRMRY_CD"), "inner")

			var pcrJoinGrpEfcnyDf = spark.emptyDataFrame
			if (columnfilter.equalsIgnoreCase("TIN")) {

				pcrJoinGrpEfcnyDf = pcrElgblJoinGrpDf.join(pcrGrpEfcncyRatioFil, pcrGrpEfcncyRatioFil("BKBN_SNAP_YEAR_MNTH_NBR") === pcrElgblProvDf("BKBN_SNAP_YEAR_MNTH_NBR") &&
						pcrGrpEfcncyRatioFil("GRP_AGRGTN_ID") === pcrElgblProvDf("TAX_ID") &&
						pcrGrpEfcncyRatioFil("NTWK_ST_CD") === pcrElgblProvDf("NTWK_ST_CD") &&
						pcrGrpEfcncyRatioFil("PCR_LOB_DESC") === pcrElgblProvDf("PCR_LOB_DESC") &&
						pcrGrpEfcncyRatioFil("PEER_MRKT_CD") === pcrElgblProvDf("PEER_MRKT_CD"), "left_outer")

			} else {

				pcrJoinGrpEfcnyDf = pcrElgblJoinGrpDf.join(pcrGrpEfcncyRatioFil, pcrGrpEfcncyRatioFil("BKBN_SNAP_YEAR_MNTH_NBR") === pcrElgblProvDf("BKBN_SNAP_YEAR_MNTH_NBR") &&
						pcrGrpEfcncyRatioFil("GRP_AGRGTN_ID") === pcrElgblProvDf("NPI") &&
						pcrGrpEfcncyRatioFil("NTWK_ST_CD") === pcrElgblProvDf("NTWK_ST_CD") &&
						pcrGrpEfcncyRatioFil("PCR_LOB_DESC") === pcrElgblProvDf("PCR_LOB_DESC") &&
						pcrGrpEfcncyRatioFil("PEER_MRKT_CD") === pcrElgblProvDf("PEER_MRKT_CD"), "left_outer")

			}
			val pcrJoinEfcncy = pcrJoinGrpEfcnyDf.join(botSubmrktDistDf, botSubmrktDistDf("SUBMRKT_CD") === pcrElgblProvDf("PRMRY_SUBMRKT_CD") &&
					botSubmrktDistDf("SUBMRKT_ST_CD") === pcrElgblProvDf("NTWK_ST_CD"), "left_outer")
					.select(pcrElgblProvDf("BKBN_SNAP_YEAR_MNTH_NBR").alias("BKBN_SNAP_YEAR_MNTH_NBR"),
							pcrElgblProvDf("NTWK_ST_CD").alias("ST_CD"),
							(when(pcrElgblProvDf("PCR_LOB_DESC") === "MEDICARE", lit("MEDICARE ADVANTAGE")) otherwise (pcrElgblProvDf("PCR_LOB_DESC"))).alias("LOB_ID"),
							(when(pcrGrpEfcncyRatioFil("BNCHMRK_PROD_DESC").isNull, ((when(pcrElgblProvDf("BNCHMRK_PROD_DESC") === "NO GROUPING", lit("ALL")) otherwise (pcrElgblProvDf("BNCHMRK_PROD_DESC")))))
									when (pcrGrpEfcncyRatioFil("BNCHMRK_PROD_DESC") === "NO GROUPING", lit("ALL")) otherwise (pcrGrpEfcncyRatioFil("BNCHMRK_PROD_DESC"))).alias("Prod_ID"),
							pcrElgblProvDf("TAX_ID").alias("PROV_TAX_ID"),
							bkbnTaxIdDf("EP_TAX_ID_1099_NM").alias("PROV_TAX_NM"),
							pcrElgblProvDf("NPI").alias("NPI"),
							lit(columnfilter).alias("AGRGTN_TYPE_CD"),
							concat(bkbnIpDf("IP_LGL_FRST_NM"), lit(" "), bkbnIpDf("IP_LGL_LAST_NM")).alias("PROV_NM"),
							pcrElgblProvDf("SPCLTY_PRMRY_DESC").alias("SPCLTY_ID"),
							pcrElgblProvDf("SPCLTY_PRMRY_CD").alias("PRIMARY_SPECIALITY_CODE"),
							pcrElgblProvDf("PRMRY_SUBMRKT_CD").alias("SUBMRKT_CD"),
							coalesce(botSubmrktDistDf("SUBMRKT_DESC"), lit("NA")).alias("SUBMRKT_DESC"),
							pcrElgblProvDf("PEER_MRKT_CD").alias("PEER_MRKT_CD")
							)
					.select($"BKBN_SNAP_YEAR_MNTH_NBR", $"ST_CD", $"LOB_ID", $"PROD_ID", $"PROV_TAX_ID", $"PROV_TAX_NM", $"NPI", $"AGRGTN_TYPE_CD",
							$"PROV_NM", $"SPCLTY_ID", $"PRIMARY_SPECIALITY_CODE", $"SUBMRKT_CD", $"SUBMRKT_DESC", $"PEER_MRKT_CD").distinct

					pcrJoinEfcncy

	}
}



case class apiProvCnty(bkbnIpBvFilDf: Dataset[Row], bkbnAdrsRltnshpDf: Dataset[Row], snapNbr: Long, sixMonthHold: Long, stagingHiveDB: String, tabApiProvWrk1: String) extends NDOMonad[Dataset[Row]] with Logging {

	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			import java.util.Calendar
			import java.util.Properties

			val apiProvWrk1Df = spark.sql(s"select PROV_TAX_ID as PROV_TAX_ID, ST_CD as ST_CD from $stagingHiveDB.$tabApiProvWrk1").filter($"AGRGTN_TYPE_CD" === "TIN").distinct()

			val apiWrk1JoinBkbnIpDf = apiProvWrk1Df.join(bkbnIpBvFilDf, bkbnIpBvFilDf("EP_TAX_ID") === apiProvWrk1Df("PROV_TAX_ID") && bkbnIpBvFilDf("NTWK_ST_CD") === apiProvWrk1Df("ST_CD") &&
			bkbnIpBvFilDf("PADRS_TIN_ST_CD") === apiProvWrk1Df("ST_CD"), "left_outer")
			.select(apiProvWrk1Df("PROV_TAX_ID"),
					apiProvWrk1Df("ST_CD"),
					coalesce(bkbnIpBvFilDf("PADRS_TIN_CNTY_NM"), lit("NA")).alias("prov_cnty_nm")).distinct()
			val apiWrk1JoinBkbnRDf = apiProvWrk1Df.join(bkbnAdrsRltnshpDf, bkbnAdrsRltnshpDf("tax_id") === apiProvWrk1Df("PROV_TAX_ID") && bkbnAdrsRltnshpDf("ADRS_ST_PRVNC_CD") === apiProvWrk1Df("st_cd") &&
			bkbnAdrsRltnshpDf("SNAP_YEAR_MNTH_NBR") >= sixMonthHold, "left_outer")
			.groupBy(apiProvWrk1Df("PROV_TAX_ID"),
					apiProvWrk1Df("st_cd"),
					bkbnAdrsRltnshpDf("ADRS_TYPE_CD"),
					bkbnAdrsRltnshpDf("SNAP_YEAR_MNTH_NBR"),
					bkbnAdrsRltnshpDf("CNTY_NM"))
			.agg(count("*").alias("cntr"))
			.select(apiProvWrk1Df("PROV_TAX_ID"),
					apiProvWrk1Df("st_cd"),
					bkbnAdrsRltnshpDf("ADRS_TYPE_CD"),
					bkbnAdrsRltnshpDf("SNAP_YEAR_MNTH_NBR"),
					bkbnAdrsRltnshpDf("CNTY_NM"),
					$"cntr")
			val windowFunc = Window.partitionBy(apiProvWrk1Df("PROV_TAX_ID"), apiProvWrk1Df("st_cd")).orderBy((when(bkbnAdrsRltnshpDf("ADRS_TYPE_CD").isNull, 9)
					when (bkbnAdrsRltnshpDf("ADRS_TYPE_CD") === "1", 1)
					when (bkbnAdrsRltnshpDf("ADRS_TYPE_CD") === "178", 2)
					when (bkbnAdrsRltnshpDf("ADRS_TYPE_CD") === "176", 3)
					when (bkbnAdrsRltnshpDf("ADRS_TYPE_CD") === "3", 4)
					when (bkbnAdrsRltnshpDf("ADRS_TYPE_CD") === "2", 5)
					otherwise (6)).asc,
					apiProvWrk1Df("PROV_TAX_ID").asc,
					bkbnAdrsRltnshpDf("SNAP_YEAR_MNTH_NBR").desc,
					$"cntr".desc, bkbnAdrsRltnshpDf("CNTY_NM").asc)

			val apiBkbnRWindow = apiWrk1JoinBkbnRDf.withColumn("ROW_NUMBER", row_number() over (windowFunc)).filter($"ROW_NUMBER" === 1).drop($"ROW_NUMBER")

			val apiBkbnJoinWindow = apiWrk1JoinBkbnIpDf.join(apiBkbnRWindow, apiBkbnRWindow("PROV_TAX_ID") === apiWrk1JoinBkbnIpDf("PROV_TAX_ID") &&
			apiBkbnRWindow("st_cd") === apiWrk1JoinBkbnIpDf("st_cd"),"left_outer")
			.select(apiWrk1JoinBkbnIpDf("PROV_TAX_ID").alias("PROV_TAX_ID"),
					apiWrk1JoinBkbnIpDf("ST_CD").alias("ST_CD"),
					(when(trim(apiWrk1JoinBkbnIpDf("prov_cnty_nm")).isin("NA", "") && trim(apiBkbnRWindow("CNTY_NM")) === "", lit("NA"))
							when (trim(apiWrk1JoinBkbnIpDf("prov_cnty_nm")).isin("NA", "") && trim(apiBkbnRWindow("CNTY_NM")).isNull, lit("NA"))
							when (trim(apiWrk1JoinBkbnIpDf("prov_cnty_nm")).isin("NA", "") && trim(apiBkbnRWindow("CNTY_NM")).isNotNull, upper(apiBkbnRWindow("CNTY_NM")))
							when (trim(apiWrk1JoinBkbnIpDf("prov_cnty_nm")) === "", lit("NA"))
							otherwise (upper(apiWrk1JoinBkbnIpDf("prov_cnty_nm")))).alias("prov_cnty_nm")).distinct()

			apiBkbnJoinWindow

	}
}

case class apiProvWrk(stagingHiveDB: String, tabApiProvWrk1: String, tabApiProvCnty: String) extends NDOMonad[Dataset[Row]] with Logging {

	override def run(spark: SparkSession): Dataset[Row] = {

			val apiProvWrk1 = spark.sql(s"select * from $stagingHiveDB.$tabApiProvWrk1")
					val apiProvWrkCnty = spark.sql(s"select * from $stagingHiveDB.$tabApiProvCnty")

					val provJoinCnty = apiProvWrk1.join(apiProvWrkCnty, apiProvWrk1("PROV_TAX_ID") === apiProvWrkCnty("PROV_TAX_ID") && apiProvWrk1("ST_CD") === apiProvWrkCnty("ST_CD"), "left_outer")
					.select(apiProvWrk1("BKBN_SNAP_YEAR_MNTH_NBR").alias("BKBN_SNAP_YEAR_MNTH_NBR"),
							apiProvWrk1("ST_CD").alias("ST_CD"),
							apiProvWrk1("LOB_ID").alias("LOB_ID"),
							apiProvWrk1("PROD_ID").alias("PROD_ID"),
							apiProvWrk1("PROV_TAX_ID").alias("PROV_TAX_ID"),
							apiProvWrk1("PROV_TAX_NM").alias("PROV_TAX_NM"),
							apiProvWrk1("NPI").alias("NPI"),
							apiProvWrk1("AGRGTN_TYPE_CD").alias("AGRGTN_TYPE_CD"),
							apiProvWrk1("PROV_NM").alias("PROV_NM"),
							apiProvWrk1("SPCLTY_ID").alias("SPCLTY_ID"),
							apiProvWrk1("PRIMARY_SPECIALITY_CODE").alias("PRIMARY_SPECIALITY_CODE"),
							apiProvWrk1("SUBMRKT_CD").alias("SUBMRKT_CD"),
							apiProvWrk1("SUBMRKT_DESC").alias("SUBMRKT_DESC"),
							apiProvWrk1("PEER_MRKT_CD").alias("PEER_MRKT_CD"),
							(when(apiProvWrk1("AGRGTN_TYPE_CD") === "NPI", lit("NA"))
									when (apiProvWrkCnty("PROV_CNTY_NM").isNull, lit("NA"))
									otherwise (apiProvWrkCnty("PROV_CNTY_NM"))).alias("PROV_CNTY_NM")
							)

					provJoinCnty

	}
}

case class apiProv(warehouseHiveDB: String, tabApiProvWrk: String) extends NDOMonad[Dataset[Row]] with Logging {

	override def run(spark: SparkSession): Dataset[Row] = {

			val apiProvWrk = spark.sql(s"select * from $warehouseHiveDB.$tabApiProvWrk")

					val apiProvDf = apiProvWrk.select(apiProvWrk("BKBN_SNAP_YEAR_MNTH_NBR").alias("BKBN_SNAP_YEAR_MNTH_NBR"),
							apiProvWrk("ST_CD").alias("ST_CD"),
							apiProvWrk("LOB_ID").alias("LOB_ID"),
							apiProvWrk("PROD_ID").alias("PROD_ID"),
							apiProvWrk("PROV_TAX_ID").alias("PROV_TAX_ID"),
							apiProvWrk("PROV_TAX_NM").alias("PROV_TAX_NM"),
							apiProvWrk("NPI").alias("NPI"),
							apiProvWrk("AGRGTN_TYPE_CD").alias("AGRGTN_TYPE_CD"),
							apiProvWrk("PROV_NM").alias("PROV_NM"),
							apiProvWrk("SPCLTY_ID").alias("SPCLTY_ID"),
							apiProvWrk("PRIMARY_SPECIALITY_CODE").alias("PRIMARY_SPECIALITY_CODE"),
							apiProvWrk("SUBMRKT_CD").alias("SUBMRKT_CD"),
							apiProvWrk("SUBMRKT_DESC").alias("SUBMRKT_DESC"),
							apiProvWrk("PROV_CNTY_NM")
							)

					apiProvDf

	}
}