package com.am.ndo.mlr

import java.time.Period

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import com.am.ndo.helper.NDOMonad

import grizzled.slf4j.Logging;
import org.apache.spark.sql.Dataset
import javassist.expr.Cast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

case class mlrSvcArea(ddimSrvcareaFil: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val mlrSvcAreaDf = ddimSrvcareaFil.select(
					ddimSrvcareaFil("SRVCAREA_CD").alias("SRVCAREA_CD"),
					substring(ddimSrvcareaFil("SRVCAREA_ST_SHRT_DESC"), 1, 2).alias("hlth_plan_rgn_cd")).distinct()
			println(s"[NDO-ETL] Showing sample data for mlrSvcAreaDf")
			mlrSvcAreaDf.show
			mlrSvcAreaDf
	}
}

case class mlrBenPlan(ddimBnftPlanFil: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val mlrBenPlanDf = ddimBnftPlanFil.select(
					ddimBnftPlanFil("BNFT_PLAN_CD"),
					(when(ddimBnftPlanFil("PROD_SGMNT_SHRT_DESC").isin("ABD", "LTSS"), ddimBnftPlanFil("PROD_SUB_SGMNT_SHRT_DESC")) otherwise (ddimBnftPlanFil("PROD_SGMNT_SHRT_DESC"))).alias("PROD"),
					ddimBnftPlanFil("PROD_LINE_CD"),
					(when (ddimBnftPlanFil("LOB_SHRT_DESC").===("Medicare"), lit("Medicare Advantage")) otherwise(ddimBnftPlanFil("LOB_SHRT_DESC"))).alias("PROD_LINE_DESC")).distinct()
			println(s"[NDO-ETL] Showing sample data for mlrBenPlanDf")
			mlrBenPlanDf.show
			mlrBenPlanDf
	}
}

case class mlrHcc(ddimHccFil: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val ddimHccDf = ddimHccFil.select(
					ddimHccFil("HCC_CD"),
					(when(ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("Cpttn/Vendor"), lit("Cap/Vendor"))
							when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("Intrst/Net Reins/Oth Med"), lit("Other Medical"))
							when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("OP HH/DME"), lit("Home Health/DME"))
							when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("Total Rvnu"), lit("Total Revenue"))
							when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("Phys - PCP"), lit("Phys PCP"))
							when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("Med MM"), lit("Membership"))
							when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("OP Mgmt Oth"), lit("Outpatient Other"))
							when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("IP Non NF"), lit("Inpatient"))
							when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("NF"), lit("Nursing Facility"))
							when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("OP Surg"), lit("OP Surgery"))
							when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("OP Hgh Tech Rdlgy"), lit("High Tech Radiology"))
							otherwise (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC"))).alias("CATEGORY"),
					(when(ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").===("Capitation Institutional"), lit("Capitation Institutional"))
            when (ddimHccFil("HCC_MDCD_MNGMNT_LOW_SHRT_DESC").===("Cpttn Phys Total"), lit("Capitation Physician"))
            when (ddimHccFil("HCC_MDCD_MNGMNT_LOW_SHRT_DESC").===("Dntl"), lit("Dental"))
            when (ddimHccFil("HCC_MDCD_MNGMNT_LOW_SHRT_DESC").===("Vsn"), lit("Vision"))
            when (ddimHccFil("HCC_MDCD_MNGMNT_LOW_SHRT_DESC").===("Trnsprtn"), lit("Transport"))
            when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").isin("Intrst/Net Reins/Oth Med", "IP Non NF"), ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").===("Outpatient Emergency Room Substance Abuse"), lit("OP ER Substance Abuse"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").===("Outpatient Emergency Room Mental Health"), lit("OP ER Mental Health"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").===("Outpatient Emergency Room Non Behavioral Heallth"), lit("OP ER Non BH"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").===("Outpatient Other - Radiology Computed Tomography"), lit("Outpatient Radiology CT"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").===("Outpatient Other - Radiology Magnetic Resonance Imaging"), lit("Outpatient Radiology MRI"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").===("Outpatient Other - Radiology Nuclear"), lit("Outpatient Radiology Nuclear"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").===("Outpatient Other - Radiology Positron Emission Tomography"), lit("Outpatient Radiology PET"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").===("Outpatient Other - Durable Medical Equipment"), lit("Outpatient Durable Medical Equipment"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").===("Outpatient Other - Personal Care"), lit("Outpatient Personal Care"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").===("Outpatient Other - Home Health Skilled"), lit("Outpatient Home Health Skilled"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").===("Outpatient Other - Home Health Unskilled"), lit("Outpatient Home Health Unskilled"))
            when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("NF"), lit("Nursing Facility Room and Board"))
            when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("OP Surg"), lit("Outpatient Surgery"))
            when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("Med MM"), lit("Membership"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").like("Kick%"), lit("Kick Revenue"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").like("Premium T%"), lit("Premium Tax"))
            when (ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC").isin("Medicaid Premium", "Medicare Membership Monthly Report Part A", "Medicare Membership Monthly Report Part B", "Medicare Membership Monthly Report Part D"), lit("Prem Rvnu"))
            when (ddimHccFil("HCC_MDCD_MNGMNT_LOW_SHRT_DESC").===("PremTax/Experienced Rated Refunds"), lit("Profit Share"))
            when (ddimHccFil("HCC_MDCD_MNGMNT_HIGH_SHRT_DESC").===("Pharmacy Net Rebates"), ddimHccFil("HCC_MDCD_RUN_RT_LOW_LONG_DESC"))
            otherwise (ddimHccFil("HCC_MDCD_MNGMNT_LOW_SHRT_DESC"))).alias("CATEGORY2"))
          .distinct()

			println(s"[NDO-ETL] Showing sample data for ddimHccDf")
			ddimHccDf.show
			ddimHccDf
	}
}

case class mlrMbr(factRsttdMbrshpFil: Dataset[Row], tabMlrSvcArea: String, tabMlrBenPlan: String, tabMlrHcc: String, priorBeginYyyymm: String, priorEndYyyymm: String,
		currentBeginYyyymm: String, currentEndYyyymm: String, priorPeriod: String, currentPeriod: String, warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val mlrSvcAreaDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrSvcArea")
			val mlrBenPlanDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrBenPlan")
			val mlrHccDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrHcc")

			val factRsttdJoin = factRsttdMbrshpFil.join(mlrSvcAreaDf, factRsttdMbrshpFil("SRVCAREA_CD") === mlrSvcAreaDf("SRVCAREA_CD"), "inner")
			val mlrBenJoin = factRsttdJoin.join(mlrBenPlanDf, factRsttdMbrshpFil("BNFT_PLAN_CD") === mlrBenPlanDf("BNFT_PLAN_CD"), "inner")
			val mlrHccJoin = mlrBenJoin.join(mlrHccDf, factRsttdMbrshpFil("HCC_CD") === mlrHccDf("HCC_CD"), "inner")
			println(s"[NDO-ETL] Showing sample data for mlrHccJoin")
			mlrHccJoin.show
			val mlrMbr = mlrHccJoin.select(
					mlrSvcAreaDf("HLTH_PLAN_RGN_CD").alias("PLN"),
					(when(factRsttdMbrshpFil("ANLYTC_INCRD_YEAR_MNTH_NBR").between(priorBeginYyyymm, priorEndYyyymm), lit(priorPeriod))
							when (factRsttdMbrshpFil("ANLYTC_INCRD_YEAR_MNTH_NBR").between(currentBeginYyyymm, currentEndYyyymm), lit(currentPeriod))
							otherwise (
									lit("XX"))).alias("TIME_PERIOD"),
					mlrBenPlanDf("PROD").alias("PROD"),
					factRsttdMbrshpFil("PCP_PROV_TAX_ID").alias("TIN"),
					lit("Membership").alias("CATEGORY"),
					lit("Membership").alias("CATEGORY2"),
					mlrBenPlanDf("PROD_LINE_DESC"),
					factRsttdMbrshpFil("PRORTD_MBR_MNTH_CNT")).groupBy(
							$"PLN",
							$"TIME_PERIOD",
							$"PROD",
							$"TIN",
							$"CATEGORY",
							$"CATEGORY2",
							$"PROD_LINE_DESC").agg(sum($"PRORTD_MBR_MNTH_CNT").alias("AMOUNT")).select(
									$"PLN",
									$"TIME_PERIOD",
									$"PROD",
									$"TIN",
									$"CATEGORY",
									$"CATEGORY2",
									$"PROD_LINE_DESC",
									$"AMOUNT")

			println(s"[NDO-ETL] Showing sample data for mlrMbr")
			mlrMbr.show

			mlrMbr
	}
}

case class mlrMbrRank(tabmlrMbr: String, currentPeriod: String, warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val mlrMbrDf = spark.sql(s"select * from $warehouseHiveDB.$tabmlrMbr")
			println(s"[NDO-ETL] Showing sample data for mlrMbrDf")
			mlrMbrDf.show

			val mlrMbrFil = mlrMbrDf.filter(mlrMbrDf("TIME_PERIOD").===("CURNT_PRD") &&
			    mlrMbrDf("TIN").isin("-2") === false)

			val mlrMbrSel = mlrMbrFil.select(
					$"PLN",
					$"TIME_PERIOD",
					$"TIN",
					$"CATEGORY",
					$"CATEGORY2",
					$"PROD_LINE_DESC",
					$"AMOUNT").groupBy(
							$"PLN",
							$"TIME_PERIOD",
							$"TIN",
							$"CATEGORY",
							$"CATEGORY2",
							$"PROD_LINE_DESC").agg(sum($"AMOUNT").alias("AMT_TOTL")).select(
									$"PLN",
									$"TIME_PERIOD",
									$"TIN",
									$"CATEGORY",
									$"CATEGORY2",
									$"PROD_LINE_DESC",
									$"AMT_TOTL")


			val windowFunc = Window.partitionBy("PLN", "PROD_LINE_DESC").orderBy($"AMT_TOTL".desc,$"TIN".asc)
			val mlrMbrRank = mlrMbrSel.select($"PLN", $"TIN", $"PROD_LINE_DESC", $"AMT_TOTL")
			.withColumn("RANKING", row_number() over (windowFunc)).drop($"AMT_TOTL")
			println(s"[NDO-ETL] Showing sample data for mlrMbrRank")
			mlrMbrRank.show
			mlrMbrRank
	}
}

case class mlrAllprov(tabmlrMbr: String, mlrMbrRankDf: Dataset[Row], warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val mlrMbrDf = spark.sql(s"select * from $warehouseHiveDB.$tabmlrMbr")
			val mlrAllprov = mlrMbrDf.join(mlrMbrRankDf, mlrMbrDf("TIN") === mlrMbrRankDf("TIN") && mlrMbrDf("PLN") === mlrMbrRankDf("PLN") && mlrMbrRankDf("PROD_LINE_DESC") === mlrMbrDf("PROD_LINE_DESC"), "inner").
			select(
					mlrMbrRankDf("RANKING"),
					mlrMbrDf("PLN"),
					mlrMbrDf("PROD"),
					mlrMbrDf("TIME_PERIOD"),
					mlrMbrDf("TIN"),
					mlrMbrDf("CATEGORY"),
					mlrMbrDf("CATEGORY2"),
					mlrMbrDf("PROD_LINE_DESC"),
					$"AMOUNT").groupBy(
							$"RANKING",
							$"PLN",
							$"PROD",
							$"TIME_PERIOD",
							$"TIN",
							$"CATEGORY",
							$"CATEGORY2",
							$"PROD_LINE_DESC",
							$"AMOUNT").agg(sum($"AMOUNT")).alias("AMOUNT").select(
									$"RANKING",
									$"PLN",
									$"PROD",
									$"TIME_PERIOD",
									$"TIN",
									$"CATEGORY",
									$"CATEGORY2",
									$"PROD_LINE_DESC",
									$"AMOUNT",
									lit(0).alias("ADMITS"),
									lit(0).alias("BEDSTAYS"),
									lit(0).alias("DAYS"),
									lit(0).alias("VISITS"),
									lit(0).alias("SERVICES"),
									lit(0).alias("SCRIPTS"))
			mlrAllprov.show
			mlrAllprov
	}
}

case class mlrPrvName(tabMlrAllprov: String, ddimProvTaxIdFil: Dataset[Row], warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {

			import spark.implicits._
			val mlrAllprovDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrAllprov")
			val windowFunc1 = Window.partitionBy("Tax_ID").orderBy($"TAX_ID_TRMNTN_DT".desc, $"TAX_ID_EFCTV_DT".asc, $"EDM_PROV_ACCT_KEY".desc)
			val ddimProvTaxIdRank = ddimProvTaxIdFil.select($"Tax_ID", $"TAX_ID_1099_NM", $"TAX_ID_TRMNTN_DT", $"TAX_ID_EFCTV_DT", $"EDM_PROV_ACCT_KEY")
			.withColumn("RANK", row_number() over (windowFunc1)).
			filter($"RANK".===(1)).drop($"COUNT_ROWS")

			val mlrAllprovSel = mlrAllprovDf.select(mlrAllprovDf("TIN")).distinct
			val mlrJoin = mlrAllprovSel.join(ddimProvTaxIdRank, mlrAllprovSel("TIN") === ddimProvTaxIdRank("TAX_ID"), "left_outer").select(
					mlrAllprovSel("TIN"),
					coalesce(ddimProvTaxIdRank("TAX_ID_1099_NM"), lit("NA")).alias("PRV_NAME"))
			mlrJoin.show
			mlrJoin
	}
}

case class mlrFinPrvName(tabMlrPrvName: String, tabMlrMbrRank: String, warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val mlrPrvNameDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrPrvName")
			val mlrMbrRankDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrMbrRank")
			val mlrFinPrvName = mlrPrvNameDf.join(mlrMbrRankDf, mlrPrvNameDf("TIN") === mlrMbrRankDf("TIN"), "inner").select(
					mlrMbrRankDf("RANKING"),
					mlrMbrRankDf("PLN"),
					mlrMbrRankDf("PROD_LINE_DESC"),
					mlrPrvNameDf("TIN"),
					mlrPrvNameDf("PRV_NAME")).distinct
			mlrFinPrvName.show
			mlrFinPrvName
	}
}

case class mlrRev(factRsttdRvnuFil: Dataset[Row], tabMlrSvcArea: String, tabMlrBenPlan: String, tabMlrHcc: String, priorBeginYyyymm: String, priorEndYyyymm: String,
		currentBeginYyyymm: String, currentEndYyyymm: String, priorPeriod: String, currentPeriod: String, warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val mlrSvcAreaDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrSvcArea")
			val mlrBenPlanDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrBenPlan")
			val mlrHccDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrHcc")

			val factJoin = factRsttdRvnuFil.join(mlrSvcAreaDf, factRsttdRvnuFil("SRVCAREA_CD") === mlrSvcAreaDf("SRVCAREA_CD"), "inner")
			val mlrBenJoin = factJoin.join(mlrBenPlanDf, factRsttdRvnuFil("BNFT_PLAN_CD") === mlrBenPlanDf("BNFT_PLAN_CD"), "inner")
			val mlrHccJoin = mlrBenJoin.join(mlrHccDf, factRsttdRvnuFil("HCC_CD") === mlrHccDf("HCC_CD"), "inner").filter($"ITRTN_CD" === "I02")

			val mlrRev = mlrHccJoin.select(
					mlrSvcAreaDf("HLTH_PLAN_RGN_CD").alias("PLN"),
					(when(factRsttdRvnuFil("ANLYTC_INCRD_YEAR_MNTH_NBR").between(priorBeginYyyymm, priorEndYyyymm), lit(priorPeriod))
							when (factRsttdRvnuFil("ANLYTC_INCRD_YEAR_MNTH_NBR").between(currentBeginYyyymm, currentEndYyyymm), lit(currentPeriod))
							otherwise (lit("XX"))).alias("TIME_PERIOD"),
					$"PROD",
					factRsttdRvnuFil("PCP_PROV_TAX_ID").alias("TIN"),
					$"CATEGORY2".alias("CATEGORY"),
					lit("Revenue").alias("CATEGORY2"),
					mlrBenPlanDf("PROD_LINE_DESC"),
					$"RSTTD_RVNU_AMT").groupBy(
							$"PLN",
							$"TIME_PERIOD",
							$"PROD",
							$"TIN",
							$"CATEGORY",
							$"CATEGORY2",
							$"PROD_LINE_DESC").agg(sum($"RSTTD_RVNU_AMT").alias("AMOUNT")).select(
									$"PLN",
									$"TIME_PERIOD",
									$"PROD",
									$"TIN",
									$"CATEGORY",
									$"CATEGORY2",
									$"PROD_LINE_DESC",
									$"AMOUNT")
			mlrRev.show
			mlrRev
	}
}

case class mlrAllprov2(tabMlrRev: String, tabMlrMbrRank: String, warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val mlrRevDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrRev")
			val mlrMbrRankDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrMbrRank")

			val mlrJoin = mlrRevDf.join(mlrMbrRankDf, mlrRevDf("TIN") === mlrMbrRankDf("TIN") &&
			mlrRevDf("PLN") === mlrMbrRankDf("PLN") &&
			mlrRevDf("PROD_LINE_DESC") === mlrMbrRankDf("PROD_LINE_DESC"), "inner")
			val mlrAllprov2 = mlrJoin.select(
					mlrMbrRankDf("RANKING"),
					mlrRevDf("PLN"),
					mlrRevDf("PROD"),
					mlrRevDf("TIME_PERIOD"),
					mlrRevDf("TIN"),
					mlrRevDf("CATEGORY"),
					mlrRevDf("CATEGORY2"),
					mlrRevDf("PROD_LINE_DESC"),
					$"AMOUNT").groupBy(
							$"RANKING",
							$"PLN",
							$"PROD",
							$"TIME_PERIOD",
							$"TIN",
							$"CATEGORY",
							$"CATEGORY2",
							$"PROD_LINE_DESC",
							$"AMOUNT").agg(sum($"AMOUNT")).alias("AMOUNT").select(
									$"RANKING",
									$"PLN",
									$"PROD",
									$"TIME_PERIOD",
									$"TIN",
									$"CATEGORY",
									$"CATEGORY2",
									$"PROD_LINE_DESC",
									$"AMOUNT",
									lit(0).alias("ADMITS"),
									lit(0).alias("BEDSTAYS"),
									lit(0).alias("DAYS"),
									lit(0).alias("VISITS"),
									lit(0).alias("SERVICES"),
									lit(0).alias("SCRIPTS"))
			mlrAllprov2.show
			mlrAllprov2
	}
}

case class mlrExp(factRsttdExpnsFil: Dataset[Row], tabMlrSvcArea: String, tabMlrBenPlan: String, tabMlrHcc: String, priorBeginYyyymm: String, priorEndYyyymm: String,
		currentBeginYyyymm: String, currentEndYyyymm: String, priorPeriod: String, currentPeriod: String, warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val mlrSvcAreaDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrSvcArea")
			val mlrBenPlanDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrBenPlan")
			val mlrHccDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrHcc")

			val factJoin = factRsttdExpnsFil.join(mlrSvcAreaDf, factRsttdExpnsFil("SRVCAREA_CD") === mlrSvcAreaDf("SRVCAREA_CD"), "inner")
			val join2 = factJoin.join(mlrBenPlanDf, factRsttdExpnsFil("BNFT_PLAN_CD") === mlrBenPlanDf("BNFT_PLAN_CD"), "inner")
			val join3 = join2.join(mlrHccDf, factRsttdExpnsFil("HCC_CD") === mlrHccDf("HCC_CD"), "inner").filter($"ITRTN_CD".===("I02"))
			val mlrExp = join3.select(
					mlrSvcAreaDf("HLTH_PLAN_RGN_CD").alias("PLN"),
					(when(factRsttdExpnsFil("ANLYTC_INCRD_YEAR_MNTH_NBR").between(priorBeginYyyymm, priorEndYyyymm), lit(priorPeriod))
							when (factRsttdExpnsFil("ANLYTC_INCRD_YEAR_MNTH_NBR").between(currentBeginYyyymm, currentEndYyyymm), lit(currentPeriod))
							otherwise (lit("XX"))).alias("TIME_PERIOD"),
					$"PROD",
					factRsttdExpnsFil("PCP_PROV_TAX_ID").alias("TIN"),
					$"CATEGORY".alias("CATEGORY"),
					lit("Medical").alias("CATEGORY2"),
					mlrBenPlanDf("PROD_LINE_DESC"),
					$"RSTTD_PAID_AMT",
					$"RSTTD_ADMT_CNT",
					$"RSTTD_DAY_CNT",
					$"RSTTD_VST_CNT",
					$"RSTTD_SRVC_CNT",
					$"RSTTD_SCRPT_CNT").groupBy(
							$"PLN",
							$"TIME_PERIOD",
							$"PROD",
							$"TIN",
							$"CATEGORY",
							$"CATEGORY2",
							$"PROD_LINE_DESC").agg(
									sum($"RSTTD_PAID_AMT").alias("AMOUNT"),
									sum($"RSTTD_ADMT_CNT").alias("ADMITS"),
									sum($"RSTTD_ADMT_CNT").alias("BEDSTAYS"),
									sum($"RSTTD_DAY_CNT").alias("DAYS"),
									sum($"RSTTD_VST_CNT").alias("VISITS"),
									sum($"RSTTD_SRVC_CNT").alias("SERVICES"),
									sum($"RSTTD_SCRPT_CNT").alias("SCRIPTS")).select(
											$"PLN",
											$"TIME_PERIOD",
											$"PROD",
											$"TIN",
											$"CATEGORY",
											$"CATEGORY2",
											$"PROD_LINE_DESC",
											$"AMOUNT",
											$"ADMITS",
											$"BEDSTAYS",
											$"DAYS",
											$"VISITS",
											$"SERVICES",
											$"SCRIPTS")
			mlrExp.show
			mlrExp
	}
}

case class mlrAllprov3(tabMlrExp: String, tabMlrMbrRank: String, warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val mlrExpDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrExp")
			val mlrMbrRankDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrMbrRank")
			val mlrJoin = mlrExpDf.join(mlrMbrRankDf, mlrExpDf("TIN") === mlrMbrRankDf("TIN") &&
			mlrExpDf("PLN") === mlrMbrRankDf("PLN") &&
			mlrExpDf("PROD_LINE_DESC") === mlrMbrRankDf("PROD_LINE_DESC"), "inner")
			val mlrAllprov3 = mlrJoin.select(
					mlrMbrRankDf("RANKING"),
					mlrExpDf("PLN"),
					mlrExpDf("PROD"),
					mlrExpDf("TIME_PERIOD"),
					mlrExpDf("TIN"),
					mlrExpDf("CATEGORY"),
					mlrExpDf("CATEGORY2"),
					mlrExpDf("PROD_LINE_DESC"),
					$"AMOUNT",
					$"ADMITS",
					$"BEDSTAYS",
					$"DAYS",
					$"VISITS",
					$"SERVICES",
					$"SCRIPTS").groupBy(
							$"RANKING",
							$"PLN",
							$"PROD",
							$"TIME_PERIOD",
							$"TIN",
							$"CATEGORY",
							$"CATEGORY2",
							$"PROD_LINE_DESC"
							).agg(
									sum($"AMOUNT").alias("AMOUNT"),
									sum($"ADMITS").alias("ADMITS"),
									sum($"BEDSTAYS").alias("BEDSTAYS"),
									sum($"DAYS").alias("DAYS"),
									sum($"VISITS").alias("VISITS"),
									sum($"SERVICES").alias("SERVICES"),
									sum($"SCRIPTS").alias("SCRIPTS")).select(
											$"RANKING",
											$"PLN",
											$"PROD",
											$"TIME_PERIOD",
											$"TIN",
											$"CATEGORY",
											$"CATEGORY2",
											$"PROD_LINE_DESC",
											$"AMOUNT",
											$"ADMITS",
											$"BEDSTAYS",
											$"DAYS",
											$"VISITS",
											$"SERVICES",
											$"SCRIPTS")
			mlrAllprov3.show
			mlrAllprov3
	}
}

case class apiMlrDtl(tabMlrAllprov: String, tabMlrFinPrvName: String, snapNbr: Long, warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val mlrAllprov3Df = spark.sql(s"select * from $warehouseHiveDB.$tabMlrAllprov")
			val mlrFinPrvNameDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrFinPrvName")
			val mlrJoin = mlrAllprov3Df.join(mlrFinPrvNameDf, mlrAllprov3Df("TIN") === mlrFinPrvNameDf("TIN") &&
			mlrAllprov3Df("RANKING") === mlrFinPrvNameDf("RANKING") &&
			mlrAllprov3Df("PLN") === mlrFinPrvNameDf("PLN") &&
			mlrAllprov3Df("PROD_LINE_DESC") === mlrFinPrvNameDf("PROD_LINE_DESC"), "inner")
			val apiMlrDtl = mlrJoin.select(
					lit(snapNbr).alias("SNAP_YEAR_MNTH_NBR"),
					mlrAllprov3Df("TIN").alias("PROV_TAX_ID"),
					mlrAllprov3Df("PLN").alias("PLAN_CD"),
					mlrAllprov3Df("RANKING").alias("PROV_COST_RANKG_NBR"),
					mlrAllprov3Df("PROD").alias("PROD_DESC"),
					mlrAllprov3Df("TIME_PERIOD").alias("MLR_DTL_TM_PRD_CD"),
					$"PRV_NAME".alias("PROV_NM"),
					$"CATEGORY".alias("MLR_HCC_CTGRY_NM"),
					$"CATEGORY2".alias("MLR_RLUP_CTGRY_NM"),
					(when($"AMOUNT".isNull, lit(0)) otherwise($"AMOUNT")).alias("MLR_AMT"),
					(when($"ADMITS".isNull, lit(0)) otherwise($"ADMITS")).alias("ADMT_CNT"),
					(when($"DAYS".isNull, lit(0)) otherwise($"DAYS")).alias("DAY_CNT"),
					(when($"VISITS".isNull, lit(0)) otherwise($"VISITS")).alias("VST_CNT"),
					(when($"SERVICES".isNull, lit(0)) otherwise($"SERVICES")).alias("SRVC_CNT"),
					(when($"SCRIPTS".isNull, lit(0)) otherwise($"SCRIPTS")).alias("RX_CNT"),
					mlrAllprov3Df("PROD_LINE_DESC")).distinct
			apiMlrDtl.show
			apiMlrDtl
	}
}
case class mlrMbrEnd(factRsttdMbrshpFil: Dataset[Row], tabMlrSvcArea: String, tabMlrBenPlan: String, tabMlrHcc: String, priorBeginYyyymm: String, priorEndYyyymm: String,
		currentBeginYyyymm: String, currentEndYyyymm: String, priorPeriod: String, currentPeriod: String, warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val mlrSvcAreaDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrSvcArea")
			val mlrBenPlanDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrBenPlan")
			val mlrHccDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrHcc")

			val factJoin = factRsttdMbrshpFil.join(mlrSvcAreaDf, factRsttdMbrshpFil("SRVCAREA_CD") === mlrSvcAreaDf("SRVCAREA_CD"), "inner").
			join(mlrBenPlanDf, factRsttdMbrshpFil("BNFT_PLAN_CD") === mlrBenPlanDf("BNFT_PLAN_CD"), "inner").
			join(mlrHccDf, factRsttdMbrshpFil("HCC_CD") === mlrHccDf("HCC_CD"), "inner")

			val mlrMbrEnd = factJoin.select(
					mlrSvcAreaDf("HLTH_PLAN_RGN_CD").alias("PLN"),
					(when(factRsttdMbrshpFil("ANLYTC_INCRD_YEAR_MNTH_NBR").between(priorBeginYyyymm, priorEndYyyymm), lit(priorPeriod))
							when (factRsttdMbrshpFil("ANLYTC_INCRD_YEAR_MNTH_NBR").between(currentBeginYyyymm, currentEndYyyymm), lit(currentPeriod))
							otherwise (lit("XX"))).alias("TIME_PERIOD"),
					$"PROD",
					factRsttdMbrshpFil("PCP_PROV_TAX_ID").alias("TIN"),
					lit("Panel").alias("CATEGORY"),
					lit("Panel").alias("CATEGORY2"),
					mlrBenPlanDf("PROD_LINE_DESC"),
					$"PRORTD_MBR_MNTH_CNT").groupBy(
							$"PLN",
							$"TIME_PERIOD",
							$"PROD",
							$"TIN",
							$"CATEGORY",
							$"CATEGORY2",
							$"PROD_LINE_DESC").agg(sum($"PRORTD_MBR_MNTH_CNT").alias("AMOUNT")).select(
									$"PLN",
									$"TIME_PERIOD",
									$"PROD",
									$"TIN",
									$"CATEGORY",
									$"CATEGORY2",
									$"PROD_LINE_DESC",
									$"AMOUNT")

			mlrMbrEnd.show
			mlrMbrEnd
	}
}

case class apiMlrDtl2(tabMlrMbrEnd: String, tabMlrMbrRank: String, tabMlrFinPrvName: String, snapNbr: Long, warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val mlrMbrEnd = spark.sql(s"select * from $warehouseHiveDB.$tabMlrMbrEnd")
			val mlrMbrRankDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrMbrRank")
			val mlrFinPrvNameDf = spark.sql(s"select * from $warehouseHiveDB.$tabMlrFinPrvName")

			val mlrJoin = mlrMbrEnd.join(mlrMbrRankDf, mlrMbrEnd("TIN") === mlrMbrRankDf("TIN") &&
			mlrMbrEnd("PLN") === mlrMbrRankDf("PLN") &&
			mlrMbrEnd("PROD_LINE_DESC") === mlrMbrRankDf("PROD_LINE_DESC"), "inner")
			
			val mlrFinJoin = mlrJoin.join(mlrFinPrvNameDf, mlrMbrEnd("TIN") === mlrFinPrvNameDf("TIN") &&
			mlrMbrRankDf("RANKING") === mlrFinPrvNameDf("RANKING") &&
			mlrMbrEnd("PLN") === mlrFinPrvNameDf("PLN") &&
			mlrMbrEnd("PROD_LINE_DESC") === mlrFinPrvNameDf("PROD_LINE_DESC"), "inner")
			val apiMlrDtl2 = mlrFinJoin.select(
					lit(snapNbr).alias("SNAP_YEAR_MNTH_NBR"),
					mlrMbrEnd("TIN").alias("PROV_TAX_ID"),
					mlrMbrEnd("PLN").alias("PLAN_CD"),
					mlrMbrRankDf("RANKING").alias("PROV_COST_RANKG_NBR"),
					mlrMbrEnd("PROD").alias("PROD_DESC"),
					mlrMbrEnd("TIME_PERIOD").alias("MLR_DTL_TM_PRD_CD"),
					$"PRV_NAME".alias("PROV_NM"),
					lit("Panel").alias("MLR_HCC_CTGRY_NM"),
					lit("Panel").alias("MLR_RLUP_CTGRY_NM"),
					(when($"AMOUNT".isNull, lit(0)) otherwise ($"AMOUNT")).alias("MLR_CTGRY_AMT"),
					lit(0).alias("ADMT_CNT"),
					lit(0).alias("DAY_CNT"),
					lit(0).alias("VST_CNT"),
					lit(0).alias("SRVC_CNT"),
					lit(0).alias("RX_CNT"),
					mlrMbrEnd("PROD_LINE_DESC")).distinct
			apiMlrDtl2.show
			apiMlrDtl2
	}
}

case class mlrDataSmry(tabApiMlrDtl: String, warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val apiMlrDtl2Df = spark.sql(s"select * from $warehouseHiveDB.$tabApiMlrDtl")
			val mlrDataSmry = apiMlrDtl2Df.select(
					$"SNAP_YEAR_MNTH_NBR",
					$"PROV_TAX_ID",
					$"PLAN_CD",
					$"PROV_COST_RANKG_NBR",
					$"MLR_DTL_TM_PRD_CD",
					$"PROV_NM",
					$"MLR_RLUP_CTGRY_NM",
					$"PROD_LINE_DESC",
					$"MLR_CTGRY_AMT").groupBy($"SNAP_YEAR_MNTH_NBR", $"PROV_TAX_ID",
							$"PLAN_CD",
							$"PROV_COST_RANKG_NBR",
							$"MLR_DTL_TM_PRD_CD",
							$"PROV_NM",
							$"MLR_RLUP_CTGRY_NM",
							$"PROD_LINE_DESC").agg(sum($"MLR_CTGRY_AMT").alias("TOTL_AMT")).select($"SNAP_YEAR_MNTH_NBR", $"PROV_TAX_ID",
									$"PLAN_CD",
									$"PROV_COST_RANKG_NBR",
									$"MLR_DTL_TM_PRD_CD",
									$"PROV_NM",
									$"MLR_RLUP_CTGRY_NM",
									$"PROD_LINE_DESC",
									$"TOTL_AMT")

			mlrDataSmry.show
			mlrDataSmry
	}
}

case class apiMlrSmry(tabMlrDataSmry: String, warehouseHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
				val mlrDataSmry = spark.sql(s"select * from $warehouseHiveDB.$tabMlrDataSmry")

			val apiMlrSmry = mlrDataSmry.select(
					$"SNAP_YEAR_MNTH_NBR",
					$"PROV_TAX_ID",
					$"PLAN_CD",
					$"PROV_COST_RANKG_NBR",
					$"PROV_NM",
					$"PROD_LINE_DESC",
					$"MLR_DTL_TM_PRD_CD",
					$"MLR_RLUP_CTGRY_NM",
					$"totl_Amt",
					(when($"MLR_DTL_TM_PRD_CD" === "PRIOR_PRD"
					&& $"MLR_RLUP_CTGRY_NM" === "Membership", $"totl_Amt")).alias("Prior_MBR_MNTH_AMT"),
					(when($"MLR_DTL_TM_PRD_CD" === "CURNT_PRD"
					&& $"MLR_RLUP_CTGRY_NM" === "Membership", $"totl_Amt")).alias("Curnt_MBR_MNTH_AMT"),
					(when($"MLR_DTL_TM_PRD_CD" === "PRIOR_PRD"
					&& $"MLR_RLUP_CTGRY_NM" === "Revenue", $"totl_Amt")).alias("PRIOR_RVNU_AMT"),
					(when($"MLR_DTL_TM_PRD_CD" === "CURNT_PRD"
					&& $"MLR_RLUP_CTGRY_NM" === "Revenue", $"totl_Amt")).alias("CURNT_RVNU_AMT"),
					(when($"MLR_DTL_TM_PRD_CD" === "PRIOR_PRD"
					&& $"MLR_RLUP_CTGRY_NM" === "Medical", $"totl_Amt")).alias("PRIOR_MDCL_AMT"),
					(when($"MLR_DTL_TM_PRD_CD" === "CURNT_PRD"
					&& $"MLR_RLUP_CTGRY_NM" === "Medical", $"totl_Amt")).alias("CURNT_MDCL_AMT"),
					(when($"MLR_DTL_TM_PRD_CD" === "PRIOR_PRD"
					&& $"MLR_RLUP_CTGRY_NM" === "Panel", $"totl_Amt")).alias("Prior_PNL_MBR_MNT_AMT"),
					(when($"MLR_DTL_TM_PRD_CD" === "CURNT_PRD"
					&& $"MLR_RLUP_CTGRY_NM" === "Panel", $"totl_Amt")).alias("CURNT_PNL_MBR_MNT_AMT")).groupBy(
							$"SNAP_YEAR_MNTH_NBR",
							$"PROV_TAX_ID",
							$"PLAN_CD",
							$"PROV_COST_RANKG_NBR",
							$"PROV_NM",
							$"PROD_LINE_DESC").agg(
									(max($"Prior_MBR_MNTH_AMT")).as("Prior_MBR_MNTH_AMT"),
									(max($"Curnt_MBR_MNTH_AMT")).as("Curnt_MBR_MNTH_AMT"),
									(max($"PRIOR_RVNU_AMT")).alias("PRIOR_RVNU_AMT"),
									(max($"CURNT_RVNU_AMT")).alias("CURNT_RVNU_AMT"),
									(max($"PRIOR_MDCL_AMT")).as("PRIOR_MDCL_AMT"),
									(max($"CURNT_MDCL_AMT")).as("CURNT_MDCL_AMT"),
									(max($"Prior_PNL_MBR_MNT_AMT")).as("Prior_PNL_MBR_MNT_AMT"),
									(max($"CURNT_PNL_MBR_MNT_AMT")).as("CURNT_PNL_MBR_MNT_AMT")).select($"SNAP_YEAR_MNTH_NBR",
							$"PROV_TAX_ID",
							$"PLAN_CD",
							$"PROV_COST_RANKG_NBR",
							$"PROV_NM",
							$"PROD_LINE_DESC",
							$"Prior_MBR_MNTH_AMT",
							$"Curnt_MBR_MNTH_AMT",
							$"PRIOR_RVNU_AMT",
							$"CURNT_RVNU_AMT",
							$"PRIOR_MDCL_AMT",
							$"CURNT_MDCL_AMT",
							$"Prior_PNL_MBR_MNT_AMT",
							$"CURNT_PNL_MBR_MNT_AMT").
							withColumn("Curnt_MBR_MNTH_AMT",(when($"Curnt_MBR_MNTH_AMT".isNull, 0) otherwise ($"Curnt_MBR_MNTH_AMT"))).
							withColumn("Prior_MBR_MNTH_AMT",(when($"Prior_MBR_MNTH_AMT".isNull, 0) otherwise ($"Prior_MBR_MNTH_AMT"))).
							withColumn("PRIOR_RVNU_AMT",(when($"Prior_MBR_MNTH_AMT"===0,0) otherwise (when($"PRIOR_RVNU_AMT".isNull, 0) otherwise ($"PRIOR_RVNU_AMT"))/$"Prior_MBR_MNTH_AMT")).
							withColumn("CURNT_RVNU_AMT",(when($"Curnt_MBR_MNTH_AMT"===0,0) otherwise (when($"CURNT_RVNU_AMT".isNull, 0) otherwise ($"CURNT_RVNU_AMT"))/$"Curnt_MBR_MNTH_AMT")).
							withColumn("PRIOR_MDCL_AMT",(when($"Prior_MBR_MNTH_AMT"===0,0) otherwise (when($"PRIOR_MDCL_AMT".isNull, 0) otherwise ($"PRIOR_MDCL_AMT"))/$"Prior_MBR_MNTH_AMT")).
							withColumn("CURNT_MDCL_AMT",(when($"Curnt_MBR_MNTH_AMT"===0,0) otherwise (when($"CURNT_MDCL_AMT".isNull, 0) otherwise ($"CURNT_MDCL_AMT"))/$"Curnt_MBR_MNTH_AMT")).
							withColumn("Curnt_MLR_PCT",(when($"CURNT_RVNU_AMT"===0,0) otherwise ((($"CURNT_MDCL_AMT".cast(DecimalType(18,4)))/($"CURNT_RVNU_AMT".cast(DecimalType(18,4))))))).
							withColumn("Prior_MLR_PCT",(when($"PRIOR_RVNU_AMT"===0,0) otherwise ((($"PRIOR_MDCL_AMT".cast(DecimalType(18,4)))/($"PRIOR_RVNU_AMT".cast(DecimalType(18,4))))))).
							withColumn("Prior_PNL_MBR_MNT_AMT",(when($"Prior_PNL_MBR_MNT_AMT".isNull, 0) otherwise ($"Prior_PNL_MBR_MNT_AMT"))).
							withColumn("CURNT_PNL_MBR_MNT_AMT",(when($"CURNT_PNL_MBR_MNT_AMT".isNull, 0) otherwise ($"CURNT_PNL_MBR_MNT_AMT"))).
							select(
									$"SNAP_YEAR_MNTH_NBR",
									$"PROV_TAX_ID",
									$"PLAN_CD",
									$"PROV_COST_RANKG_NBR",
									$"PROV_NM",
									$"Curnt_MBR_MNTH_AMT",
									$"Prior_MBR_MNTH_AMT",
									$"CURNT_RVNU_AMT",
									$"PRIOR_RVNU_AMT",
									$"CURNT_MDCL_AMT",
									$"PRIOR_MDCL_AMT",
									$"Curnt_MLR_PCT",
									$"Prior_MLR_PCT",
									$"CURNT_PNL_MBR_MNT_AMT",
									$"Prior_PNL_MBR_MNT_AMT",
									$"PROD_LINE_DESC")    


			apiMlrSmry.show
			apiMlrSmry
	}
}
