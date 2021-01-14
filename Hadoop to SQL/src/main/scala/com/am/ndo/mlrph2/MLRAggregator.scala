package com.am.ndo.mlrph2

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

case class mlrSvcArea(ddimSrvcareaFil: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val mlrSvcAreaDf = ddimSrvcareaFil.select(
					ddimSrvcareaFil("SRVCAREA_CD").alias("SRVCAREA_CD"),
					substring(ddimSrvcareaFil("SRVCAREA_ST_SHRT_DESC"), 1, 2).alias("hlth_plan_rgn_cd")).distinct()
			//println(s"[NDO-ETL] Showing sample data for mlrSvcAreaDf")
			//mlrSvcAreaDf.show
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
    //println(s"[NDO-ETL] Showing sample data for mlrBenPlanDf")
    //mlrBenPlanDf.show
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

    //println(s"[NDO-ETL] Showing sample data for ddimHccDf")
    //ddimHccDf.show
			ddimHccDf
	}
}

case class mlrMbr(factRsttdMbrshpFil: Dataset[Row], tabMlrSvcArea: String, tabMlrBenPlan: String, tabMlrHcc: String, ddimAgeMnthDf: Dataset[Row] ,priorBeginYyyymm: String, priorEndYyyymm: String,
		currentBeginYyyymm: String, currentEndYyyymm: String, priorPeriod: String, currentPeriod: String,stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val mlrSvcAreaDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrSvcArea")
			val mlrBenPlanDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrBenPlan")
			val mlrHccDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrHcc")

			val factRsttdJoin = factRsttdMbrshpFil.join(mlrSvcAreaDf, factRsttdMbrshpFil("SRVCAREA_CD") === mlrSvcAreaDf("SRVCAREA_CD"), "inner")
			val mlrBenJoin = factRsttdJoin.join(mlrBenPlanDf, factRsttdMbrshpFil("BNFT_PLAN_CD") === mlrBenPlanDf("BNFT_PLAN_CD"), "inner")
			val mlrHccJoin = mlrBenJoin.join(mlrHccDf, factRsttdMbrshpFil("HCC_CD") === mlrHccDf("HCC_CD"), "inner")
			val ddimJoin = mlrHccJoin.join(ddimAgeMnthDf, ddimAgeMnthDf("SNAP_YEAR_MNTH_NBR") === factRsttdMbrshpFil("SNAP_YEAR_MNTH_NBR") && ddimAgeMnthDf("AGE_MNTH_CD") === factRsttdMbrshpFil("STNDRD_AGE_MNTH_CD"), "inner")
    //println(s"[NDO-ETL] Showing sample data for mlrHccJoin")
    //mlrHccJoin.show
			val mlrMbr = ddimJoin.select(
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
					factRsttdMbrshpFil("PRORTD_MBR_MNTH_CNT"),
					(when($"GNDR_CD"==="F", factRsttdMbrshpFil("PRORTD_MBR_MNTH_CNT")) 
					    otherwise (lit(0))).alias("FML_GNDR_CNT"),
					 (when($"GNDR_CD"==="M", factRsttdMbrshpFil("PRORTD_MBR_MNTH_CNT")) 
					    otherwise (lit(0))).alias("MALE_GNDR_CNT"),
					   ddimAgeMnthDf("age_year_nbr") ).groupBy(
							$"PLN",
							$"TIME_PERIOD",
							$"PROD",
							$"TIN",
							$"CATEGORY",
							$"CATEGORY2",
							$"PROD_LINE_DESC").agg(sum($"PRORTD_MBR_MNTH_CNT").alias("AMOUNT"),
							    sum($"FML_GNDR_CNT").alias("FML_GNDR_CNT"),
							    sum($"MALE_GNDR_CNT").alias("MALE_GNDR_CNT"),
							    //Change the datatype .cast(DecimalType(18,2))
							    ((sum(ddimAgeMnthDf("age_year_nbr") * $"PRORTD_MBR_MNTH_CNT") / (sum($"PRORTD_MBR_MNTH_CNT"))).cast(DecimalType(18,2))).alias("AVG_AGE_NBR")
							    ).select(
									$"PLN",
									$"TIME_PERIOD",
									$"PROD",
									$"TIN",
									$"CATEGORY",
									$"CATEGORY2",
									$"PROD_LINE_DESC",
									$"AMOUNT",
									$"FML_GNDR_CNT",
									$"MALE_GNDR_CNT",
									$"AVG_AGE_NBR")

    //println(s"[NDO-ETL] Showing sample data for mlrMbr")
    //mlrMbr.show

			mlrMbr
	}
}

case class mlrMbrRank(tabmlrMbr: String, currentPeriod: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val mlrMbrDf = spark.sql(s"select * from $stagingHiveDB.$tabmlrMbr")
    //println(s"[NDO-ETL] Showing sample data for mlrMbrDf")
    //mlrMbrDf.show

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
									$"TIN",
									$"PROD_LINE_DESC",
									$"AMT_TOTL")


			val windowFunc = Window.partitionBy("PLN", "PROD_LINE_DESC").orderBy($"AMT_TOTL".desc,$"TIN".asc)
			val mlrMbrRank = mlrMbrSel.select($"PLN", $"TIN", $"PROD_LINE_DESC", $"AMT_TOTL")
			.withColumn("RANKING", row_number() over (windowFunc)).drop($"AMT_TOTL")
    //println(s"[NDO-ETL] Showing sample data for mlrMbrRank")
    //mlrMbrRank.show
			mlrMbrRank
	}
}

case class ndoPhrmcyClmSmry(factPhrmcyClmCmry: Dataset[Row], tabMlrSvcArea: String, tabMlrBenPlan: String, tabMlrHcc: String, priorBeginYyyymm: String, priorEndYyyymm: String,
		currentBeginYyyymm: String, currentEndYyyymm: String, priorPeriod: String, currentPeriod: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val mlrSvcAreaDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrSvcArea")
			val mlrBenPlanDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrBenPlan")
			val mlrHccDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrHcc")

			val factRsttdJoin = factPhrmcyClmCmry.join(mlrSvcAreaDf, factPhrmcyClmCmry("RSTTD_SRVCAREA_CD") === mlrSvcAreaDf("SRVCAREA_CD"), "inner")
			val mlrBenJoin = factRsttdJoin.join(mlrBenPlanDf, factPhrmcyClmCmry("RSTTD_BNFT_PLAN_CD") === mlrBenPlanDf("BNFT_PLAN_CD"), "inner")
			val mlrHccJoin = mlrBenJoin.join(mlrHccDf, factPhrmcyClmCmry("HCC_CD") === mlrHccDf("HCC_CD"), "inner")
			//println(s"[NDO-ETL] Showing sample data for ndoPhrmcyClmSmry")
			//mlrHccJoin.show
			val mlrMbr = mlrHccJoin.select(
			    factPhrmcyClmCmry("SNAP_YEAR_MNTH_NBR").alias("SNAP_YEAR_MNTH_NBR"),
					mlrSvcAreaDf("HLTH_PLAN_RGN_CD"),
					factPhrmcyClmCmry("PCP_PROV_TAX_ID").alias("PCP_PROV_TAX_ID"),
					mlrBenPlanDf("PROD").alias("PROD"),
					(when(factPhrmcyClmCmry("ANLYTC_INCRD_YEAR_MNTH_NBR").between(priorBeginYyyymm, priorEndYyyymm), lit(priorPeriod))
							when (factPhrmcyClmCmry("ANLYTC_INCRD_YEAR_MNTH_NBR").between(currentBeginYyyymm, currentEndYyyymm), lit(currentPeriod))
							otherwise (lit("XX"))).alias("TIME_PERIOD"),
					mlrBenPlanDf("PROD_LINE_DESC"),
					(
						when($"DSPNSD_BRND_GNRC_CD"==="Y", (when($"ALWD_AMT" < 0, lit(-1)) when($"BILLD_CHRG_AMT"===0, lit(0)) otherwise(lit(1))))
						otherwise(lit(0))).as("GNRC_RX_CNT_TEMP"),
					(when($"DSPNSD_BRND_GNRC_CD".isin("M", "N", "O" ,"Y"), (when($"ALWD_AMT" < 0, lit(-1)) when($"BILLD_CHRG_AMT"===0, lit(0)) otherwise(lit(1))) ) otherwise(lit(0))).as("DSPNSD_RX_CNT_TEMP")
					)
		//println("print schema"+ mlrMbr.printSchema() )
		val mlrMbr1=mlrMbr.groupBy(
							$"SNAP_YEAR_MNTH_NBR",
							$"HLTH_PLAN_RGN_CD",
							$"PCP_PROV_TAX_ID",
							$"PROD",
							$"TIME_PERIOD",
							$"PROD_LINE_DESC"
							).agg(sum($"GNRC_RX_CNT_TEMP").as("GNRC_RX_CNT"),sum($"DSPNSD_RX_CNT_TEMP").as("DSPNSD_RX_CNT")).select(
									$"SNAP_YEAR_MNTH_NBR",
									$"HLTH_PLAN_RGN_CD",
							  $"PCP_PROV_TAX_ID",
							  $"PROD",
							  $"TIME_PERIOD",
							  $"PROD_LINE_DESC",
								$"GNRC_RX_CNT",
							  $"DSPNSD_RX_CNT")

    //println(s"[NDO-ETL] Showing sample data for ndoPhrmcyClmSmry")
    //mlrMbr1.show

			mlrMbr1
	}
}


case class mlrAllprov(tabmlrMbr: String, mlrMbrRankDf: Dataset[Row], tabndoPhrmcyClmSmry: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val mlrMbrDf = spark.sql(s"select * from $stagingHiveDB.$tabmlrMbr")
			val ndoPhrmcyClmSmry = spark.sql(s"select * from $stagingHiveDB.$tabndoPhrmcyClmSmry")
			
			val mlrAllprov = mlrMbrDf.join(mlrMbrRankDf, mlrMbrDf("TIN") === mlrMbrRankDf("TIN") && mlrMbrDf("PLN") === mlrMbrRankDf("PLN") && 
			    mlrMbrRankDf("PROD_LINE_DESC") === mlrMbrDf("PROD_LINE_DESC"), "inner").
			join(ndoPhrmcyClmSmry,ndoPhrmcyClmSmry("PCP_PROV_TAX_ID") === mlrMbrDf("TIN") && ndoPhrmcyClmSmry("HLTH_PLAN_RGN_CD") === mlrMbrDf("PLN") && 
			    ndoPhrmcyClmSmry("Prod") === mlrMbrDf("Prod") && ndoPhrmcyClmSmry("Time_Period") === mlrMbrDf("time_period") && ndoPhrmcyClmSmry("PROD_LINE_DESC") === mlrMbrDf("PROD_LINE_DESC"), "left_outer").
			select(
					mlrMbrRankDf("RANKING"),
					mlrMbrDf("PLN"),
					mlrMbrDf("PROD"),
					mlrMbrDf("TIME_PERIOD"),
					mlrMbrDf("TIN"),
					mlrMbrDf("CATEGORY"),
					mlrMbrDf("CATEGORY2"),
					mlrMbrDf("PROD_LINE_DESC"),
					$"AMOUNT",
					mlrMbrDf("FML_GNDR_CNT"),
					mlrMbrDf("MALE_GNDR_CNT"),
					mlrMbrDf("AVG_AGE_NBR"),
					ndoPhrmcyClmSmry("GNRC_RX_CNT"),
					ndoPhrmcyClmSmry("DSPNSD_RX_CNT")
					).groupBy(
							$"RANKING",
							$"PLN",
							$"PROD",
							$"TIME_PERIOD",
							$"TIN",
							$"CATEGORY",
							$"CATEGORY2",
							$"PROD_LINE_DESC"
							).agg(sum($"AMOUNT").alias("AMOUNT"),
							    sum($"FML_GNDR_CNT").alias("FML_GNDR_CNT"),
							    sum($"MALE_GNDR_CNT").alias("MALE_GNDR_CNT"),
							    //Change the datatype
							    (sum($"AVG_AGE_NBR" * $"AMOUNT") / (sum($"AMOUNT"))).cast(DecimalType(18,2)).alias("AVG_AGE_NBR"),
							    sum($"GNRC_RX_CNT").alias("GNRC_RX_CNT"),
							    sum($"DSPNSD_RX_CNT").alias("DSPNSD_RX_CNT")
							    ).select(
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
									lit(0).alias("SCRIPTS"),
									$"FML_GNDR_CNT",
									$"MALE_GNDR_CNT",
									$"AVG_AGE_NBR",
				(when($"GNRC_RX_CNT".isNull, lit(0)) otherwise ($"GNRC_RX_CNT")).alias("GNRC_RX_CNT"),
				(when($"DSPNSD_RX_CNT".isNull, lit(0)) otherwise ($"DSPNSD_RX_CNT")).alias("DSPNSD_RX_CNT")
									)
									//withColumn("GNRC_RX_CNT",(when($"GNRC_RX_CNT".isNull, lit(0)) otherwise ($"GNRC_RX_CNT"))).
									//withColumn("DSPNSD_RX_CNT",(when($"DSPNSD_RX_CNT".isNull, lit(0)) otherwise ($"DSPNSD_RX_CNT")))
    //println(s"[NDO-ETL] Showing sample data for mlrAllprov")
    //mlrAllprov.show
			mlrAllprov
	}
}

case class mlrPrvName(tabMlrAllprov: String, ddimProvTaxIdFil: Dataset[Row], stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {

			import spark.implicits._
			val mlrAllprovDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrAllprov")
			
			val windowFunc1 = Window.partitionBy("Tax_ID").orderBy($"TAX_ID_TRMNTN_DT".desc, $"TAX_ID_EFCTV_DT".asc, $"EDM_PROV_ACCT_KEY".desc)
			val ddimProvTaxIdRank = ddimProvTaxIdFil.select($"Tax_ID", $"TAX_ID_1099_NM", $"TAX_ID_TRMNTN_DT", $"TAX_ID_EFCTV_DT", $"EDM_PROV_ACCT_KEY")
			.withColumn("RANK", row_number() over (windowFunc1)).
			filter($"RANK".===(1)).select($"Tax_ID", $"TAX_ID_1099_NM")
			
			val mlrAllprovSel = mlrAllprovDf.select(mlrAllprovDf("TIN")).distinct
			val mlrJoin = mlrAllprovSel.join(ddimProvTaxIdRank, mlrAllprovSel("TIN") === ddimProvTaxIdRank("TAX_ID"), "left_outer").select(
					mlrAllprovSel("TIN"),
					coalesce(ddimProvTaxIdRank("TAX_ID_1099_NM"), lit("UNK")).alias("PRV_NAME"))
    //println(s"[NDO-ETL] Showing sample data for mlrPrvName")
    //mlrJoin.show
			mlrJoin
	}
}

case class mlrFinPrvName(tabMlrPrvName: String, tabMlrMbrRank: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val mlrPrvNameDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrPrvName")
			val mlrMbrRankDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrMbrRank")
			val mlrFinPrvName = mlrPrvNameDf.join(mlrMbrRankDf, mlrPrvNameDf("TIN") === mlrMbrRankDf("TIN"), "inner").select(
					mlrMbrRankDf("RANKING"),
					mlrMbrRankDf("PLN"),
					mlrMbrRankDf("PROD_LINE_DESC"),
					mlrPrvNameDf("TIN"),
					mlrPrvNameDf("PRV_NAME")).distinct
    //println(s"[NDO-ETL] Showing sample data for mlrFinPrvName")
    //mlrFinPrvName.show
			mlrFinPrvName
	}
}

case class mlrRev(factRsttdRvnuFil: Dataset[Row], tabMlrSvcArea: String, tabMlrBenPlan: String, tabMlrHcc: String, priorBeginYyyymm: String, priorEndYyyymm: String,
		currentBeginYyyymm: String, currentEndYyyymm: String, priorPeriod: String, currentPeriod: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val mlrSvcAreaDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrSvcArea")
			val mlrBenPlanDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrBenPlan")
			val mlrHccDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrHcc")

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
    //println(s"[NDO-ETL] Showing sample data for mlrRev")
    //mlrRev.show
			mlrRev
	}
}


case class mlrAllprov2(tabMlrRev: String, tabMlrMbrRank: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val mlrRevDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrRev")
			val mlrMbrRankDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrMbrRank")

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
									lit(0).alias("SCRIPTS"),
									lit(0).alias("FML_GNDR_CNT"),
									lit(0).alias("MALE_GNDR_CNT"),
									lit(0).alias("AVG_AGE_NBR"),
									lit(0).alias("GNRC_RX_CNT"),
									lit(0).alias("DSPNSD_RX_CNT")
			)
    //println(s"[NDO-ETL] Showing sample data for mlrAllprov2")
    //mlrAllprov2.show
			mlrAllprov2
	}
}


case class mlrExp(factRsttdExpnsFil: Dataset[Row], tabMlrSvcArea: String, tabMlrBenPlan: String, tabMlrHcc: String, priorBeginYyyymm: String, priorEndYyyymm: String,
		currentBeginYyyymm: String, currentEndYyyymm: String, priorPeriod: String, currentPeriod: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val mlrSvcAreaDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrSvcArea")
			val mlrBenPlanDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrBenPlan")
			val mlrHccDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrHcc")

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
    //println(s"[NDO-ETL] Showing sample data for mlrExp")
    //mlrExp.show
			mlrExp
	}
}

case class mlrAllprov3(tabMlrExp: String, tabMlrMbrRank: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val mlrExpDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrExp")
			val mlrMbrRankDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrMbrRank")
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
											$"SCRIPTS",
				lit(0).alias("FML_GNDR_CNT"),
				lit(0).alias("MALE_GNDR_CNT"),
				lit(0).alias("AVG_AGE_NBR"),
				lit(0).alias("GNRC_RX_CNT"),
				lit(0).alias("DSPNSD_RX_CNT"))
    //println(s"[NDO-ETL] Showing sample data for mlrAllprov3")
    //mlrAllprov3.show
			mlrAllprov3
	}
}

case class mlrAgeMedian(factRsttdMbrshpFil: Dataset[Row], tabMlrSvcArea: String, tabMlrBenPlan: String, tabMlrHcc: String, ddimAgeMnthDf: Dataset[Row] ,priorBeginYyyymm: String, priorEndYyyymm: String,
		currentBeginYyyymm: String, currentEndYyyymm: String, priorPeriod: String, currentPeriod: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
	val mlrSvcAreaDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrSvcArea")
			val mlrBenPlanDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrBenPlan")
			val mlrHccDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrHcc")

			val factRsttdJoin = factRsttdMbrshpFil.join(mlrSvcAreaDf, factRsttdMbrshpFil("SRVCAREA_CD") === mlrSvcAreaDf("SRVCAREA_CD"), "inner")
			val mlrBenJoin = factRsttdJoin.join(mlrBenPlanDf, factRsttdMbrshpFil("BNFT_PLAN_CD") === mlrBenPlanDf("BNFT_PLAN_CD"), "inner")
			val mlrHccJoin = mlrBenJoin.join(mlrHccDf, factRsttdMbrshpFil("HCC_CD") === mlrHccDf("HCC_CD"), "inner")
			val ddimJoin = mlrHccJoin.join(ddimAgeMnthDf, ddimAgeMnthDf("SNAP_YEAR_MNTH_NBR") === factRsttdMbrshpFil("SNAP_YEAR_MNTH_NBR") && ddimAgeMnthDf("AGE_MNTH_CD") === factRsttdMbrshpFil("STNDRD_AGE_MNTH_CD"), "inner")
			//println(s"[NDO-ETL] Showing sample data for mlrHccJoin")
			//mlrHccJoin.show
			val mlrMbr = ddimJoin.select(
					factRsttdMbrshpFil("SNAP_YEAR_MNTH_NBR").alias("SNAP_YEAR_MNTH_NBR"),
					factRsttdMbrshpFil("PCP_PROV_TAX_ID").alias("PCP_PROV_TAX_ID"),
					mlrSvcAreaDf("HLTH_PLAN_RGN_CD").alias("HLTH_PLAN_RGN_CD"),
					mlrBenPlanDf("PROD_LINE_DESC").alias("PROD_LINE_DESC"),
					ddimAgeMnthDf("age_year_nbr").alias("age_year_nbr"),
					factRsttdMbrshpFil("ANLYTC_INCRD_YEAR_MNTH_NBR").alias("ANLYTC_INCRD_YEAR_MNTH_NBR"),
					factRsttdMbrshpFil("PCP_EDM_PROV_ACCT_KEY").alias("PCP_EDM_PROV_ACCT_KEY"),
					factRsttdMbrshpFil("BNFT_PLAN_CD").alias("BNFT_PLAN_CD"),
					factRsttdMbrshpFil("SRVCAREA_CD").alias("SRVCAREA_CD"),
					factRsttdMbrshpFil("EDM_CLNT_GRP_SUBGRP_ACCT_KEY").alias("EDM_CLNT_GRP_SUBGRP_ACCT_KEY"),
					factRsttdMbrshpFil("HCC_CD").alias("HCC_CD"),
					factRsttdMbrshpFil("EXPRNC_COHRT_CD").alias("EXPRNC_COHRT_CD"),
					factRsttdMbrshpFil("GNDR_CD").alias("GNDR_CD"),
					factRsttdMbrshpFil("STNDRD_AGE_MNTH_CD").alias("STNDRD_AGE_MNTH_CD"),
					factRsttdMbrshpFil("PCP_GRP_EDM_PROV_ACCT_KEY").alias("PLPCP_GRP_EDM_PROV_ACCT_KEYN"),
					factRsttdMbrshpFil("PCP_IPA_EDM_PROV_ACCT_KEY").alias("PCP_IPA_EDM_PROV_ACCT_KEY"),
					factRsttdMbrshpFil("PRORTD_MBR_MNTH_CNT").alias("PRORTD_MBR_MNTH_CNT"),
					factRsttdMbrshpFil("AUDT_JOB_CNTRL_ID").alias("AUDT_JOB_CNTRL_ID")
					).distinct

    //println(s"[NDO-ETL] Showing sample data for mlrMbr")
    //mlrMbr.show

			mlrMbr
	}
}
case class apiMlrDtl(tabMlrAllprov: String, tabMlrFinPrvName: String, snapNbr: Long, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			val mlrAllprov3Df = spark.sql(s"select * from $stagingHiveDB.$tabMlrAllprov")
			val mlrFinPrvNameDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrFinPrvName")
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
					mlrAllprov3Df("PROD_LINE_DESC"),
					(when($"FML_GNDR_CNT".isNull, lit(0)) otherwise($"FML_GNDR_CNT")).alias("FML_GNDR_CNT"),
					(when($"MALE_GNDR_CNT".isNull, lit(0)) otherwise($"MALE_GNDR_CNT")).alias("MALE_GNDR_CNT"),
					(when($"AVG_AGE_NBR".isNull, lit(0)) otherwise($"AVG_AGE_NBR")).alias("AVG_AGE_NBR"),
					(when($"GNRC_RX_CNT".isNull, lit(0)) otherwise($"GNRC_RX_CNT")).alias("GNRC_RX_CNT"),
					(when($"DSPNSD_RX_CNT".isNull, lit(0)) otherwise($"DSPNSD_RX_CNT")).alias("DSPNSD_RX_CNT")).distinct
    //println(s"[NDO-ETL] Showing sample data for apiMlrDtl")
    //apiMlrDtl.show
			apiMlrDtl
	}
}
case class mlrMbrEnd(factRsttdMbrshpFil: Dataset[Row], tabMlrSvcArea: String, tabMlrBenPlan: String, tabMlrHcc: String, priorBeginYyyymm: String, priorEndYyyymm: String,
		currentBeginYyyymm: String, currentEndYyyymm: String, priorPeriod: String, currentPeriod: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val mlrSvcAreaDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrSvcArea")
			val mlrBenPlanDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrBenPlan")
			val mlrHccDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrHcc")

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
    //println(s"[NDO-ETL] Showing sample data for mlrMbrEnd")
    //mlrMbrEnd.show
			mlrMbrEnd
	}
}

case class apiMlrDtl2(tabMlrMbrEnd: String, tabMlrMbrRank: String, tabMlrFinPrvName: String, snapNbr: Long, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._

			val mlrMbrEnd = spark.sql(s"select * from $stagingHiveDB.$tabMlrMbrEnd")
			val mlrMbrRankDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrMbrRank")
			val mlrFinPrvNameDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrFinPrvName")

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
					lit("0").alias("ADMT_CNT"),
					lit("0").alias("DAY_CNT"),
					lit("0").alias("VST_CNT"),
					lit("0").alias("SRVC_CNT"),
					lit("0").alias("RX_CNT"),
					mlrMbrEnd("PROD_LINE_DESC"),
					lit(0).alias("FML_GNDR_CNT"),
					lit(0).alias("MALE_GNDR_CNT"),
					lit(0).alias("AVG_AGE_NBR"),
					lit(0).alias("GNRC_RX_CNT"),
					lit(0).alias("DSPNSD_RX_CNT")).distinct
    //println(s"[NDO-ETL] Showing sample data for apiMlrDtl2")
    //apiMlrDtl2.show
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
					$"MLR_CTGRY_AMT",
					$"FML_GNDR_CNT",
					$"MALE_GNDR_CNT",
					$"AVG_AGE_NBR",
					$"GNRC_RX_CNT",
					$"DSPNSD_RX_CNT").groupBy($"SNAP_YEAR_MNTH_NBR", $"PROV_TAX_ID",
							$"PLAN_CD",
							$"PROV_COST_RANKG_NBR",
							$"MLR_DTL_TM_PRD_CD",
							$"PROV_NM",
							$"MLR_RLUP_CTGRY_NM",  
							$"PROD_LINE_DESC").agg(sum($"MLR_CTGRY_AMT").alias("TOTL_AMT"),
							    sum($"FML_GNDR_CNT").alias("FML_GNDR_CNT"),
							    sum($"MALE_GNDR_CNT").alias("MALE_GNDR_CNT"),
							    ((sum($"AVG_AGE_NBR" * $"MLR_CTGRY_AMT") / (sum($"MLR_CTGRY_AMT"))).cast(DecimalType(8,2))).alias("AVG_AGE_NBR"),
							    sum($"GNRC_RX_CNT").alias("GNRC_RX_CNT"),
							    sum($"DSPNSD_RX_CNT").alias("DSPNSD_RX_CNT")
							   ).select($"SNAP_YEAR_MNTH_NBR", $"PROV_TAX_ID",
									$"PLAN_CD",
									$"PROV_COST_RANKG_NBR",
									$"MLR_DTL_TM_PRD_CD",
									$"PROV_NM",
									$"MLR_RLUP_CTGRY_NM",
									$"PROD_LINE_DESC",
									(when($"TOTL_AMT".isNull, lit(0)) otherwise ($"TOTL_AMT")).alias("TOTL_AMT"),
									(when($"FML_GNDR_CNT".isNull, lit(0)) otherwise ($"FML_GNDR_CNT")).alias("FML_GNDR_CNT"),
									(when($"MALE_GNDR_CNT".isNull, lit(0)) otherwise ($"MALE_GNDR_CNT")).alias("MALE_GNDR_CNT"),
									(when($"AVG_AGE_NBR".isNull, lit(0)) otherwise ($"AVG_AGE_NBR")).alias("AVG_AGE_NBR"),
									(when($"GNRC_RX_CNT".isNull, lit(0)) otherwise ($"GNRC_RX_CNT")).alias("GNRC_RX_CNT"),
									(when($"DSPNSD_RX_CNT".isNull, lit(0)) otherwise ($"DSPNSD_RX_CNT")).alias("DSPNSD_RX_CNT")
								)
    //println(s"[NDO-ETL] Showing sample data for mlrDataSmry")
    //mlrDataSmry.show
			mlrDataSmry
	}
}

case class mlrDataSmry2(tabApiMlrDtl: String, warehouseHiveDB: String, tabMlrAgeMedian: String, mlrDataSmryDf : Dataset[Row] , currentPeriod: String,stagingHiveDB : String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
			
			val apiMlrDtl2Df = spark.sql(s"select * from $warehouseHiveDB.$tabApiMlrDtl")
			val ddimAgeMnthDf = spark.sql(s"select * from $stagingHiveDB.$tabMlrAgeMedian")
			
			ddimAgeMnthDf.createOrReplaceTempView("ddimAgeMnth")
		val ddimAgeMntAfterMedian =	spark.sql("select SNAP_YEAR_MNTH_NBR, PCP_PROV_TAX_ID,HLTH_PLAN_RGN_CD,PROD_LINE_DESC,percentile_approx(age_year_nbr, 0.5) as median_age from ddimAgeMnth group by SNAP_YEAR_MNTH_NBR, PCP_PROV_TAX_ID,HLTH_PLAN_RGN_CD,PROD_LINE_DESC")
//			val median = ddimAgeMnthDf.stat.approxQuantile("age_year_nbr",Array(0.5),0.0)
			
			val ddimAgeMnthJoin = ddimAgeMntAfterMedian.select($"SNAP_YEAR_MNTH_NBR",
			    $"PCP_PROV_TAX_ID",
			    $"HLTH_PLAN_RGN_CD",
			    $"PROD_LINE_DESC",
			    $"median_age"
			    )
			    
			val joinMlrSmry = mlrDataSmryDf.join(ddimAgeMnthJoin, ddimAgeMnthJoin("SNAP_YEAR_MNTH_NBR") === mlrDataSmryDf("SNAP_YEAR_MNTH_NBR") &&
			     ddimAgeMnthJoin("PCP_PROV_TAX_ID") === mlrDataSmryDf("PROV_TAX_ID") &&
			      ddimAgeMnthJoin("HLTH_PLAN_RGN_CD") === mlrDataSmryDf("PLAN_CD") &&
			       ddimAgeMnthJoin("PROD_LINE_DESC") === mlrDataSmryDf("PROD_LINE_DESC"),"inner").where(mlrDataSmryDf("MLR_RLUP_CTGRY_NM")==="Membership" && 
			           mlrDataSmryDf("MLR_DTL_TM_PRD_CD")===currentPeriod)
			       
			val mlrDataSmry = joinMlrSmry.select(
					mlrDataSmryDf("SNAP_YEAR_MNTH_NBR"),
					mlrDataSmryDf("PROV_TAX_ID"),
					mlrDataSmryDf("PLAN_CD"),
					mlrDataSmryDf("PROV_COST_RANKG_NBR"),
					mlrDataSmryDf("MLR_DTL_TM_PRD_CD"),
					mlrDataSmryDf("PROV_NM"),
					lit("Median").alias("MLR_RLUP_CTGRY_NM"),
					mlrDataSmryDf("PROD_LINE_DESC"),
					ddimAgeMnthJoin("median_age").alias("TOTL_AMT"),
					lit(0).alias("FML_GNDR_CNT")).distinct()
					.withColumn("MALE_GNDR_CNT", lit(0)).withColumn("AVG_AGE_NBR", lit(0)).withColumn("GNRC_RX_CNT", lit(0)).withColumn("DSPNSD_RX_CNT", lit(0))
//				
    //println(s"[NDO-ETL] Showing sample data for mlrDataSmry2")
    //mlrDataSmry.show
			mlrDataSmry
	}
}

case class apiMlrSmry(tabMlrDataSmry: String, stagingHiveDB: String) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
			import spark.implicits._
				val mlrDataSmry = spark.sql(s"select * from $stagingHiveDB.$tabMlrDataSmry")

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
					&& $"MLR_RLUP_CTGRY_NM" === "Panel", $"totl_Amt")).alias("CURNT_PNL_MBR_MNT_AMT"),
					
					(when($"MLR_DTL_TM_PRD_CD" === "CURNT_PRD"
					&& $"MLR_RLUP_CTGRY_NM" === "Membership", (when($"FML_GNDR_CNT"===0, lit(0)) when($"FML_GNDR_CNT">0 && $"MALE_GNDR_CNT"<=0, lit(100))
					    when($"FML_GNDR_CNT">0 && $"MALE_GNDR_CNT">0, (($"FML_GNDR_CNT".cast(DecimalType(18,4)) / ($"FML_GNDR_CNT" + $"MALE_GNDR_CNT").cast(DecimalType(18,4)))*100).cast(DecimalType(8,2)))))).alias("FML_GNDR_PCT"),
					    
					(when($"MLR_DTL_TM_PRD_CD" === "CURNT_PRD"
					&& $"MLR_RLUP_CTGRY_NM" === "Membership", $"AVG_AGE_NBR")).alias("AVG_AGE_NBR"),
					
					(when($"MLR_DTL_TM_PRD_CD" === "CURNT_PRD"
					&& $"MLR_RLUP_CTGRY_NM" === "Median", $"totl_Amt")).alias("MEDN_AGE"),
					
					(when($"MLR_DTL_TM_PRD_CD" === "PRIOR_PRD"
					&& $"MLR_RLUP_CTGRY_NM" === "Membership", (when($"DSPNSD_RX_CNT"===0, lit(0)) otherwise((($"GNRC_RX_CNT".cast(DecimalType(18,4)) / ($"DSPNSD_RX_CNT").cast(DecimalType(18,4)))*100).cast(DecimalType(8,2)))))).alias("PRIOR_GNRC_PCT"),
					
					(when($"MLR_DTL_TM_PRD_CD" === "CURNT_PRD"
					&& $"MLR_RLUP_CTGRY_NM" === "Membership",  (when($"DSPNSD_RX_CNT"===0, lit(0)) otherwise((($"GNRC_RX_CNT".cast(DecimalType(18,4)) / ($"DSPNSD_RX_CNT").cast(DecimalType(18,4)))*100).cast(DecimalType(8,2)))))).alias("CURNT_GNRC_PCT")
					
					).groupBy(
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
									(max($"CURNT_PNL_MBR_MNT_AMT")).as("CURNT_PNL_MBR_MNT_AMT"),
									(max($"FML_GNDR_PCT")).as("FML_GNDR_PCT"),
									(max($"AVG_AGE_NBR")).as("AVG_AGE_NBR"),
									(max($"MEDN_AGE")).as("MDN_AGE_NBR"),
									(max($"PRIOR_GNRC_PCT")).as("PRIOR_GNRC_PCT"),
									(max($"CURNT_GNRC_PCT")).as("CURNT_GNRC_PCT")).select($"SNAP_YEAR_MNTH_NBR",
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
							$"CURNT_PNL_MBR_MNT_AMT",
							$"FML_GNDR_PCT",
							$"AVG_AGE_NBR",
							$"MDN_AGE_NBR",
							$"PRIOR_GNRC_PCT",
							$"CURNT_GNRC_PCT").
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
							withColumn("FML_GNDR_PCT",(when($"FML_GNDR_PCT".isNull, 0) otherwise ($"FML_GNDR_PCT"))).
							withColumn("AVG_AGE_NBR",(when($"AVG_AGE_NBR".isNull, 0) otherwise ($"AVG_AGE_NBR"))).
							withColumn("MDN_AGE_NBR",(when($"MDN_AGE_NBR".isNull, 0) otherwise ($"MDN_AGE_NBR"))).
							withColumn("PRIOR_GNRC_PCT",(when($"PRIOR_GNRC_PCT".isNull, 0) otherwise ($"PRIOR_GNRC_PCT"))).
							withColumn("CURNT_GNRC_PCT",(when($"CURNT_GNRC_PCT".isNull, 0) otherwise ($"CURNT_GNRC_PCT"))).
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
									$"PROD_LINE_DESC",
									$"FML_GNDR_PCT",
							$"AVG_AGE_NBR",
							$"MDN_AGE_NBR",
							$"PRIOR_GNRC_PCT",
							$"CURNT_GNRC_PCT")

    //println(s"[NDO-ETL] Showing sample data for apiMlrSmry")
    //apiMlrSmry.show
			apiMlrSmry
	}
}
