/*
* Copyright (c) 2017, Anthem Inc. All rights reserved.
* DO NOT ALTER OR REMOVE THIS FILE HEADER.
*
*/

package com.am.cogx.pi.dao

import java.io.{ PrintWriter, StringWriter }

import collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.util.Bytes
import java.io.PrintWriter
import java.io.StringWriter
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.PairRDDFunctions
import com.am.cogx.pi.dto.CogxUMRecord
import com.am.cogx.pi.dto.cogxUmInfo
import com.am.cogx.pi.dto.cogxUmHistory
import com.am.etl.config.TableConfig
import com.am.etl.dao.ETLStrategy
import com.am.etl.dao.ImportTrait
import com.am.etl.dao.JSONHolder
import com.am.etl.util.Utils
import com.am.cogx.pi.dto.CogxUMRecord
import org.apache.spark.sql.types.StringType


/**
 * @author Yasin Shaik
 * @version 1.0
 *
 * Imports Cogx UM Auth info from Edward to HBase
 *
 */

class CogxUMHbaseLoad extends ETLStrategy with ImportTrait {

  override def importData(tableConfig: TableConfig): (Int, String, String) = {
    val loadLogKey = getLoadLogKey()
    val loadStartDtm = getLoadDateTime()
    val loadLogKeyColumn = "loadLogKey"
    var rowCount: Long = 0

    try {

      val spark = getSparkSession()
      import spark.implicits._

      var cogxSQL = """SELECT
          DISTINCT
          sum(1) over( rows unbounded preceding ) as  ROW_ID,  
          TRIM(R.RFRNC_NBR) AS  RFRNC_NBR,
          TRIM(S.SRVC_LINE_NBR) AS SRVC_LINE_NBR,
          TRIM(R.CLNCL_SOR_CD) AS CLNCL_SOR_CD,
          TRIM(R.MBRSHP_SOR_CD) AS MBRSHP_SOR_CD,
          TRIM(STTS.UM_SRVC_STTS_CD) AS UM_SRVC_STTS_CD,
          TRIM(STTS.SRC_UM_SRVC_STTS_CD) AS SRC_UM_SRVC_STTS_CD,
          TRIM(R.SRC_SBSCRBR_ID) AS SRC_SBSCRBR_ID,
          TRIM(R.SRC_MBR_CD) AS SRC_MBR_CD,
          TRIM(R.PRMRY_DIAG_CD) AS PRMRY_DIAG_CD,
          TRIM(S.RQSTD_PLACE_OF_SRVC_CD) AS RQSTD_PLACE_OF_SRVC_CD,
          TRIM(S.SRC_RQSTD_PLACE_OF_SRVC_CD) AS SRC_RQSTD_PLACE_OF_SRVC_CD,
          TRIM(S.AUTHRZD_PLACE_OF_SRVC_CD) AS AUTHRZD_PLACE_OF_SRVC_CD,
          TRIM(S.SRC_AUTHRZD_PLACE_OF_SRVC_CD) AS SRC_AUTHRZD_PLACE_OF_SRVC_CD,
          TRIM(S.RQSTD_SRVC_FROM_DT) AS RQSTD_SRVC_FROM_DT,
          TRIM(S.AUTHRZD_SRVC_FROM_DT) AS AUTHRZD_SRVC_FROM_DT,
          TRIM(S.RQSTD_SRVC_TO_DT) AS RQSTD_SRVC_TO_DT,
          TRIM(S.AUTHRZD_SRVC_TO_DT) AS AUTHRZD_SRVC_TO_DT,
          TRIM(S.RQSTD_PROC_SRVC_CD) AS RQSTD_PROC_SRVC_CD,
          TRIM(S.AUTHRZD_PROC_SRVC_CD) AS AUTHRZD_PROC_SRVC_CD,
          TRIM(S.RQSTD_QTY) AS RQSTD_QTY,
          TRIM(S.AUTHRZD_QTY) AS AUTHRZD_QTY,
          TRIM(SP.SRC_UM_PROV_ID) AS SRC_UM_PROV_ID,
          TRIM(SP.PROV_ID) AS PROV_ID,
          TRIM(RP.SRC_UM_PROV_ID) AS SRC_UM_PROV_ID_RP,
          TRIM(RP.PROV_ID) AS PROV_ID_RP,
          TRIM(RP.SRC_PROV_FRST_NM) AS SRC_PROV_FRST_NM_RP,
          TRIM(RP.SRC_PROV_LAST_NM) AS SRC_PROV_LAST_NM_RP,
          TRIM(SP.SRC_PROV_FRST_NM) AS SRC_PROV_FRST_NM,
          TRIM(SP.SRC_PROV_LAST_NM) AS SRC_PROV_LAST_NM
          FROM
      T89_ETL_VIEWS_ENT.UM_RQST R
          INNER JOIN
      T89_ETL_VIEWS_ENT.UM_SRVC S
          ON 
          R.RFRNC_NBR = S.RFRNC_NBR  
          AND R.CLNCL_SOR_CD = S.CLNCL_SOR_CD
          LEFT JOIN
      T89_ETL_VIEWS_ENT.UM_RQST_PROV RP
          ON
          R.RFRNC_NBR = RP.RFRNC_NBR 
          AND R.CLNCL_SOR_CD = RP.CLNCL_SOR_CD
          LEFT JOIN
      T89_ETL_VIEWS_ENT.UM_SRVC_PROV SP
          ON
          R.RFRNC_NBR = SP.RFRNC_NBR
          AND S.SRVC_LINE_NBR = SP.SRVC_LINE_NBR 
          AND R.CLNCL_SOR_CD = SP.CLNCL_SOR_CD 
          AND S.CLNCL_SOR_CD = SP.CLNCL_SOR_CD
          LEFT JOIN
      T89_ETL_VIEWS_ENT.UM_SRVC_STTS STTS
          ON
          R.RFRNC_NBR = STTS.RFRNC_NBR
          AND S.SRVC_LINE_NBR = STTS.SRVC_LINE_NBR
          AND R.CLNCL_SOR_CD = STTS.CLNCL_SOR_CD
          AND S.CLNCL_SOR_CD = STTS.CLNCL_SOR_CD
          WHERE R.MBRSHP_SOR_CD in ('808', '815') SAMPLE 100"""
     
      
      val dbTable: String = "(".concat(cogxSQL).concat(") T")

      val dboptions = scala.collection.mutable.Map[String, String]()
      dboptions += ("url" -> tableConfig.dataSourceConfig.dbserverurl)
      dboptions += ("user" -> tableConfig.dataSourceConfig.username)
      dboptions += ("password" -> getCredentialSecret(tableConfig.dataSourceConfig.credentialproviderurl,tableConfig.dataSourceConfig.password))
      dboptions += ("driver" -> tableConfig.dataSourceConfig.jdbcdriver)
      dboptions += ("dbtable" -> dbTable)
      dboptions += ("fetchSize" -> String.valueOf(tableConfig.dataSourceConfig.fetch_size))
      if (tableConfig.num_partns > 0 && null != tableConfig.partn_col && !tableConfig.partn_col.isEmpty() && tableConfig.queries.containsKey("bound_query")) {
        dboptions += ("partitionColumn" -> tableConfig.partn_col)
        val bounds = getQueryBounds(tableConfig)
        dboptions += ("lowerBound" -> bounds._1)
        dboptions += ("upperBound" -> bounds._2)
        dboptions += ("numPartitions" -> String.valueOf(tableConfig.num_partns))
      }
      
      var cogxUMDF = spark.read.format("jdbc").options(dboptions).load()
      println("INFO: COGX SQL => " + cogxSQL)
      
      
       cogxUMDF = cogxUMDF.columns.foldLeft(cogxUMDF) { (df, colName) =>
          df.schema(colName).dataType match {
            case StringType => { println(s"%%%%%%% TRIMMING COLUMN ${colName.toLowerCase()} %%%%%% VALUE '${trim(col(colName))}'"); df.withColumn(colName.toLowerCase, trim(col(colName))); }
            case _ => { println("Column " + colName.toLowerCase() + " is not being trimmed"); df.withColumn(colName.toLowerCase, col(colName)); }
          }
        }

      rowCount = cogxUMDF.count()
      println("INFO: CogX Row Count => " + rowCount)

      val CogxUmHbaseDataSet = cogxUMDF.as[CogxUMRecord]

      var groupCogxUmHbaseDataSet = CogxUmHbaseDataSet.groupByKey { key =>
        ((key.src_sbscrbr_id))
      }
      val groupedUMHbaseDataSet1 = groupCogxUmHbaseDataSet.mapGroups((k, iter) => {
        var cogxUmSet = Set[CogxUMRecord]()
        val cogxUmInfo = iter.map(cogx => new cogxUmInfo(
          new CogxUMRecord(
            cogx.rfrnc_nbr,
            cogx.srvc_line_nbr,
            cogx.clncl_sor_cd,
            cogx.mbrshp_sor_cd,
            cogx.um_srvc_stts_cd,
            cogx.src_um_srvc_stts_cd,
            cogx.src_sbscrbr_id,
            cogx.src_mbr_cd,
            cogx.prmry_diag_cd,
            cogx.rqstd_place_of_srvc_cd,
            cogx.src_rqstd_place_of_srvc_cd,
            cogx.authrzd_place_of_srvc_cd,
            cogx.src_authrzd_place_of_srvc_cd,
            cogx.rqstd_srvc_from_dt,
            cogx.authrzd_srvc_from_dt,
            cogx.rqstd_srvc_to_dt,
            cogx.authrzd_srvc_to_dt,
            cogx.rqstd_proc_srvc_cd,
            cogx.authrzd_proc_srvc_cd,
            cogx.rqstd_qty,
            cogx.authrzd_qty,
            cogx.src_um_prov_id,
            cogx.prov_id,
            cogx.src_um_prov_id_rp,
            cogx.prov_id_rp,
            cogx.src_prov_frst_nm_rp,
            cogx.src_prov_last_nm_rp,
            cogx.src_prov_frst_nm,
            cogx.src_prov_last_nm))).toArray

        for (um <- cogxUmInfo) {
          cogxUmSet += um.cogxUMdata
        }

        val cogxUM = new cogxUmHistory(cogxUmSet.toArray)

        (String.valueOf(k), cogxUM) // getString(0) i.e. hbase_key is the column for the rowkey 
      }).repartition(2000)

      val columnFamily = tableConfig.hbase_table_columnfamily
      val columnName = tableConfig.hbase_table_columnname
      val putRDD = groupedUMHbaseDataSet1.rdd.map {
        case (key, value) => {
          val digest = DigestUtils.md5Hex(String.valueOf(key))
          val rowKey = new StringBuilder(digest.substring(0, 8)).append(key).toString()
          val holder = Utils.asJSONString(value)
          val p = new Put(Bytes.toBytes(rowKey))
          p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(holder))
          (new org.apache.hadoop.hbase.io.ImmutableBytesWritable, p)
        }
      }

      val hbaseTable = tableConfig.hbase_schema.concat(":").concat(tableConfig.hbase_table_name)
      new PairRDDFunctions(putRDD).saveAsNewAPIHadoopDataset(getMapReduceJobConfiguration(hbaseTable))

   //   insertLoadLogTable(tableConfig, loadLogKey, loadStartDtm, getLoadDateTime(), rowCount, "Y", "")
      return (0, tableConfig.hbase_table_name, "")

    } catch {
      case e: Exception => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        val stackTrace = sw.toString
    //    insertLoadLogTable(tableConfig, loadLogKey, loadStartDtm, getLoadDateTime(), rowCount, "N", stackTrace.replace("\n", "\\n"))
        return (1, tableConfig.hbase_table_name, stackTrace)
      }
    }
  }
}



