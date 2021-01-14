/*
 * Copyright (c) 2017, Anthem Inc. All rights reserved.
 * DO NOT ALTER OR REMOVE THIS FILE HEADER.
 *
*/
package com.am.cogx.etl.config

/**
 * Keys used in -.conf, -.sql files.
 *
 */
object CogxConfigKey {

  // App/DB config
  val inboundHiveDB: String = "inbound-hive-db"
	val stageHiveDB: String = "stage-hive-db"
	val wareHouseHiveDB: String = "warehouse-hive-db"
	
  val hiveWriteDataFormat: String = "hive-write-data-format"
  val auditColumnName: String = "audit-column-name"
  val saveMode: String = "save-mode"

  // MTCLM Query config
  val mtclmTargetTable: String = "mtclm_target_table"
  val mtclmcoaTargetTable: String = "mtclm_coa_target_table"
  
  val clmQuery: String = "clm_query"
  val clmLineQuery: String = "clm_line_query"
  val clmPaidQuery: String = "clm_paid_query"
  val fcltyClmQuery: String = "fclty_clm_query"
  val clmLineCoaQuery: String = "clm_line_coa_query"
  
  // Spend Query config
  
  
   // TargetTin Query config

  
  // Miscellaneous
  val sourceDBPlaceHolder = "<SOURCE_DB>"
  val stagingDBPlaceHolder = "<STAGING_DB>"
  val warehouseDBPlaceHolder = "<WAREHOUSE_DB>"
  val lastUpdtTm = "<LAST_UPDT_DT>"

}
