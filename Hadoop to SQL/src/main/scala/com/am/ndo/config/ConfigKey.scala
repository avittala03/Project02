/*
 * Copyright (c) 2017, Anthem Inc. All rights reserved.
 * DO NOT ALTER OR REMOVE THIS FILE HEADER.
 *
 */
package com.am.ndo.config

/**
 * Keys used in -.conf, -.sql files.
 *
 */
object ConfigKey {

	// App/DB config
			val inboundHiveDB: String = "inbound-hive-db"
			val inboundHiveDBNogbd: String = "inbound-hive-db-nogbd"
			val stageHiveDB: String = "stage-hive-db"
			val wareHouseHiveDB: String = "warehouse-hive-db"
			val wareHouseHiveDBPath: String = "warehouse-hive-db-path"
			val hiveWriteDataFormat: String = "hive-write-data-format"
			val auditColumnName: String = "audit-column-name"
			val saveMode: String = "save-mode"
	
			//ABC
			
			val ndoAudit: String = "audit_table"

			// Miscellaneous
			val sourceDBPlaceHolder = "<SOURCE_DB>"
			val warehouseDBPlaceHolder = "<WAREHOUSE_DB>"
			val stagingDBPlaceholder = "<STAGING_DB>"
			val snapNbrPlaceholder = "<SNAP_NBR>"
		
				
			//Authrzn Fact query config
			val authrznfactTargetTable: String = "AUTHRZN_FACT_TARGET_TABLE"
			val authrznfactStgTable: String = "AUTHRZN_FACT_STG_TABLE"
			val authrznfactWorkTable: String = "WORK_AUTHRZN_FACT_TABLE"
			val stgquery: String = "query_authrzn_fact_stg"
			val tgtquery: String = "query_authrzn_fact_tgt"
}
