/*
 * Copyright (c) 2017, Anthem Inc. All rights reserved.
 * DO NOT ALTER OR REMOVE THIS FILE HEADER.
 *
 */

package com.am.etl.config

import scala.beans.BeanProperty
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter

/**
 * @author T Murali
 * @version 1.0
 *
 * Encapsulates the details of an HBase table configuration
 * 
 */

class TableConfig {
  
  @BeanProperty var hbase_schema:String = " "
  @BeanProperty var hbase_table_name:String = " "
  @BeanProperty var hbase_table_columnfamily:String = " "
  @BeanProperty var hbase_table_columnname:String = " "
  @BeanProperty var hive_inbound_schema:String = " "
  @BeanProperty var hive_work_schema:String = " "
  @BeanProperty var hive_work_path:String = " "
  @BeanProperty var hive_warehouse_schema:String = " "
  @BeanProperty var hive_warehouse_path:String = " "
  @BeanProperty var partn_col:String = " "  
  @BeanProperty var num_partns:Int = 0
  @BeanProperty var daoClass:String = " "
  @BeanProperty var auditTable = " "
  @BeanProperty var historicalLoad:Boolean = false
  @BeanProperty var tags:java.util.List[String] = new java.util.ArrayList[String]()
  @BeanProperty var queries:java.util.Map[String, String] = new java.util.HashMap[String,String]()
  @BeanProperty var dataSourceConfig:DataSourceConfig = new DataSourceConfig
  @BeanProperty var partn_val:String = " "

  def printObject() {
    println("    hbase_schema: " + hbase_schema)
    println("    hbase_table: " + hbase_table_name)
    println("    hbase_table_columnfamily: " + hbase_table_columnfamily)
    println("    hbase_table_columnname: " + hbase_table_columnname)
    println("    hive_inbound_schema: " + hive_inbound_schema)
    println("    hive_work_schema: " + hive_work_schema)
    println("    hive_work_path: " + hive_work_path)
    println("    hive_warehouse_schema: " + hive_warehouse_schema)
    println("    hive_warehouse_path: " + hive_warehouse_path)
    println("    partn_col: " + partn_col)
    println("    num_partns: " + num_partns)
    println("    daoClass: " + daoClass)
    println("    auditTable: " + auditTable)
    println("    historicalLoad: " + historicalLoad)
    println("    partn_val: " + partn_val)
    println("    tags: " + tags)
    println("    queries: ")
    if(null != queries && !queries.isEmpty())
      queries.asScala foreach (x => println ("        " + x._1 + ": " + x._2))
    println("    dataSourceConfig: ")
    println(dataSourceConfig.printObject())
  }

}

