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
 * Encapsulates the details of the ETL DataSources
 * 
 */

class DataSourceConfig {

  @BeanProperty var credentialproviderurl = ""
  @BeanProperty var username = " "
  @BeanProperty var password = " "
  @BeanProperty var dbserverurl = " "
  @BeanProperty var jdbcdriver = " "
  @BeanProperty var sourceType = " "
  @BeanProperty var sourceDesc = " "
  @BeanProperty var fetch_size: Int = 0
  @BeanProperty var cdl_dw_schema: String = " "
  @BeanProperty var cdl_ref_schema: String = " "

  def printObject() {
    println("        credentialproviderurl: " + credentialproviderurl)
    println("        username: " + username)
    println("        password: HIDDEN")
    println("        dbserverurl: " + dbserverurl)
    println("        jdbcdriver:  " + jdbcdriver)
    println("        sourceType: " + sourceType)
    println("        sourceDesc: " + sourceDesc)
    println("        fetch_size: " + fetch_size)
    println("        cdl_dw_schema: " + cdl_dw_schema)
    println("        cdl_ref_schema: " + cdl_ref_schema)
  }

}

