/*
 * Copyright (c) 2017, Anthem Inc. All rights reserved.
 * DO NOT ALTER OR REMOVE THIS FILE HEADER.
 *
 */

package com.am.etl.dao

import com.am.etl.config.TableConfig

/**
 * @author T Murali
 * @version 1.0
 *
 * Generic interface to support ETL imports
 * 
 */

@SerialVersionUID(2017L)
trait ImportTrait extends Serializable {

  /**
    * A generic method for importing Data
    * 
    */
  def importData(dataobj:TableConfig) : (Int, String, String) {}

}