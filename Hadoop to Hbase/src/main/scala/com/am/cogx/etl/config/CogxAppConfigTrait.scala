/*
 * Copyright (c) 2017, Anthem Inc. All rights reserved.
 * DO NOT ALTER OR REMOVE THIS FILE HEADER.
 *
*/
package com.am.cogx.etl.config

import org.apache.hadoop.fs.FileSystem

trait CogxAppConfigTrait extends Serializable {
  val hdfs: FileSystem
  var hiveWriteFileFormat: String = "parquet"
}