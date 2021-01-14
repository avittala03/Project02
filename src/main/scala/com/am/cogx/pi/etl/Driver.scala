/*
 * Copyright (c) 2017, Anthem Inc. All rights reserved.
 * DO NOT ALTER OR REMOVE THIS FILE HEADER.
 *
 */

package com.am.cogx.pi.etl

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import org.yaml.snakeyaml.Yaml
import com.am.etl.config.{DataSourceConfig, TableConfig}
import com.am.etl.dao.ImportTrait

object Driver {

  def main(args: Array[String]) {
    val yaml = new Yaml
    // TODO: Split the data source config to a separate file
    val allConfig = yaml.loadAll(Driver.getClass.getClassLoader.getResourceAsStream("application.conf"))
    var numErrors = 0
    var errorArr = Array[(String, String)]()
    if(args.length == 0)
      println("INFO: No tags provided. Executing all non-historical loads...")

    val allTableConfig: List[TableConfig] = transform(allConfig.asScala, args)
    if(allTableConfig.length == 0)
      println("INFO: No table configs loaded!")
    else {
      println("INFO: Loaded the following " + allTableConfig.length + " table config(s):")
      allTableConfig.foreach(cfg => {
        cfg.printObject()
        println("=" * 50)
      })
    }

    allTableConfig.foreach(tableConfig => {
      val dao:ImportTrait = Class.forName(tableConfig.daoClass).newInstance().asInstanceOf[ImportTrait]
      // check for partition argument. If present, update the source description to reflect that
      if(args.length == 2) {
        tableConfig.partn_val = args(1)
        tableConfig.dataSourceConfig.sourceDesc = tableConfig.dataSourceConfig.sourceDesc + " - partition: " + args(1)
      }
      val result = dao.importData(tableConfig)
      numErrors += result._1
      if(result._1 > 0)
        errorArr = errorArr :+ (result._2, result._3)
      else {println("INFO: Table " + result._2 + " (sourced from " + tableConfig.dataSourceConfig.sourceType + ") loaded successfully!")}
    })

    if(numErrors > 0){
      var errorText = ""
      errorArr.foreach(err => {
        errorText += ("#"*50) + "\n" + "Table " + err._1 + " load failed with the following error:\n" + err._2 + "\n"
      })
      errorText += ("#"*50) + "\n\n"
      throw new Exception("ERROR: " + String.valueOf(numErrors) + " table(s) failed in this load. The errors below were captured in the audit table as well.\n" + errorText)
    }
  }

  def transform(allTables: Iterable[Object], tags: Array[String]): List[TableConfig] = {
    val tableConfigList = ListBuffer[TableConfig]()
    val iterator = allTables.iterator
    while (iterator.hasNext) {
      val tableConfig = iterator.next().asInstanceOf[TableConfig]
      if(checkTags(tableConfig, tags))
        tableConfigList += tableConfig
    }
    tableConfigList.toList
  }

  def checkTags(config: TableConfig, args: Array[String]): Boolean = {
    var foundTag:Boolean = false
    if(args != null && args.length > 0) {
      val splitArr = args(0).split(",")
      var index = 0
      while (index < splitArr.length && !foundTag) {
        // ignore this table if it's a historical load with no partition argument or
        //  if it's not a historical load and a partition argument is present
        if((config.historicalLoad && args.length == 1) || (!config.historicalLoad && args.length == 2))
          foundTag = false
        else { // check for a match on a tag or a table name
          if (config.tags.contains(splitArr(index)) || config.hbase_table_name.equals(splitArr(index))) {
            foundTag = true
          }
        }
        index += 1
      }
      foundTag
    }
    else { // if there are no tags, we only need to check for a false historicalLoad flag
      !config.historicalLoad
    }
  }

}