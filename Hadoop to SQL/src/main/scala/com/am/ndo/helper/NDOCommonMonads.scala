package com.am.ndo.helper

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import grizzled.slf4j.Logging
import scala.collection.mutable.ArrayBuffer


case class WriteManagedTblInsertOverwrite(df: Dataset[Row], hiveDB: String, tableName: String) extends NDOMonad[Unit] with Logging {
  override def run(spark: SparkSession): Unit = {
    println(s"[NDO-ETL] Write started for table $tableName at:" + DateTime.now())
    df.write.mode("overwrite").insertInto(hiveDB + """.""" + tableName)
    println(s"[NDO-ETL] Table created as $hiveDB.$tableName")
  }
}

case class WriteSaveAsTableOverwrite(df: Dataset[Row], hiveDB: String, tableName: String) extends NDOMonad[Unit] with Logging {
  override def run(spark: SparkSession): Unit = {
    println(s"Write started for final  table $hiveDB.$tableName at:" + DateTime.now())
    df.write.mode("overwrite").saveAsTable(hiveDB + """.""" + tableName)
    println(s"[NDO-ETL] Table created as $hiveDB.$tableName")
  }
}

case class WriteExternalTblAppend(df: Dataset[Row], hiveDB: String, tableName: String) extends NDOMonad[Unit] with Logging {
  override def run(spark: SparkSession): Unit = {
    println(s"Write started for appending the data to final  table $hiveDB.$tableName at:" + DateTime.now())
    df.write.mode("append").insertInto(hiveDB + """.""" + tableName)
    println(s"[NDO-ETL] Table loaded as $hiveDB.$tableName")
  }
}

case class WriteExternalTblWithPartition(df: Dataset[Row], hiveDB: String, tableName: String,partitionColumn:String,partitionValue:String) extends NDOMonad[Unit] with Logging {
  override def run(spark: SparkSession): Unit = {
    println(s"[NDO-ETL]Write started for final  table $hiveDB.$tableName at:" + DateTime.now())
    df.withColumn("last_updt_dtm",lit(current_timestamp())).withColumn(partitionColumn, lit(partitionValue)).write.mode("overwrite").insertInto(hiveDB + """.""" + tableName)
    println(s"[NDO-ETL] Table created as $hiveDB.$tableName")
  }
}

case class Unpersist(dfs: Dataset[Row]*) extends NDOMonad[Unit] with Logging {
  override def run(spark: SparkSession): Unit = {
    println(s"[NDO-ETL] Unpersist started")
    dfs.foreach(df => df.unpersist())
    println(s"[NDO-ETL]: Unpersist ended")
  }
}

case class UnionAll(dsArgs: Dataset[Row]*) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
	  var dsArr = ArrayBuffer[Dataset[Row]]()
			println(s"[NDO-ETL] UnionAll started")
			dsArgs.foreach(ds => dsArr+=ds )
			val unionedDs = dsArgs.reduceLeft((a,b) => a.union(b))
			println(s"[NDO-ETL] UnionAll ended")
			unionedDs
	}
}

case class Union(dsArgs: Dataset[Row]*) extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
	  var dsArr = ArrayBuffer[Dataset[Row]]()
			println("Union started")
			dsArgs.foreach(ds => dsArr+=ds )
			val unionedDs = dsArgs.reduceLeft((a,b) => a.union(b).distinct)
			println("Union ended")
			unionedDs
	}
}

case class CsvLoader(path: String, delimiter : String, columnMap: Map[String,String]) extends NDOMonad[Dataset[Row]] with Logging {

  override def run(spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    val csvDs = spark.read.option("header","true").option("delimiter",delimiter).csv(path)
    //TODO: will dynamically select column and assign its aliases
    csvDs
  }
} 

case class keyGenerator(dataframe: Dataset[Row]) extends NDOMonad[Dataset[Row]] with Logging {

  override def run(spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    val cdDimTemp=dataframe.withColumn("KEY_Temp", monotonically_increasing_id())
      val windowSpec=Window.orderBy("KEY_Temp")
      val cdDim=cdDimTemp.withColumn("Key_Column",row_number.over(windowSpec))
    cdDim
  }
} 

case class ProcFreq(df:Dataset[Row],columnName:String)extends NDOMonad[Dataset[Row]] with Logging {
	override def run(spark: SparkSession): Dataset[Row] = {
	  import spark.implicits._
	  	  
	  df.createOrReplaceTempView("parent")
	  
	  val percent=spark.sql(s"select $columnName,count($columnName) as frequency,cast((count($columnName)*100/(select count(*) from parent)) as decimal(4,2)) as percent From parent Group by $columnName")
    percent.persist()
    println(percent.count)
    val P1=percent.as("P1")
	  val P2=percent.as("P2")
    val finalDF=P1.repartition(P1(columnName)).join(P2,col(s"P1.$columnName")>=col(s"P2.$columnName"),"inner").groupBy(P1(columnName),$"P1.FREQUENCY",$"P1.PERCENT").agg(sum($"P2.FREQUENCY"),sum($"P2.PERCENT")).orderBy(P1(columnName))
    
    finalDF
	}
}


