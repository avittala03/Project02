package com.am.cogx.etl.util

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.joda.time.Minutes

object CogxDateUtils {

  def getLastDayOfTheMonth(date: String): String = {
    val formatter = new SimpleDateFormat("yyyyMMdd")

    //TODO:Try using JODA api
    val dt = formatter.parse(date)
    val calendar = Calendar.getInstance()
    calendar.setTime(dt)

    calendar.add(Calendar.MONTH, 1)
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    calendar.add(Calendar.DATE, -1)

    val lastDay = calendar.getTime()
    formatter.format(lastDay)
  }

  def getCurrentDate(): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = new Date()
    val today = dateFormat.format(date)
    (today)
  }

  def getCurrentDateTime(format: String): String = {
    DateTimeFormat.forPattern(format).print(new DateTime())
  }

  def getCurrentDateTime: String = {
    val format = "yyyy-MM-dd HH:mm:ss"
    DateTimeFormat.forPattern(format).print(new DateTime())
  }
   def getCurrentDateAsString() :String = {
    return java.time.LocalDate.now.toString()    
  }
  
   def addMonthToDate(date:String,interval:Int):String={
    val calendar = Calendar.getInstance()
    val formatter = new SimpleDateFormat("yyyyMMdd");    
    val de = formatter.parse(date)
    calendar.setTime(de)
    calendar.add(Calendar.MONTH,interval)
    formatter.format(calendar.getTime)
  }
   
      def addMonthToDateWithFormat(date:String,format:String,interval:Int):String={
    val calendar = Calendar.getInstance()
    val formatter = new SimpleDateFormat(format);    
    val de = formatter.parse(date)
    calendar.setTime(de)
    calendar.add(Calendar.MONTH,interval)
    formatter.format(calendar.getTime)
  }
      
  def getCurrentDateWithFormat(format: String): String = {
    val dateFormat = new SimpleDateFormat(format)
    val date = new Date()
    val today = dateFormat.format(date)
    (today)
  }
}