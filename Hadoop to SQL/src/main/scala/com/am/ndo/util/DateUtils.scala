package com.am.ndo.util

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

object DateUtils {

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

}