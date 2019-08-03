package com.dtdream.ecd.spark.utils

import java.text.SimpleDateFormat
import java.util.Date

object DateTimeUtil {

  val DATE_FORMAT = "yyyy-mm-dd"

  def getDateAsString(d: Date): String = {
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    dateFormat.format(d)
  }

  def convertStringToDate(s: String): Date = {
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    dateFormat.parse(s)
  }
}
