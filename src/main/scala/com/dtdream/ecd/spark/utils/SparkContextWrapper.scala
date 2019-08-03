package com.dtdream.ecd.spark.utils

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

trait SparkContextWrapper {
  def getContext(appName:String):SparkContext = {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = SparkContext.getOrCreate(sparkConf)
    sc
  }

  def getHiveContext(appName:String):HiveContext = {
    val sc = getContext(appName)
    val hiveContext = new HiveContext(sc)
    hiveContext
  }

  lazy val hiveContext = {
    getHiveContext("SparkTestApp")
  }

}
