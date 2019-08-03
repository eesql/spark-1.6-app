package com.dtdream.ecd.spark

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

trait SparkTestContextWrapper {
  lazy val sparkConf = new SparkConf().setAppName("SparkTestApp").setMaster("local[2]")

  lazy val sc = SparkContext.getOrCreate(sparkConf)
  lazy val hiveContext = new HiveContext(sc)
}
