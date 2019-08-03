package com.dtdream.ecd.spark.app

import com.dtdream.ecd.spark.utils.SparkContextWrapper
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._


object OneId4PeopleApp extends SparkContextWrapper {
  import hiveContext.sql


  def main(args: Array[String]) {
    val appName = "OneIdGenerationForPeople"
    val sc = getContext(appName)

    import hiveContext.implicits._
    hiveContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

    if (args.length < 1) {
      System.err.println("Usage: OneIdGenerationForPeople <statis_date> [project_name]")
      System.exit(1)
    }

    val statisDate = args(0)
    val projectName = args.length match {
      case 2 => args(1)
      case 1 => "default"
      case _ => "default"
    }

    if (projectName != "default") {
      sql(s"use $projectName")
    }

    implicit val BASE_DATA_TABLE = "tmp_one_id_ddl"
    implicit val RESULT_TABLE_NAME = "tmp_one_id_people" //输出结果表

    // A hive context adds support for finding tables in the MetaStore and writing queries
    // using HiveQL. Users who do not have an existing Hive deployment can still create a
    // HiveContext. When not configured by the hive-site.xml, the context automatically
    // creates metastore_db and warehouse in the current directory.
    // val hiveContext = getHiveContext(appName)


    // Create one id table if not exist
    sql(s"""CREATE TABLE IF NOT EXISTS $RESULT_TABLE_NAME
                    (
                               jmsfzh STRING COMMENT '居民身份证号',
                               sjhm STRING COMMENT '手机号码',
                               cysjhm STRING COMMENT '曾用手机号码',
                               xm STRING COMMENT '姓名',
                               cyxm STRING COMMENT '曾用姓名',
                               hzhm STRING COMMENT '护照号码',
                               jkkh STRING COMMENT '健康卡号',
                               jkdabh STRING COMMENT '城乡居民健康档案编号',
                               oneid STRING COMMENT '唯一身份id'
                    )
                    PARTITIONED BY (statis_date STRING)
                    STORED AS PARQUET
      """)

    // Combine id using staging Data
    val baseDF = sql(
      s"""
         SELECT jmsfzh,
                sjhm,
                cysjhm,
                xm,
                cyxm,
                hzhm,
                jkkh,
                jkdabh
           FROM $BASE_DATA_TABLE
       """.stripMargin).toDF
    baseDF.registerTempTable("tmp_one_id_staging")

    // Join OneId with existed data
    val existedDF = sql(
      s"""
         SELECT t1.jmsfzh,
                case when t1.sjhm is not null then t1.sjhm
                else t2.sjhm end as sjhm,
                case when t1.cysjhm is not null then t1.cysjhm
                else t2.cysjhm end as cysjhm,
                case when t1.xm is not null then t1.xm
                else t2.xm end as xm,
                case when t1.cyxm is not null then t1.cyxm
                else t2.cyxm end as cyxm,
                case when t1.hzhm is not null then t1.hzhm
                else t2.hzhm end as hzhm,
                case when t1.jkkh is not null then t1.jkkh
                else t2.jkkh end as jkkh,
                case when t1.jkdabh is not null then t1.jkdabh
                else t2.jkdabh end as jkdabh,
                t2.oneid
           FROM tmp_one_id_staging t1
           JOIN $RESULT_TABLE_NAME t2
             ON t1.jmsfzh = t2.jmsfzh
       """.stripMargin
    )

    // Generate OneId for new data, union with existed Data and writing table
    val appendDF = sql(
      s"""
         SELECT t1.jmsfzh,
                t1.sjhm,
                t1.cysjhm,
                t1.xm,
                t1.cyxm,
                t1.hzhm,
                t1.jkkh,
                t1.jkdabh
           FROM tmp_one_id_staging t1
           LEFT JOIN $RESULT_TABLE_NAME t2
             ON t1.jmsfzh = t2.jmsfzh
           WHERE t2.jmsfzh is NULL
       """.stripMargin
    )
    appendDF.withColumn("oneid", monotonicallyIncreasingId).unionAll(existedDF).
      write.mode(SaveMode.Overwrite).saveAsTable(
      RESULT_TABLE_NAME
    )

    sc.stop()
  }
}
// scalastyle:on println

