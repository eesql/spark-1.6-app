package com.dtdream.ecd.spark.app

import com.dtdream.ecd.spark.utils.SparkContextWrapper
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.SaveMode


object QualityBaseScoreApp extends SparkContextWrapper {
  case class BasicIndRecord(row_cnt: Int, distinct_cnt: Int, null_cnt:Int, row_num:Int)

  import hiveContext.implicits._
  import hiveContext.sql


  def main(args: Array[String]) {
    val appName = "QualityBaseScore"
    val sc = getContext(appName)

    import hiveContext.implicits._
    hiveContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

    if (args.length < 1) {
      System.err.println("Usage: QualityBaseScore <statis_date> [project_name]")
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

    implicit val RESULT_TABLE_NAME = "t_dv_table_basic_ind_d" //基础指标存放中间表名

    // A hive context adds support for finding tables in the MetaStore and writing queries
    // using HiveQL. Users who do not have an existing Hive deployment can still create a
    // HiveContext. When not configured by the hive-site.xml, the context automatically
    // creates metastore_db and warehouse in the current directory.
    // val hiveContext = getHiveContext(appName)


    // Create table if not exist
    sql(s"""CREATE TABLE IF NOT EXISTS $RESULT_TABLE_NAME
                    (project_name STRING, table_name STRING, row_cnt INT, distinct_cnt INT, null_cnt INT)
                    PARTITIONED BY (statis_date STRING)
                    STORED AS PARQUET
      """)


    val tableNames = hiveContext.tableNames(projectName)
    val prjNames = List.fill(tableNames.length)(projectName)

    val tableNameDF = tableNames.zip(prjNames).zipWithIndex.map(t => (t._1._1,t._1._2,t._2)).toSeq.
      toDF("table_name","project_name", "row_num").as("names")

    val dfs = tableNames.map(t => hiveContext.table(t))
    // 基础指标统计，包括行数，唯一值行数，空值数
    val countIndDF = dfs.zipWithIndex.map((tableDF) =>
      BasicIndRecord(tableDF._1.count.toInt, tableDF._1.distinct.count.toInt,
        // Null values count for each column
        tableDF._1.columns.map(colName =>
          tableDF._1.filter(tableDF._1(colName).isNull || tableDF._1(colName) === "").count()
        ).reduce((a,b) => a + b).toInt,
        tableDF._2
      )
    ).toSeq.toDF.as("indicators")

    val resultsDF = tableNameDF.join(countIndDF, col("names.row_num") === col("indicators.row_num")).
      withColumn("statis_date", lit(statisDate))

    resultsDF.select("project_name","table_name","row_cnt","distinct_cnt","null_cnt", "statis_date").
      repartition(1).
      registerTempTable("tmp_spark_table")

    //resultsDF.write.mode(SaveMode.Overwrite).saveAsTable(RESULT_TABLE_NAME)

    sql(
      s"""
         INSERT OVERWRITE TABLE $RESULT_TABLE_NAME PARTITION (statis_date)
                         SELECT project_name, table_name, row_cnt, distinct_cnt, null_cnt, statis_date
                         FROM tmp_spark_table
       """.stripMargin)


    sc.stop()
  }
}
// scalastyle:on println
