package com.dtdream.ecd.spark.app

import com.dtdream.ecd.spark.app.AssetValueModelApp.hiveContext
import com.dtdream.ecd.spark.utils.{SparkContextWrapper}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, lit, stddev_pop, mean}


object QualityCalcScoreApp extends SparkContextWrapper {
  case class BasicIndRecord(row_cnt: Int, distinct_cnt: Int, null_cnt:Int, row_num:Int)

  import hiveContext.sql

  def main(args: Array[String]) {
    val appName = "QualityCalcScore"
    val sc = getContext(appName)

    import hiveContext.implicits._
    hiveContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

    if (args.length < 1) {
      System.err.println("Usage: QualityCalcScore <statis_date> [project_name]")
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

    implicit val RESULT_TABLE_NAME = "t_dv_table_calc_ind_d" //基础计算指标存放表名

    // A hive context adds support for finding tables in the MetaStore and writing queries
    // using HiveQL. Users who do not have an existing Hive deployment can still create a
    // HiveContext. When not configured by the hive-site.xml, the context automatically
    // creates metastore_db and warehouse in the current directory.
    // val hiveContext = getHiveContext(appName)

    // Create table if not exist
    sql(s"""CREATE TABLE IF NOT EXISTS $RESULT_TABLE_NAME
                    (project_name STRING, table_name STRING, null_score DOUBLE, uniq_score DOUBLE,
                     stab_score DOUBLE, time_score DOUBLE)
                    PARTITIONED BY (statis_date STRING)
                    STORED AS PARQUET
      """)


    val tableNames = hiveContext.tableNames(projectName)
    val prjNames = List.fill(tableNames.length)(projectName)

    val tableNameDF = tableNames.zip(prjNames).zipWithIndex.map(t => (t._1._1,t._1._2,t._2)).toSeq.
      toDF("table_name","project_name", "row_num").as("names")

    implicit val BASE_IND_TABLE_NAME = "t_dv_table_basic_ind_d" //基础指标存放中间表名
    implicit val BASE_META_TABLE_NAME = "t_dv_hive_table_meta_d" //元数据基础信息表名

    // 关联元数据信息与基础统计指标表
    val tableInfoDF = hiveContext.table(BASE_IND_TABLE_NAME).join(hiveContext.table(BASE_META_TABLE_NAME),
      Seq("project_name","table_name","statis_date")).filter($"statis_date" === statisDate)
      .repartition(1)
    tableInfoDF.registerTempTable("tmp_meta_table")

    // 完整性与唯一性简单计算，实际更新频率计算
    val simpleScoreDF = sql(
      s"""
         |select project_name,table_name,statis_date,freq,
         | null_cnt / ( row_cnt *  cols_cnt) as null_score,
         | distinct_cnt /  row_cnt as uniq_score,
         | abs( last_update_time - unix_timestamp(statis_date,'yyyy-MM-dd')) / 86400 as act_freq
         |from tmp_meta_table
       """.stripMargin)
      .repartition(1)
    simpleScoreDF.registerTempTable("tmp_spark_table")

    simpleScoreDF.write.mode(SaveMode.Overwrite).saveAsTable("t_tmp_simple_score")

    // 计算稳定性分数
    implicit val stabilityStatDays = 30
    val rowCntDF = sql(
      s"""
         |select a.project_name, a.table_name, a.statis_date, a.row_cnt
         | from $BASE_IND_TABLE_NAME a
          where statis_date >= date_format(date_sub(to_date(statis_date) ,30), "yyyy-MM-dd")
       """.stripMargin).toDF.repartition(1)
    // 计算变异系数，sigmoid归一化
    val tmpDF = rowCntDF.groupBy($"project_name",$"table_name").agg(stddev_pop($"row_cnt").alias("stddev"),
                                                           mean($"row_cnt").alias("mean"))
    tmpDF.registerTempTable("tmp_stab_cnt")

    tmpDF.write.mode(SaveMode.Overwrite).saveAsTable("t_tmp_calc_score")


    sql(
      s"""
         |INSERT OVERWRITE TABLE $RESULT_TABLE_NAME PARTITION (statis_date)
         |select a.project_name, a.table_name,
         |       nvl(a.null_score,0) as null_score,
         |       nvl(a.uniq_score,0) as uniq_score,
         |       nvl((1 - (1 / (1 + exp(-b.stddev/b.mean) )))*2, 0) as stab_score,
         |       1 - (case when act_freq <= 1 then 10
         |            when act_freq <= 7 then 20
         |            when act_freq < 31 then 30
         |            when act_freq < 91 then 50
         |            else 80
         |         end - freq) / 100 as time_score,
         |       a.statis_date
         |  from tmp_spark_table a
         |  join tmp_stab_cnt b
         |    on a.project_name = b.project_name
         |   and a.table_name = b.table_name
       """.stripMargin)

    sc.stop()
  }
}
// scalastyle:on println
