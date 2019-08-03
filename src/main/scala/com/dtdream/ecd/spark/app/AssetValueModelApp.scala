package com.dtdream.ecd.spark.app

import com.dtdream.ecd.spark.utils.SparkContextWrapper
import org.apache.spark.sql.functions.{col, lit, sum}
import org.apache.spark.sql.SaveMode


object AssetValueModelApp extends SparkContextWrapper {
  case class BasicIndRecord(row_cnt: Int, distinct_cnt: Int, null_cnt:Int, row_num:Int)

  import hiveContext.implicits._
  import hiveContext.sql

  def getEntropy(s:Double):Double = {
    s match {
      case Double.NaN => 0
      case 0 => 0
      case _ => s * math.log(s)
    }
  }


  def main(args: Array[String]) {
    val appName = "AssetValueModel"
    val sc = getContext(appName)

    import hiveContext.implicits._
    hiveContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

    if (args.length < 1) {
      System.err.println("Usage: AssetValueModel <statis_date> [project_name]")
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

    implicit val BASE_SCORE_TABLE = "t_dv_table_calc_ind_d"
    implicit val RESULT_TABLE_NAME = "t_dv_table_model_value_d" //评估模型结果

    // A hive context adds support for finding tables in the MetaStore and writing queries
    // using HiveQL. Users who do not have an existing Hive deployment can still create a
    // HiveContext. When not configured by the hive-site.xml, the context automatically
    // creates metastore_db and warehouse in the current directory.
    // val hiveContext = getHiveContext(appName)


    // Create table if not exist
    sql(s"""CREATE TABLE IF NOT EXISTS $RESULT_TABLE_NAME
                    (project_name STRING, table_name STRING, bvi_score DOUBLE, ivi_score DOUBLE, cvi_score DOUBLE)
                    PARTITIONED BY (statis_date STRING)
                    STORED AS PARQUET
      """)

    val tableScoreDF = hiveContext.table(BASE_SCORE_TABLE).filter($"statis_date" === statisDate)
    .alias("base_score")

    val kCnt = 1 / math.log( tableScoreDF.count().toInt )
    val sumNull = tableScoreDF.select($"null_score").map(_.getDouble(0)).reduce(_ + _)
    val sumStab = tableScoreDF.select($"stab_score").map(_.getDouble(0)).reduce(_ + _)
    val sumUniq = tableScoreDF.select($"uniq_score").map(_.getDouble(0)).reduce(_ + _)


    // 计算信息熵
    val entropyNull = tableScoreDF.withColumn("sum_null_score", lit(sumNull)).
      select($"null_score" / $"sum_null_score" as "null_ratio").na.fill("0", Seq("null_ratio")).
      map(row => getEntropy( row.getDouble(0)) ).reduce(_ + _) * kCnt * -1

    val entropyStab = tableScoreDF.withColumn("sum_stab_score", lit(sumNull)).
      select($"stab_score" / $"sum_stab_score" as "stab_ratio").na.fill("0", Seq("stab_ratio")).
      map(row => getEntropy( row.getDouble(0)) ).reduce(_ + _) * kCnt * -1

    val entropyUniq = tableScoreDF.withColumn("sum_uniq_score", lit(sumNull)).
      select($"uniq_score" / $"sum_uniq_score" as "uniq_ratio").na.fill("0", Seq("uniq_ratio")).
      map(row => getEntropy( row.getDouble(0)) ).reduce(_ + _) * kCnt * -1

    // 计算IVI指标权重
    val iviNullWeight = (1 - entropyNull) / ( 3 - (entropyNull + entropyStab + entropyUniq) )
    val iviStabWeight = (1 - entropyStab) / ( 3 - (entropyNull + entropyStab + entropyUniq) )
    val iviUniqWeight = (1 - entropyUniq) / ( 3 - (entropyNull + entropyStab + entropyUniq) )

    // 计算评估模型加权总分
    tableScoreDF.withColumn("ivi_score", $"null_score" * iviNullWeight + $"stab_score" * iviStabWeight +
      $"uniq_score" * iviUniqWeight).
      withColumn("bvi_score", $"null_score" * iviNullWeight * 0.2 + $"stab_score" * iviStabWeight * 0.4 +
      $"uniq_score" * iviUniqWeight * 0.4).
      withColumn("cvi_score", $"null_score" * iviNullWeight + $"stab_score" * iviStabWeight +
        $"uniq_score" * iviUniqWeight).
      select("project_name","table_name","statis_date","bvi_score","ivi_score","cvi_score")
      .repartition(1).registerTempTable("tmp_value_table")


    sql(
      s"""
         INSERT OVERWRITE TABLE $RESULT_TABLE_NAME PARTITION (statis_date)
                         SELECT project_name, table_name, bvi_score, ivi_score, cvi_score, statis_date
                         FROM tmp_value_table
       """.stripMargin)

    sc.stop()
  }
}
// scalastyle:on println

