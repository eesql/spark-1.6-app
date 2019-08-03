package com.dtdream.ecd.spark.app

import com.dtdream.ecd.spark.utils.SparkContextWrapper
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, lit}


object HiveMetaInfoApp extends SparkContextWrapper {
  case class BasicIndRecord(row_cnt: Int, distinct_cnt: Int, null_cnt:Int, row_num:Int)

  import hiveContext.sql

  implicit val _SCAN_TABLES_WILDCARD = "**"


  def main(args: Array[String]) {
    val appName = "HiveMetaInfo"
    val sc = getContext(appName)

    import hiveContext.implicits._
    hiveContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

    if (args.length < 1) {
      System.err.println("Usage: HiveMetaInfo <statis_date> [project_name]")
      System.exit(1)
    }

    val statisDate = args(0)
    val projectName = args.length match {
      case 2 => args(1)
      case 1 => "default"
      case _ => "default"
    }
    implicit val RESULT_TABLE_NAME = "t_dv_hive_table_meta_d" //元数据基础信息表名

    // A hive context adds support for finding tables in the MetaStore and writing queries
    // using HiveQL. Users who do not have an existing Hive deployment can still create a
    // HiveContext. When not configured by the hive-site.xml, the context automatically
    // creates metastore_db and warehouse in the current directory.
    // val hiveContext = getHiveContext(appName)

    if (projectName != "default") {
      sql(s"use $projectName")
    }


    val metaDataDF = sql(s"show table extended like '${_SCAN_TABLES_WILDCARD}'").toDF

    // Create table if not exist
    sql(s"""CREATE TABLE IF NOT EXISTS $RESULT_TABLE_NAME
                    (project_name STRING, table_name STRING, owner STRING, partitioned STRING,
                     cols_cnt INT, toal_num_files INT, total_fize_size BIGINT, last_access_time BIGINT,
                     last_update_time BIGINT, freq INT)
                    PARTITIONED BY (statis_date STRING)
                    STORED AS PARQUET
      """)

    //resultsDF.write.mode(SaveMode.Overwrite).saveAsTable(RESULT_TABLE_NAME)

    val tabCnt = hiveContext.tableNames(projectName).length
    val _propsCnt = 14
    //构造表序号列表，与meta记录形成笛卡尔积
    val tabIdxs = (1 to tabCnt).map(List.fill(_propsCnt)(_)).flatten

    val props = metaDataDF.filter(col("result").!==("")).
      map(Row => Row.getString(0).split(":")).collect().toList

    tabIdxs.zip(props).map(row => (row._1, row._2(0), row._2.length match {case 2 => row._2(1) case _ => ""}))
      .toDF("id", "key","value").registerTempTable("tmp_props")

    sql(
      s"""select "$projectName" as project_name,
        |        "$statisDate" as statis_date,
        |       max(case when key="tableName" then value end) as table_name,
        |       max(case when key="owner" then value end) as owner,
        |       max(case when key="partitioned" then value end) as partitioned,
        |       max(case when key="columns" then length(value)-length(regexp_replace(value,",",""))+1 end) as cols_cnt,
        |       max(case when key="totalNumberFiles" then value end) as toal_num_files,
        |       max(case when key="totalFileSize" then value end) as total_fize_size,
        |       max(case when key="lastAccessTime" then value end) as last_access_time,
        |       max(case when key="lastUpdateTime" then value end) as last_update_time
        |  from tmp_props
        | group by id
        |
      """.stripMargin)
      .toDF.repartition(1).registerTempTable("tmp_spark_table")

    sql(
      s"""
         INSERT OVERWRITE TABLE $RESULT_TABLE_NAME PARTITION (statis_date)
                         SELECT project_name, table_name, owner, partitioned, cols_cnt, toal_num_files,
                                total_fize_size, last_access_time, last_update_time,
                               |case when substr(table_name,-2) == '_d' then 10
                               |            when substr(table_name,-2) == '_w' then 20
                               |            when substr(table_name,-2) == '_m' then 30
                               |            when substr(table_name,-2) == '_m' then 50
                               |            else 80
                               |        end as freq,
         |                      statis_date
                         FROM tmp_spark_table
       """.stripMargin)

    sc.stop()
  }
}
// scalastyle:on println
