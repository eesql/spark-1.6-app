package com.dtdream.ecd.spark

import org.apache.spark.sql.Column
import org.scalatest.FunSpec

class SparkAppSpec extends FunSpec with SparkTestContextWrapper {
  describe("spark task test") {

    hiveContext.setConf("hive.metastore.warehouse.dir", "/tmp/warehouse")

    it("Test SparkContext Creation and Data Process") {
      import hiveContext.sql

      sql("CREATE TABLE IF NOT EXISTS ods_t_corp (key INT, value STRING)")

      val metaDF = sql(s"SHOW TBLPROPERTIES ods_t_corp").toDF()

      /**
      val tableDF = hiveContext.table("ods_t_corp")
      val rowCount = tableDF.count
      val distinctRowCount = tableDF.distinct.count

      val columns:Seq[Column] = tableDF.columns.map(columns => tableDF.col(columns))

      val nullCount = tableDF.columns.map(colName =>
        tableDF.filter(tableDF(colName).isNull || tableDF(colName) === "" || tableDF(colName).isNaN).count()
      ).reduce((a,b) => a + b)

      println("---------------null valus count: " + nullCount)
        **/
    }
  }
}
