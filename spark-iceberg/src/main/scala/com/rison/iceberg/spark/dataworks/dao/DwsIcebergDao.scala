package com.rison.iceberg.spark.dataworks.dao

import org.apache.spark.sql.SparkSession

object DwsIcebergDao {
  /**
   * 查询宽表数据
   * @param spark
   * @param querySql
   * @return
   */
  def queryData(spark: SparkSession, querySql: String) = {
    spark.sql(querySql)
  }
}
