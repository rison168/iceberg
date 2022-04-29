package com.rison.iceberg.spark.dataworks.controller

import com.rison.iceberg.spark.dataworks.service.DwdIcebergService
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object DwdIcebergController extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.local.type", "hadoop")
      .set("spark.sql.catalog.local.warehouse", "hdfs:///apps/hive/warehouse")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .set("spark.sql.catalog.spark_catalog.type", "hive")
      .set("spark.sql.catalog.spark_catalog.uri", "thrift://tbds-172-16-16-41:9083")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("spark.sql.session.timeZone", "CST")
      .set("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .set("spark.sql.catalog.catalog-name.default-namespace", "default")
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
      //      .setMaster("local[*]")
      .setAppName("ods_app")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //初始化ods数据
    logInfo(s"==== ods -> dwd start... =====")
    DwdIcebergService.insertBatchAllDwdTable(sparkSession)
    logInfo(s"==== ods -> dwd finish ! =====")


    sparkSession.close()
  }
}
