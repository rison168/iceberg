package com.rison.iceberg.spark.dataworks.controller

import com.rison.iceberg.spark.dataworks.service.{OdsHiveService, OdsIcebergService}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession


object OdsHiveController extends Logging {
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
      //这里设置hive动态分区
      //set hive.exec.dynamic.partition=true;
      // set hive.exec.dynamic.partition.mode=nostrick;
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nostrick")
      //      .setMaster("local[*]")
      .setAppName("hive_ods_app")
    //这里记得要开启hiveSupport
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //初始化hive ods数据,iceberg ods -> hive ods
    logInfo(s"==== hive ods init start... =====")
    OdsHiveService.insertBatchAllHiveOdsTable(sparkSession)
    logInfo(s"==== hive ods init finish =====")

    sparkSession.close()


  }
}

/**
 * /usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.iceberg.spark.dataworks.controller.OdsHiveController \
 * --master yarn \
 * --deploy-mode cluster \
 * --driver-memory 500m \
 * --executor-memory 500m \
 * --executor-cores 1 \
 * --queue default \
 * /root/spark-dir/spark-iceberg-1.0-SNAPSHOT.jar
 * */