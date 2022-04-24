package com.rison.iceberg.spark.dataworks.controller

import com.rison.iceberg.spark.dataworks.service.OdsHiveService
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession


object TestController extends Logging {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
//    sparkConf.set("spark.sql.adaptive.enabled", "true")
//    sparkConf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
//    sparkConf.set("spark.sql.catalog.local.type", "hadoop")
//    sparkConf.set("spark.sql.catalog.local.warehouse", "hdfs:///apps/hive/warehouse")
//    sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
//    sparkConf.set("spark.sql.catalog.spark_catalog.type", "hive")
//    sparkConf.set("spark.sql.catalog.spark_catalog.uri", "thrift://tbds-172-16-16-41:9083")
//    sparkConf.set("spark.sql.parquet.binaryAsString", "true")
//    sparkConf.set("spark.sql.session.timeZone", "CST")
//    sparkConf.set("spark.sql.warehouse.dir", "hdfs:///apps/hive/warehouse")
//    sparkConf.set("spark.sql.catalog.catalog-name.default-namespace", "default")
//    sparkConf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
//    sparkConf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
//    sparkConf.set("hive.exec.dynamic.partition", "true")
//    sparkConf.set("hive.exec.dynamic.partition.mode", "nostrick")
    sparkConf.setAppName("ods_app")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    sparkSession.sql("select * from rison_hive_db.ods_p_center_mem_pay_money").show(20)

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