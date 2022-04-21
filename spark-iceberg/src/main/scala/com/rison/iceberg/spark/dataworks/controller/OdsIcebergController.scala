package com.rison.iceberg.spark.dataworks.controller

import com.rison.iceberg.spark.dataworks.service.OdsIcebergService
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession


object OdsIcebergController extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.local.type", "hadoop")
      .set("spark.sql.catalog.local.warehouse", "hdfs://apps/hive/warehouse")
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
    logInfo(s"==== ods init start... =====")
    OdsIcebergService.loadOdsBaseAdData(sparkSession,"hdfs:///rison/ods/ods_base_ad")
    OdsIcebergService.loadOdsBaseWebsiteData(sparkSession,"hdfs:///rison/ods/ods_base_website")
    OdsIcebergService.loadOdsMemberData(sparkSession,"hdfs:///rison/ods/ods_member")
    OdsIcebergService.loadOdsMemberRegTypeData(sparkSession,"hdfs:///rison/ods/ods_member_reg_type")
    OdsIcebergService.loadOdsPCenterMemPayMoneyData(sparkSession,"hdfs:///rison/ods/ods_p_center_mem_pay_money")
    OdsIcebergService.loadOdsVipLevelData(sparkSession,"hdfs:///rison/ods/ods_vip_level")
    logInfo(s"==== ods init finish =====")

    sparkSession.close()


  }
}
/**
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.iceberg.spark.dataworks.controller.OdsIcebergController \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 500m \
    --executor-memory 500m \
    --executor-cores 1 \
    --queue default \
    /root/spark-dir/spark-iceberg-1.0-SNAPSHOT.jar
**/