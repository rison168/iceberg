package com.rison.iceberg.spark.dataworks.service

import com.rison.iceberg.spark.dataworks.bean.QueryResult
import com.rison.iceberg.spark.dataworks.dao.DwsIcebergDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object AdsIcebergService {
  /**
   * 统计数据，加载数据到ads
   */

  def insertAds(spark: SparkSession, dt: String): Unit = {
    import spark.implicits._
    val sql: String =
      s"""
         |select
         |uid,
         |ad_id,
         |memberlevel,
         |register,
         |appregurl,
         |regsource,
         |regsourcename,
         |adname,
         |siteid,
         |sitename,
         |vip_level,
         |cast(paymoney as decimal(10,4)) as paymoney,
         |dt,
         |dn
         |from spark_catalog.rison_iceberg_db.dws_member
         |whert dt = '$dt'
         |""".stripMargin
    val result: Dataset[QueryResult] = DwsIcebergDao.queryData(spark, sql).as[QueryResult]
    result.cache()

    //统计url统计人数
    result.mapPartitions(
      partition => {
        partition.map(
          item => {
            (item.appregurl + "_" + item.dn + "_" + item.dt, 1)
          }
        )
      }
    ).groupByKey(_._1)
      .mapValues(item => item._2)
      .reduceGroups(_ + _)
      .map(
        item => {
          val keys = item._1.split("_")
          val url = keys(0)
          val dn = keys(1)
          val dt = keys(3)
          (url, item._2, dt, dn)
        }
      )
      .toDF("appregurl", "num", "dt", "dn")
      .writeTo("spark_catalog.rison_iceberg_db.ads_register_appregurlnum")
      .overwritePartitions()

    //统计等级， 支付前三的用户
    import org.apache.spark.sql.functions._
    result.withColumn(
      "rownum",
      row_number().over(Window.partitionBy("memberlevel").orderBy("paymoney"))
    )
      .where("rownum < 4")
      .orderBy(
        "memberlevel",
        "rownum"
      )
      .select(
        "uid",
        "memberlevel",
        "register",
        "appregurl",
        "regsourcename",
        "adname",
        "sitename",
        "vip_level",
        "paymoney",
        "rownum",
        "dt",
        "dn"
      )
      .writeTo("spark_catalog.rison_iceberg_db.ads_register_top3memberpay")
      .overwritePartitions()

  }
}
