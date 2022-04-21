package com.rison.iceberg.spark.dataworks.dao

import org.apache.spark.sql.{DataFrame, SparkSession}

object OdsIcebergDao {
  /**
   * 加载数据到ods_base_ad表
   *
   * @param spark
   * @param path
   */
  def loadOdsBaseAdData(spark: SparkSession, path: String): Unit = {
    spark.read.json(path)
      .writeTo("spark_catalog.rison_iceberg_db.ods_base_ad")
      .overwritePartitions()
  }

  /**
   * 加载数据到ods_base_website表
   *
   * @param spark
   * @param path
   */
  def loadOdsBaseWebsiteData(spark: SparkSession, path: String): Unit = {
    spark.read.json(path)
      .writeTo("spark_catalog.rison_iceberg_db.ods_base_website")
      .overwritePartitions()
  }

  /**
   * 加载数据到ods_member
   *
   * @param spark
   * @param path
   */
  def loadOdsMemberData(spark: SparkSession, path: String): Unit = {
    spark.read.json(path)
      .writeTo("spark_catalog.rison_iceberg_db.ods_member")
      .overwritePartitions()
  }

  /**
   * 加载数据到ods_member_reg_type
   *
   * @param spark
   * @param path
   */
  def loadOdsMemberRegTypeData(spark: SparkSession, path: String): Unit = {
    spark.read.json(path)
      .writeTo("spark_catalog.rison_iceberg_db.ods_member_reg_type")
      .overwritePartitions()
  }

  /**
   * 加载数据到ods_vip_level
   *
   * @param spark
   * @param path
   */
  def loadOdsVipLevelData(spark: SparkSession, path: String): Unit = {
    spark.read.json(path)
      .writeTo("spark_catalog.rison_iceberg_db.ods_vip_level")
      .overwritePartitions()
  }

  /**
   * 加载数据到ods_p_center_mem_pay_money
   *
   * @param spark
   * @param path
   */
  def loadOdsPCenterMemPayMoneyData(spark: SparkSession, path: String): Unit = {
    spark.read.json(path)
      .writeTo("spark_catalog.rison_iceberg_db.ods_p_center_mem_pay_money")
      .overwritePartitions()
  }


  /**
   * 查询ods_base_ad表
   *
   * @param spark
   */
  def queryOdsBaseAdData(spark: SparkSession): DataFrame = {
    spark.sql("select * from spark_catalog.rison_iceberg_db.ods_base_ad")

  }

  /**
   * 查询ods_base_website表
   *
   * @param spark
   */
  def queryOdsBaseWebsiteData(spark: SparkSession): DataFrame = {
    spark.sql("select * from spark_catalog.rison_iceberg_db.ods_base_website")

  }

  /**
   * 查询ods_member
   *
   * @param spark
   */
  def queryOdsMemberData(spark: SparkSession): DataFrame = {
    spark.sql("select * from spark_catalog.rison_iceberg_db.ods_member")

  }

  /**
   * 查询ods_member_reg_type
   *
   * @param spark
   */
  def queryOdsMemberRegTypeData(spark: SparkSession): DataFrame = {
    spark.sql("select * from spark_catalog.rison_iceberg_db.ods_member_reg_type")

  }

  /**
   * 查询ods_vip_level
   *
   * @param spark
   */
  def queryOdsVipLevelData(spark: SparkSession): DataFrame = {
    spark.sql("select * from spark_catalog.rison_iceberg_db.ods_vip_level")

  }

  /**
   * 查询ods_p_center_mem_pay_money
   *
   * @param spark
   */
  def queryOdsPCenterMemPayMoneyData(spark: SparkSession): DataFrame = {
    spark.sql("select * from spark_catalog.rison_iceberg_db.ods_p_center_mem_pay_money")

  }


}
