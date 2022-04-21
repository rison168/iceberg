package com.rison.iceberg.spark.dataworks.service

import com.rison.iceberg.spark.dataworks.dao.OdsIcebergDao
import org.apache.spark.sql.{DataFrame, SparkSession}

object OdsIcebergService {
  /**
   * 加载数据到ods_base_ad表
   *
   * @param spark
   * @param path
   */
  def loadOdsBaseAdData(spark: SparkSession, path: String): Unit = {
    OdsIcebergDao.loadOdsBaseAdData(spark, path)
  }

  /**
   * 加载数据到ods_base_website表
   *
   * @param spark
   * @param path
   */
  def loadOdsBaseWebsiteData(spark: SparkSession, path: String): Unit = {
    OdsIcebergDao.loadOdsBaseWebsiteData(spark, path)
  }

  /**
   * 加载数据到ods_member
   *
   * @param spark
   * @param path
   */
  def loadOdsMemberData(spark: SparkSession, path: String): Unit = {
    OdsIcebergDao.loadOdsMemberData(spark, path)
  }

  /**
   * 加载数据到ods_member_reg_type
   *
   * @param spark
   * @param path
   */
  def loadOdsMemberRegTypeData(spark: SparkSession, path: String): Unit = {
    OdsIcebergDao.loadOdsMemberRegTypeData(spark, path)
  }

  /**
   * 加载数据到ods_vip_level
   *
   * @param spark
   * @param path
   */
  def loadOdsVipLevelData(spark: SparkSession, path: String): Unit = {
    OdsIcebergDao.loadOdsVipLevelData(spark, path)
  }

  /**
   * 加载数据到ods_p_center_mem_pay_money
   *
   * @param spark
   * @param path
   */
  def loadOdsPCenterMemPayMoneyData(spark: SparkSession, path: String): Unit = {
    OdsIcebergDao.loadOdsPCenterMemPayMoneyData(spark, path)
  }

  /**
   * 查询ods_base_ad表
   *
   * @param spark
   */
  def queryOdsBaseAdData(spark: SparkSession): DataFrame = {
    OdsIcebergDao.queryOdsBaseAdData(spark)

  }

  /**
   * 查询ods_base_website表
   *
   * @param spark
   */
  def queryOdsBaseWebsiteData(spark: SparkSession): DataFrame = {
    OdsIcebergDao.queryOdsBaseWebsiteData(spark)

  }

  /**
   * 查询ods_member
   *
   * @param spark
   */
  def queryOdsMemberData(spark: SparkSession): DataFrame = {
    OdsIcebergDao.queryOdsMemberData(spark)


  }

  /**
   * 查询ods_member_reg_type
   *
   * @param spark
   */
  def queryOdsMemberRegTypeData(spark: SparkSession): DataFrame = {
    OdsIcebergDao.queryOdsMemberRegTypeData(spark)


  }

  /**
   * 查询ods_vip_level
   *
   * @param spark
   */
  def queryOdsVipLevelData(spark: SparkSession): DataFrame = {
    OdsIcebergDao.queryOdsVipLevelData(spark)


  }

  /**
   * 查询ods_p_center_mem_pay_money
   *
   * @param spark
   */
  def queryOdsPCenterMemPayMoneyData(spark: SparkSession): DataFrame = {
    OdsIcebergDao.queryOdsPCenterMemPayMoneyData(spark)

  }
}
