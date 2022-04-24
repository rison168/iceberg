package com.rison.iceberg.spark.dataworks.dao

import com.rison.iceberg.spark.dataworks.service.OdsIcebergService
import org.apache.spark.sql.SparkSession


object OdsHiveDao {
  /**
   * 批插入Hive ods_base_ad表
   *
   * @param spark
   */
  def insertBatchHiveOdsBaseAdData(spark: SparkSession): Unit = {
    OdsIcebergService.queryOdsBaseAdData(spark)
      .write
      .mode("overwrite")
      .insertInto("rison_hive_db.ods_base_ad")

  }

  /**
   * 批插入Hive ods_base_website表
   *
   * @param spark
   */
  def insertBatchHiveOdsBaseWebsiteData(spark: SparkSession): Unit = {
    OdsIcebergService.queryOdsBaseWebsiteData(spark)
      .write
      .mode("overwrite")
      .insertInto("rison_hive_db.ods_base_website")

  }

  /**
   * 批插入Hive ods_member
   *
   * @param spark
   */
  def insertBatchHiveOdsMemberData(spark: SparkSession): Unit = {
    OdsIcebergService.queryOdsMemberData(spark)
      .write
      .mode("overwrite")
      .insertInto("rison_hive_db.ods_member")
  }

  /**
   * 批插入Hive ods_member_reg_type
   *
   * @param spark
   */
  def insertBatchHiveOdsMemberRegTypeData(spark: SparkSession): Unit = {
    OdsIcebergService.queryOdsMemberRegTypeData(spark)
      .write
      .mode("overwrite")
      .insertInto("rison_hive_db.ods_member_reg_type")


  }

  /**
   * 批插入Hive ods_vip_level
   *
   * @param spark
   */
  def insertBatchHiveOdsVipLevelData(spark: SparkSession): Unit = {
    import spark.implicits._
    OdsIcebergService.queryOdsVipLevelData(spark)
      .write
      .mode("overwrite")
      .insertInto("rison_hive_db.ods_vip_level")

  }

  /**
   * 批插入Hive ods_p_center_mem_pay_money
   *
   * @param spark
   */
  def insertBatchHiveOdsPCenterMemPayMoneyData(spark: SparkSession): Unit = {
    OdsIcebergService.queryOdsPCenterMemPayMoneyData(spark)
      .write
      .mode("overwrite")
      .insertInto("rison_hive_db.ods_p_center_mem_pay_money")
  }
}
