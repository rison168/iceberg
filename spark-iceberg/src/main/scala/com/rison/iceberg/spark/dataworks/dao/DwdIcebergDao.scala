package com.rison.iceberg.spark.dataworks.dao

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.rison.iceberg.spark.dataworks.bean.{BaseWebsite, MemberRegType, VipLevel}
import com.rison.iceberg.spark.dataworks.service.OdsIcebergService
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DwdIcebergDao {
  /**
   * 批插入Dwd_base_ad表
   *
   * @param spark
   */
  def insertBatchDwdBaseAdData(spark: SparkSession): Unit = {
    OdsIcebergService.queryOdsBaseAdData(spark)
      .withColumn("adid", col("adid").cast("Int"))
      .writeTo("spark_catalog.rison_iceberg_db.dwd_base_ad").overwritePartitions()

  }

  /**
   * 批插入Dwd_base_website表
   *
   * @param spark
   */
  def insertBatchDwdBaseWebsiteData(spark: SparkSession): Unit = {
    import spark.implicits._
    OdsIcebergService.queryOdsBaseWebsiteData(spark)
      .map(item => {
        val createtime = item.getAs[String]("createtime")
        val str = LocalDate.parse(createtime, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay().
          format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        BaseWebsite(item.getAs[String]("siteid").toInt, item.getAs[String]("sitename"),
          item.getAs[String]("siteurl"), item.getAs[String]("delete").toInt,
          Timestamp.valueOf(str), item.getAs[String]("creator"), item.getAs[String]("dn"))
      }).writeTo("spark_catalog.rison_iceberg_db.dwd_base_website").overwritePartitions()

  }

  /**
   * 批插入Dwd_member
   *
   * @param spark
   */
  def insertBatchDwdMemberData(spark: SparkSession): Unit = {
    OdsIcebergService.queryOdsMemberData(spark)
      .drop("dn")
      .withColumn("uid", col("uid").cast("int"))
      .withColumn("ad_id", col("ad_id").cast("int"))
      .writeTo("spark_catalog.rison_iceberg_db.dwd_member").overwritePartitions()
  }

  /**
   * 批插入Dwd_member_reg_type
   *
   * @param spark
   */
  def insertBatchDwdMemberRegTypeData(spark: SparkSession): Unit = {
    import spark.implicits._
    OdsIcebergService.queryOdsMemberRegTypeData(spark)
      .drop("domain").drop("dn")
      .withColumn("regsourcename", col("regsource"))
      .map(item => {
        val createtime = item.getAs[String]("createtime")
        val str = LocalDate.parse(createtime, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay().
          format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        MemberRegType(item.getAs[String]("uid").toInt, item.getAs[String]("appkey"),
          item.getAs[String]("appregurl"), item.getAs[String]("bdp_uuid"),
          Timestamp.valueOf(str), item.getAs[String]("isranreg"),
          item.getAs[String]("regsource"), item.getAs[String]("regsourcename"),
          item.getAs[String]("websiteid").toInt, item.getAs[String]("dt"))
      }).writeTo("spark_catalog.rison_iceberg_db.dwd_member_reg_type").overwritePartitions()


  }

  /**
   * 批插入Dwd_vip_level
   *
   * @param spark
   */
  def insertBatchDwdVipLevelData(spark: SparkSession): Unit = {
    import spark.implicits._
    OdsIcebergService.queryOdsVipLevelData(spark)
      .drop("discountval")
      .map(item => {
        val startTime = item.getAs[String]("start_time")
        val endTime = item.getAs[String]("end_time")
        val last_modify_time = item.getAs[String]("last_modify_time")
        val startTimeStr = LocalDate.parse(startTime, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay().
          format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        val endTimeStr = LocalDate.parse(endTime, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay().
          format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        val last_modify_timeStr = LocalDate.parse(last_modify_time, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay().
          format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        VipLevel(item.getAs[String]("vip_id").toInt, item.getAs[String]("vip_level"),
          Timestamp.valueOf(startTimeStr), Timestamp.valueOf(endTimeStr), Timestamp.valueOf(last_modify_timeStr),
          item.getAs[String]("max_free"), item.getAs[String]("min_free"),
          item.getAs[String]("next_level"), item.getAs[String]("operator"),
          item.getAs[String]("dn"))
      }).writeTo("spark_catalog.rison_iceberg_db.dwd_vip_level").overwritePartitions()

  }

  /**
   * 批插入Dwd_p_center_mem_pay_money
   *
   * @param spark
   */
  def insertBatchDwdPCenterMemPayMoneyData(spark: SparkSession): Unit = {
    OdsIcebergService.queryOdsPCenterMemPayMoneyData(spark)
      .withColumn("uid", col("uid").cast("int"))
      .withColumn("siteid", col("siteid").cast("int"))
      .withColumn("vip_id", col("vip_id").cast("int"))
      .writeTo("spark_catalog.rison_iceberg_db.dwd_p_center_mem_pay_money").overwritePartitions()
  }

  /**
   * 查询dwd
   * @param spark
   * @param sql
   */
  def queryDwdData(spark: SparkSession, sql: String) = {
    spark.sql(sql)
  }
}
