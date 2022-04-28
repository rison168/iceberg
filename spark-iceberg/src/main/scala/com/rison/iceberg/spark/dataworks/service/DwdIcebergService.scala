package com.rison.iceberg.spark.dataworks.service

import com.rison.iceberg.spark.dataworks.dao.DwdIcebergDao
import org.apache.spark.sql.SparkSession

object DwdIcebergService {
  /**
   * 数据从ods加载到dwd
   * @param spark
   */
  def insertBatchAllDwdTable(spark: SparkSession): Unit = {
    DwdIcebergDao.insertBatchDwdBaseAdData(spark)
    DwdIcebergDao.insertBatchDwdBaseWebsiteData(spark)
    DwdIcebergDao.insertBatchDwdMemberData(spark)
    DwdIcebergDao.insertBatchDwdMemberRegTypeData(spark)
    DwdIcebergDao.insertBatchDwdPCenterMemPayMoneyData(spark)
    DwdIcebergDao.insertBatchDwdVipLevelData(spark)
  }

  def getDwdMember(sparkSession: SparkSession) = {
    val sql =
      """
        |select
        |uid,
        |ad_id ,
        |birthday,
        |email,
        |fullname,
        |iconurl,
        |lastlogin,
        |mailaddr,
        |memberlevel,
        |password,
        |phone,
        |qq,
        |register,
        |regupdatetime,
        |unitname,
        |userip,
        |zipcode,
        |dt
        |from spark_catalog.rison_iceberg_db.dwd_member
        |""".stripMargin
    DwdIcebergDao.queryDwdData(sparkSession, sql)
  }

  def getDwdPcentermempaymoney(sparkSession: SparkSession) = {
    val sql: String =
      """
        |select
        |uid,
        |paymoney,
        |siteid,
        |vip_id,
        |dt,
        |dn
        |from spark_catalog.rison_iceberg_db.dwd_p_center_mem_pay_money
        |""".stripMargin
    DwdIcebergDao.queryDwdData(sparkSession, sql)
  }

  def getDwdVipLevel(sparkSession: SparkSession) = {
    val sql: String =
      """
        |select
        |vip_id,
        |vip_level,
        |start_time as vip_start_time,
        |end_time as vip_end_time,
        |last_modify_time as vip_last_modify_time,
        |max_free as vip_max_free,
        |min_free as vip_min_free,
        |next_level as vip_next_level,
        |operator as vip_operator,
        |dn
        |from spark_catalog.rison_iceberg_db.dwd_vip_level
        |""".stripMargin
    DwdIcebergDao.queryDwdData(sparkSession, sql)
  }

  def getDwdBaseWebsite(sparkSession: SparkSession) = {
    val sql: String =
      """
        |select
        |siteid,
        |sitename,
        |siteurl,
        |delete as site_delete,
        |createtime as site_createtime,
        |creator as site_creator,
        |dn
        |from spark_catalog.rison_iceberg_db.dwd_base_website
        |""".stripMargin
    DwdIcebergDao.queryDwdData(sparkSession, sql)
  }

  def getDwdMemberRegtyp(sparkSession: SparkSession) = {
    val sql: String =
      """
        |select
        |uid,
        |appkey,
        |appregurl,
        |bdp_uuid,
        |createtime as reg_createtime,
        |isranreg,
        |regsource,
        |regsourcename,
        |websiteid,
        |dt
        |from spark_catalog.rison_iceberg_db.dwd_member_reg_type
        |""".stripMargin
    DwdIcebergDao.queryDwdData(sparkSession, sql)
  }

  def getDwdBaseAd(sparkSession: SparkSession) = {
    val sql: String =
      """
        |select adid as ad_id,adname,dn from spark_catalog.rison_iceberg_db.dwd_base_ad
        |""".stripMargin
    DwdIcebergDao.queryDwdData(sparkSession, sql)
  }

}
