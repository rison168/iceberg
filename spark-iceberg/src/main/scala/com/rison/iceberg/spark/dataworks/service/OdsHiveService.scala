package com.rison.iceberg.spark.dataworks.service

import com.rison.iceberg.spark.dataworks.dao.OdsHiveDao
import org.apache.spark.sql.SparkSession

object OdsHiveService {
  /**
   * 数据从iceberg ods加载到Hive ods
   * @param spark
   */
  def insertBatchAllHiveOdsTable(spark: SparkSession): Unit = {
    OdsHiveDao.insertBatchHiveOdsBaseAdData(spark)
    OdsHiveDao.insertBatchHiveOdsBaseWebsiteData(spark)
    OdsHiveDao.insertBatchHiveOdsMemberData(spark)
    OdsHiveDao.insertBatchHiveOdsMemberRegTypeData(spark)
    OdsHiveDao.insertBatchHiveOdsPCenterMemPayMoneyData(spark)
    OdsHiveDao.insertBatchHiveOdsVipLevelData(spark)
  }


}
