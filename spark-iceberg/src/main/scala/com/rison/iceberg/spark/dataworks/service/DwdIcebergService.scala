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

}
