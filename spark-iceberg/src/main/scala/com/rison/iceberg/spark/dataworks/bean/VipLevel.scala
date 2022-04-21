package com.rison.iceberg.spark.dataworks.bean

import java.sql.Timestamp

case class VipLevel(
                     vip_id: Int,
                     vip_level: String,
                     start_time: Timestamp,
                     end_time: Timestamp,
                     last_modify_time: Timestamp,
                     max_free: String,
                     min_free: String,
                     next_level: String,
                     operator: String,
                     dn: String
                   )
