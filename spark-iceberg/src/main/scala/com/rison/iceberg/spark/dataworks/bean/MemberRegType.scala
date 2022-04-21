package com.rison.iceberg.spark.dataworks.bean

import java.sql.Timestamp

case class MemberRegType(
                          uid: Int,
                          appkey: String,
                          appregurl: String,
                          bdp_uuid: String,
                          createtime: Timestamp,
                          isranreg: String,
                          regsource: String,
                          regsourcename: String,
                          websiteid: Int,
                          dt: String
                        )
