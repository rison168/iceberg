package com.rison.iceberg.spark.dataworks.bean

case class QueryResult(
                        uid: Int,
                        ad_id: Int,
                        memberlevel: String,
                        register: String,
                        appregurl: String,
                        regsource: String,
                        regsourcename: String,
                        adname: String,
                        siteid: String,
                        sitename: String,
                        vip_level: String,
                        paymoney: BigDecimal,
                        dt: String,
                        dn: String
                      )
