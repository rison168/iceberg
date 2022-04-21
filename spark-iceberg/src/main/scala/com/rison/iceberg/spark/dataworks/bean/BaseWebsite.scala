package com.rison.iceberg.spark.dataworks.bean

import java.sql.Timestamp

case class BaseWebsite(
                        siteid: Int,
                        sitename: String,
                        siteurl: String,
                        delete: Int,
                        createtime: Timestamp,
                        creator: String,
                        dn: String
                      )
