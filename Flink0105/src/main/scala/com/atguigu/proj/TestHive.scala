package com.atguigu.proj

import org.apache.flink.table.api._
import org.apache.flink.table.catalog.hive.HiveCatalog

object TestHive {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().inBatchMode().build()
    val tableEnv = TableEnvironment.create(settings)

    val name            = "myhive"
    val defaultDatabase = "mydb"
    val hiveConfDir     = "/Users/yuanzuo/Downloads/apache-hive-3.1.2-bin/conf" // a local path
    val version         = "3.1.2"

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    tableEnv.registerCatalog("myhive", hive)

    // set the HiveCatalog as the current catalog of the session
    tableEnv.useCatalog("myhive")
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    tableEnv.useDatabase("mydb")

    tableEnv.executeSql("insert into table t_user values ('7', 'hello')").print()
  }
}