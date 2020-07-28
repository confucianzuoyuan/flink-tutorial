package com.atguigu.proj

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog

object TestHiveStreaming {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val stream = env
      .fromElements(
        ("15", "hhhh"),
        ("16", "eeee")
      )

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

    tableEnv.createTemporaryView("users", stream, 'id, 'name)

    tableEnv.executeSql("insert into t_user select id, name from users")
    tableEnv.executeSql("select * from t_user").print()
  }
}