package com.atguigu.project.topnhotitems

import java.sql.Timestamp

import com.atguigu.project.util.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.table.api.scala._

object HotItemsTable {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env
      .readTextFile("/home/parallels/flink-tutorial/Flink0830Tutorial/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp)

    val table = tEnv.fromDataStream(stream, 'timestamp.rowtime, 'itemId)
    val t = table
      .window(Tumble over 60.minutes on 'timestamp as 'w)
      .groupBy('itemId, 'w)
      .aggregate('itemId.count as 'icount)
      .select('itemId, 'icount, 'w.end as 'windowEnd)
      .toAppendStream[(Long, Long, Timestamp)]

    tEnv.createTemporaryView("topn", t, 'itemId, 'icount, 'windowEnd)

    val result = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM (
        |    SELECT *,
        |        ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY icount DESC) as row_num
        |    FROM topn)
        |WHERE row_num <= 5
        |""".stripMargin
    )
    result.toRetractStream[(Long, Long, Timestamp, Long)].print()

    env.execute()
  }
}