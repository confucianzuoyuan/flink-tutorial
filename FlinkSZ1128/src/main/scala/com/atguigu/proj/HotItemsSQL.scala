package com.atguigu.proj

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.types.Row

// 使用sql实现实时top n需求
object HotItemsSQL {

  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Long,
                          behavior: String,
                          timestamp: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 新建表环境
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/flink-tutorial/FlinkSZ1128/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp) // 分配升序时间戳 DataStream

    // 创建临时表
    tableEnv.createTemporaryView("t", stream, 'itemId, 'timestamp.rowtime as 'ts)

    // top n只有blink planner支持
    // 最内部的子查询实现了：stream.keyBy(_.itemId).timeWindow(Time.hours(1), Time.minutes(5)).aggregate(new CountAgg, new WindowResult)
    // 倒数第二层子查询：.keyBy(_.windowEnd).process(Sort)
    // 最外层：取出前三名
    val result = tableEnv
      .sqlQuery(
        """
          |SELECT *
          |FROM (
          |    SELECT *,
          |           ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY icount DESC) as row_num
          |    FROM (
          |          SELECT itemId, count(itemId) as icount,
          |                 HOP_END(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd
          |                 FROM t GROUP BY itemId, HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)
          |    )
          |)
          |WHERE row_num <= 3
          |""".stripMargin)

    result
        .toRetractStream[Row]
        .filter(_._1 == true)
        .print()

    env.execute()
  }
}