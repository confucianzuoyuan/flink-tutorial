package com.atguigu.proj

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

// 和底层api的实现有什么区别呢？
// sql没办法实现增量聚合和全窗口聚合结合使用
// 所以很占存储空间
object UserBehaviourAnalysisBySQL {

  case class UserBehaviour(userId: Long,
                           itemId: Long,
                           categoryId: Int,
                           behaviour: String,
                           timestamp: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 为了时间旅行，必须使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/flink-tutorial/Flink0105/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        // 注意，时间戳单位必须是毫秒
        UserBehaviour(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behaviour.equals("pv")) // 过滤出pv事件
      .assignAscendingTimestamps(_.timestamp) // 分配升序时间戳

    val settings = EnvironmentSettings
        .newInstance()
        .inStreamingMode()
        .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.createTemporaryView("t", stream, 'itemId, 'timestamp.rowtime as 'ts)

    // HOP_END是关键字，用来获取窗口结束时间
    // 最内层的子查询相当于stream.keyBy(_.itemId).timeWindow(滑动窗口).aggregate()
    // 倒数第二层的子查询相当于.keyBy(_.windowEnd).process(排序)
    // 最外层查询相当于.take(3)
    tEnv
        .sqlQuery(
          """
            |SELECT *
            |FROM (
            |      SELECT *,
            |             ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY itemCount DESC) as row_num
            |      FROM (
            |            SELECT itemId, count(itemId) as itemCount,
            |                   HOP_END(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd
            |            FROM t GROUP BY itemId, HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)
            |      )
            |)
            |WHERE row_num <= 3""".stripMargin)
        .toRetractStream[Row] // 每个窗口的前三名是不断变化的，所以用撤回流
        .filter(_._1 == true) // 把更新的数据过滤出来
        .print()

    env.execute()
  }
}