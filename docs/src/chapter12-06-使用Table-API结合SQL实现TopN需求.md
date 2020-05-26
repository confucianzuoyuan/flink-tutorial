## 使用Table API结合SQL实现TopN需求

```scala
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
    // 有关Blink的配置，样板代码
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    // 创建流式表的环境
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setParallelism(1)
    // 使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 过滤出pv事件，并抽取时间戳
    val stream = env
      .readTextFile("`UserBehavior.csv`的绝对路径")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong,
          arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp)

    // 从流中提取两个字段，时间戳；itemId，组成一张表
    val table = tEnv.fromDataStream(stream, 'timestamp.rowtime, 'itemId)
    val t = table
      .window(Tumble over 60.minutes on 'timestamp as 'w) // 一小时滚动窗口
      .groupBy('itemId, 'w)                               // 根据itemId和窗口进行分组
      .aggregate('itemId.count as 'icount)                // 对itemId进行计数
      .select('itemId, 'icount, 'w.end as 'windowEnd)     // 查询三个字段
      .toAppendStream[(Long, Long, Timestamp)]            // 转换成DataStream

    // 创建临时表
    tEnv.createTemporaryView("topn", t, 'itemId, 'icount, 'windowEnd)

    // topN查询，Blink支持的特性
    val result = tEnv.sqlQuery(
      """
        |SELECT *
        |FROM (
        |    SELECT *,
        |        ROW_NUMBER() OVER
        |        (PARTITION BY windowEnd ORDER BY icount DESC) as row_num
        |    FROM topn)
        |WHERE row_num <= 5
        |""".stripMargin
    )
    // 使用toRetractStream转换成DataStream，用来实时更新排行榜
    // true代表insert, false代表delete
    result.toRetractStream[(Long, Long, Timestamp, Long)].print()

    env.execute()
  }
}
```

