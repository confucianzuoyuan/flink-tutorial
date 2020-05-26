package com.atguigu.proj

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// uv: unique visitor
// 有多少用户访问过网站；pv按照userid去重
// 滑动窗口：窗口长度1小时，滑动距离5秒钟，每小时用户数量1亿
// 大数据去重的唯一解决方案：布隆过滤器
// 布隆过滤器的组成：bit数组，哈希函数
object UV {

  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Long,
                          behavior: String,
                          timestamp: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/flink-tutorial/FlinkSZ1128/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp) // 分配升序时间戳 DataStream
      .map(r => ("key", r.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .process(new UvProcessFunc)

    stream.print()
    env.execute()
  }

  class UvProcessFunc extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      var s: Set[Long] = Set()
      for (e <- elements) {
        s += e._2
      }
      out.collect("窗口" + new Timestamp(context.window.getStart) + "---" + new Timestamp(context.window.getEnd) + "的UV数是：" + s.size)
    }
  }
}