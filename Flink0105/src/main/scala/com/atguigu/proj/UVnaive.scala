package com.atguigu.proj

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.Set

object UVnaive {

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
      .map(r => ("key", r.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .process(new WindowResult)

    stream.print()

    env.execute()
  }

  // 如果访问量怎么办？这里的方法会把所有的PV数据放在窗口里面，然后去重
  class WindowResult extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      var s: Set[Long] = Set()
      for (e <- elements) {
        s += e._2
      }
      out.collect("窗口结束时间为：" + new Timestamp(context.window.getEnd) + "的窗口的UV统计值是：" + s.size)
    }
  }
}