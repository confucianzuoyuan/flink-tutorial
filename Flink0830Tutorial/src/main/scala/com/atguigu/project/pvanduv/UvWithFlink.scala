package com.atguigu.project.pvanduv

import com.atguigu.project.util.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UvWithFlink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.readTextFile("/home/parallels/flink-tutorial/Flink0830Tutorial/src/main/resources/UserBehavior.csv")

    stream
      .map(r => {
        val arr = r.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior == "pv")
      .map(r => ("uv", r.userId))
      .keyBy(_._1)
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .process(new FullWindow)
      .print()

    env.execute()
  }

  class FullWindow extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      // 使用 Set 数据结构进行蛆虫
      var s = scala.collection.mutable.Set[Long]()

      for (uv <- elements) {
        s.add(uv._2)
      }

      out.collect("窗口结束时间是： " + context.window.getEnd.toString + " 的窗口的 UV 是 " + s.size.toString)
    }
  }
}
