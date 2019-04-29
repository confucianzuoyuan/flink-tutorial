package com.atguigu.project.pvanduv

import com.atguigu.project.util.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object PVwithFlink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.readTextFile("/home/parallels/flink-tutorial/Flink0830Tutorial/src/main/resources/UserBehavior.csv")

    stream
      .map(r => {
        val arr = r.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior == "pv")
      .map(_ => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg, new FullWindowAgg)
      .print()

    env.execute()
  }

  class CountAgg extends AggregateFunction[(String, Int), Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: (String, Int), accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class FullWindowAgg extends ProcessWindowFunction[Long, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      out.collect("窗口结束时间是 " + context.window.getEnd.toString + " 毫秒的窗口中 PV 是： " + elements.iterator.next)
    }
  }
}
