package com.atguigu.day10

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.Set

object UVPerWindowNaive {

  case class UserBehavior(userId: String, itemId: String, categoryId: String, behavior: String, timestamp: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("/home/zuoyuan/flink-tutorial/flink-scala-code/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000L)
      })
      .filter(r => r.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp)

    stream
      .map(r => ("key", r.userId))
      .keyBy(r => r._1)
      .timeWindow(Time.hours(1))
      .process(new WindowResult)
      .print()

    env.execute()
  }

  class WindowResult extends ProcessWindowFunction[(String, String), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, String)], out: Collector[String]): Unit = {
      var s : Set[String] = Set()
      for (e <- elements) {
        s += e._2
      }
      out.collect("window end: " + context.window.getEnd + " uv count: " + s.size)
    }
  }
}