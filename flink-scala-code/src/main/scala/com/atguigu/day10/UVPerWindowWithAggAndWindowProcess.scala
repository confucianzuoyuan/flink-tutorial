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

object UVPerWindowWithAggAndWindowProcess {

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
      .aggregate(new CountAgg, new WindowResult)
      .print()

    env.execute()
  }

  class WindowResult extends ProcessWindowFunction[Long, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      out.collect("window end: " + new Timestamp(context.window.getEnd) + " uv count: " + elements.head)
    }
  }

  class CountAgg extends AggregateFunction[(String, String), (Set[String], Long), Long] {
    override def createAccumulator(): (mutable.Set[String], Long) = (mutable.Set[String](), 0L)

    override def add(in: (String, String), acc: (mutable.Set[String], Long)): (mutable.Set[String], Long) = {
      if (!acc._1.contains(in._2)) {
        acc._1 += in._2
        (acc._1, acc._2 + 1)
      } else {
        acc
      }
    }

    override def getResult(acc: (mutable.Set[String], Long)): Long = acc._2

    override def merge(acc: (mutable.Set[String], Long), acc1: (mutable.Set[String], Long)): (mutable.Set[String], Long) = ???
  }
}