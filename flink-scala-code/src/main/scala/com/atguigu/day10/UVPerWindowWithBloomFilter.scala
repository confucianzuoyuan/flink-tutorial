package com.atguigu.day10

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.shaded.guava18.com.google.common.hash.{BloomFilter, Funnels}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import java.lang
import java.sql.Timestamp

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UVPerWindowWithBloomFilter {
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
      .map(r => ("key", r.userId.toLong))
      .keyBy(r => r._1)
      .timeWindow(Time.hours(1))
      .aggregate(new CountAgg, new WindowResult)
      .print()

    env.execute()
  }

  class CountAgg extends AggregateFunction[(String, Long), (Long, BloomFilter[lang.Long]), Long] {
    override def createAccumulator(): (Long, BloomFilter[lang.Long]) = (0, BloomFilter.create(Funnels.longFunnel(), 100000000, 0.01))

    override def add(in: (String, Long), acc: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = {
      if (!acc._2.mightContain(in._2)) {
        acc._2.put(in._2)
        (acc._1 + 1, acc._2)
      } else {
        acc
      }
    }

    override def getResult(acc: (Long, BloomFilter[lang.Long])): Long = acc._1

    override def merge(acc: (Long, BloomFilter[lang.Long]), acc1: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = ???

  }

  class WindowResult extends ProcessWindowFunction[Long, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      out.collect("window end: " + new Timestamp(context.window.getEnd) + " uv count: " + elements.head)
    }
  }
}