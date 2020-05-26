package com.atguigu.proj

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.Set

object UVagg {

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
      .aggregate(new CountAgg, new WindowResult)

    stream.print()

    env.execute()
  }

  class Agg {
    var count = 0L
    var set = Set[Long]()
  }

  class CountAgg extends AggregateFunction[(String, Long), Agg, Long] {
    override def createAccumulator(): Agg = new Agg()

    override def add(value: (String, Long), accumulator: Agg): Agg = {
      if (!accumulator.set.contains(value._2)) {
        accumulator.set += value._2
        accumulator.count += 1
      }
      accumulator
    }

    override def getResult(accumulator: Agg): Long = accumulator.count

    override def merge(a: Agg, b: Agg): Agg = ???
  }

  // 如果滑动窗口是1小时，滑动距离是5秒钟，每小时用户数量是10亿呢？还管用吗？
  // 也就是说每小时的UV是10亿，去重完以后Set里面都有10亿个userid
  // 每一个userid是1kb，10亿个userid是多少？1T的数据；每隔5s就会产生1T的数据
  class WindowResult extends ProcessWindowFunction[Long, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      out.collect("窗口结束时间为：" + new Timestamp(context.window.getEnd) + "的窗口的UV统计值是：" + elements.head)
    }
  }
}