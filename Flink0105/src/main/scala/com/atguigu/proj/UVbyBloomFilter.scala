package com.atguigu.proj

import java.sql.Timestamp
import java.lang.{Long => JLong}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels
// flink内置的布隆过滤器
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UVbyBloomFilter {

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

  class CountAgg extends AggregateFunction[(String, Long), (Long, BloomFilter[JLong]), Long] {
    override def createAccumulator(): (Long, BloomFilter[JLong]) = {
      // 第一个参数：指定了布隆过滤器要过滤的数据类型是Long
      // 第二个参数：指定了大概有多少不同的元素需要去重，这里设置了100万，也就是说假设有100万不同的用户
      // 第三个参数：误报率，这里设置了1%
      (0L, BloomFilter.create(Funnels.longFunnel(), 1000000, 0.01))
    }

    override def add(value: (String, Long), accumulator: (Long, BloomFilter[JLong])): (Long, BloomFilter[JLong]) = {
      var bloom = accumulator._2
      var uvCount = accumulator._1
      // 如果布隆过滤器没有碰到过value._2这个userid
      if (!bloom.mightContain(value._2)) {
        bloom.put(value._2) // 写入布隆过滤器
        uvCount += 1
      }
      (uvCount, bloom)
    }

    override def getResult(accumulator: (Long, BloomFilter[JLong])): Long = accumulator._1

    override def merge(a: (Long, BloomFilter[JLong]), b: (Long, BloomFilter[JLong])): (Long, BloomFilter[JLong]) = ???
  }

  class WindowResult extends ProcessWindowFunction[Long, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      out.collect("窗口结束时间为：" + new Timestamp(context.window.getEnd) + "的窗口的UV统计值是：" + elements.head)
    }
  }
}