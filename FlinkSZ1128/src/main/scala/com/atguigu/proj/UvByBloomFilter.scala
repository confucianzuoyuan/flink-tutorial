package com.atguigu.proj

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

// uv: unique visitor
// 有多少用户访问过网站；pv按照userid去重
// 滑动窗口：窗口长度1小时，滑动距离5秒钟，每小时用户数量1亿
// 大数据去重的唯一解决方案：布隆过滤器
// 布隆过滤器的组成：bit数组，哈希函数
object UvByBloomFilter {

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
      .trigger(new UvTrigger)
      .process(new UvProcessFunc)

    stream.print()
    env.execute()
  }

  class UvTrigger extends Trigger[(String, Long), TimeWindow] {
    // 来一条元素调用一次
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      // 来一个事件，就触发一次窗口计算，并清空窗口
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      if (ctx.getCurrentWatermark >= window.getEnd) {
        val jedis = new Jedis("localhost", 6379)
        val windowEnd = window.getEnd.toString
        println(new Timestamp(windowEnd.toLong), jedis.hget("UvCount", windowEnd))
        TriggerResult.FIRE_AND_PURGE // 为保险起见
      }
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }

  class UvProcessFunc extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    // 连接到redis
    lazy val jedis = new Jedis("localhost", 6379)

    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      // 窗口结束时间 ==> UV数
      // 窗口结束时间 ==> bit数组

      // 拿到key
      val windowEnd = context.window.getEnd.toString

      var count = 0L

      if (jedis.hget("UvCount", windowEnd) != null) {
        count = jedis.hget("UvCount", windowEnd).toLong
      }

      // 迭代器中只有一条元素，因为每来一条元素，窗口清空一次，见trigger
      val userId = elements.head._2.toString
      // 计算userId对应的bit数组的下标
      val idx = hash(userId, 1 << 29)

      // 判断userId是否访问过
      if (!jedis.getbit(windowEnd, idx)) { // 对应的bit为0的话，返回false，用户一定没访问过
        jedis.setbit(windowEnd, idx, true) // 将idx对应的bit翻转为1
        jedis.hset("UvCount", windowEnd, (count + 1).toString)
      }
    }
  }

  // 为了方便理解，只实现一个哈希函数，返回值是Long，bit数组的下标
  // value: 字符串；size：bit数组的长度
  def hash(value: String, size: Long): Long = {
    val seed = 61 // 种子
    var result = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    (size - 1) & result
  }
}