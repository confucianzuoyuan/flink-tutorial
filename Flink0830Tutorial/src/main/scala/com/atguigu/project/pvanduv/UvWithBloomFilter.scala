package com.atguigu.project.pvanduv

import com.atguigu.project.util.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UvWithBloomFilter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

//    val stream = env.readTextFile("/home/parallels/flink-tutorial/Flink0830Tutorial/src/main/resources/UserBehavior.csv")
    // 1,1,1,pv,1
    // 2,1,1,pv,3
    // 1,1,1,pv,10000
    val stream = env.socketTextStream("localhost", 9999, '\n')

    stream
      .map(r => {
        val arr = r.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior == "pv")
      .map(r => ("uv", r.userId))
      .keyBy(_._1)
      .timeWindow(Time.minutes(60))
      .trigger(new UvTrigger)
      .process(new UvWindowProcess)
      .print()

    env.execute()
  }

  class UvTrigger extends Trigger[(String, Long), TimeWindow] {
    // 每来一条数据都会触发一次ProcessWindowFunction函数的执行
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      // 触发窗口计算并清空窗口的元素
      ctx.registerEventTimeTimer(window.getEnd)
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      if (time == window.getEnd) {
        val jedis = new Jedis("localhost", 6379)
        val key = window.getEnd.toString
        println(key, jedis.hget("UvCountHashTable", key))
        TriggerResult.FIRE_AND_PURGE
      }
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }

  class UvWindowProcess extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    lazy val jedis = new Jedis("localhost", 6379)
    lazy val bloom = new Bloom(1 << 16)

    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      // 每一个窗口 uv 的数量是存储在 redis 里面的，key是窗口的结束时间
      val storeKey = context.window.getEnd.toString
      // uv 数量初始化为 0
      var count = 0L

      // hset UvCountHashTable windowEnd count
      if (jedis.hget("UvCountHashTable", storeKey) != null) {
        count = jedis.hget("UvCountHashTable", storeKey).toLong
      }

      // 为什么 elements 里面只有一个元素？
      // 因为每次来一条数据，就会 trigger 一次窗口聚合函数的执行，并清空窗口的元素
      val userId = elements.last._2
      // offset 是位数组的下标值
      val offset = bloom.hash(userId.toString)

      // 这里为什么不用创建位数组就直接可以getbit呢？
      // 如果没有位数组的话，getbit会自动创建位数组，创建一个offset大小的位数组
      // 这里创建位数组的操作其实有点消耗性能
      // 所以优化策略是：提前针对窗口结束时间创建大的位数组
      // getbit 操作返回 false 的话，说明 userId 没来过
      val isExist = jedis.getbit(storeKey, offset)
      if (!isExist) {
        // 将 offset 对应的 bit 设置为 1
        jedis.setbit(storeKey, offset, true)
        jedis.hset("UvCountHashTable", storeKey, (count + 1).toString)
      }
//      out.collect(count.toString)
    }
  }

  // 所谓的 Bloom 其实是一个哈希函数
  class Bloom(val size: Long) extends Serializable {
    // value 值是 userId.toString
    def hash(value: String): Long = {
      var result = 0
      for (i <- 0 until value.length) {
        // seed 是 61，61 是一个素数
        result = result * 61 + value.charAt(i)
      }
      (size - 1) & result
    }
  }

}
