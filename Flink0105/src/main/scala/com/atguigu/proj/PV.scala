package com.atguigu.proj

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object PV {

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
      // 针对主流直接开窗口，计算每个小时的pv
      .timeWindowAll(Time.hours(1))
      .aggregate(new CountAgg, new WindowResult)

    stream.print()
    env.execute()
  }

  class CountAgg extends AggregateFunction[UserBehaviour, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehaviour, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  // `ProcessWindowFunction`用于keyby.timeWindow以后的流
  class WindowResult extends ProcessAllWindowFunction[Long, String, TimeWindow] {
    override def process(context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      out.collect("窗口结束时间为：" + new Timestamp(context.window.getEnd) + "的窗口的PV统计值是：" + elements.head)
    }
  }
}