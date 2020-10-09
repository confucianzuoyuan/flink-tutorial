package com.atguigu.day06

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UserBehaviorAnalysis {

  case class UserBahavior(userId: String, itemId: String, categoryId: String, behavior: String, timestamp: Long)

  case class ItemViewCount(itemId: String, count: Long, windowStart: Long, windowEnd: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("/home/zuoyuan/flink-tutorial/flink-code-java-0421/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBahavior(arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000L)
      })
      .filter(r => r.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(r => r.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg, new WindowResult)
      .keyBy(r => r.windowEnd)
      .process(new TopN(3))

    stream.print()

    env.execute()
  }

  class CountAgg extends AggregateFunction[UserBahavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: UserBahavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = ???
  }

  class WindowResult extends ProcessWindowFunction[Long, ItemViewCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key, elements.iterator.next(), context.window.getStart, context.window.getEnd))
    }
  }

  class TopN(top: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

    lazy val itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", Types.of[ItemViewCount]))

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)
      context.timerService().registerEventTimeTimer(i.windowEnd + 100L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      itemState.clear()
    }
  }
}