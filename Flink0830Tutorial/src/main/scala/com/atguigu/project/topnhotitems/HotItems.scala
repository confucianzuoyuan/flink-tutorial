package com.atguigu.project.topnhotitems

import java.sql.Timestamp

import com.atguigu.project.util.{ItemViewCount, UserBehavior}
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

import scala.collection.mutable.ListBuffer

object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env
      .readTextFile("/home/parallels/flink-tutorial/Flink0830Tutorial/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.itemId)
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg, new WindowResultFunction)
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))

    stream.print()

    env.execute()
  }

  class TopNHotItems(n: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
    lazy val itemState = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("items", Types.of[ItemViewCount])
    )

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      itemState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (item <- itemState.get) {
        allItems += item
      }
      itemState.clear()

      val sortedItems = allItems.sortBy(-_.count).take(n)
      val result = new StringBuilder
      result
        .append("==================================")
        .append("时间： ")
        .append(new Timestamp(timestamp - 1))
        .append("\n")
      for (i <- sortedItems.indices) {
        val currentItem = sortedItems(i)
        result
          .append("No")
          .append(i + 1)
          .append(": ")
          .append(" 商品ID = ")
          .append(currentItem.itemId)
          .append(" 浏览量 = ")
          .append(currentItem.count)
          .append("\n")
      }
      result
        .append("===================================")
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }

  class WindowResultFunction extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key, context.window.getEnd, elements.iterator.next()))
    }
  }

  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }
}
