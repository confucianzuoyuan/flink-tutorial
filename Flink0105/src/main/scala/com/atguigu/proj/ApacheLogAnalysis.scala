package com.atguigu.proj

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object ApacheLogAnalysis {

  case class ApacheLog(ipAddr: String,
                       userId: String,
                       eventTime: Long,
                       method: String,
                       url: String)

  case class UrlViewCount(url: String,
                          windowEnd: Long,
                          count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/flink-tutorial/Flink0105/src/main/resources/apachelog.txt")
      .map(line => {
        val arr = line.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts = simpleDateFormat.parse(arr(3)).getTime
        ApacheLog(arr(0), arr(2), ts, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.seconds(1)) {
          override def extractTimestamp(element: ApacheLog): Long = element.eventTime
        }
      )
      .keyBy(_.url)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .aggregate(new CountAgg, new WindowResult)
      .keyBy(_.windowEnd)
      .process(new TopNUrl)

    stream.print()

    env.execute()
  }

  class TopNUrl extends KeyedProcessFunction[Long, UrlViewCount, String] {
    lazy val urlState = getRuntimeContext.getListState(
      new ListStateDescriptor[UrlViewCount](
        "urlState-state",
        Types.of[UrlViewCount]
      )
    )

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
      urlState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (url <- urlState.get) {
        allUrlViews += url
      }
      urlState.clear()

      val sortedUrlViews = allUrlViews
        .sortBy(-_.count)
        .take(3)

      var result: StringBuilder = new StringBuilder
      result
        .append("====================================\n")
        .append("时间: ")
        .append(new Timestamp(timestamp - 100L))
        .append("\n")

      for (i <- sortedUrlViews.indices) {
        val currentUrlView: UrlViewCount = sortedUrlViews(i)
        // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
        result
          .append("No")
          .append(i + 1)
          .append(": ")
          .append("  URL=")
          .append(currentUrlView.url)
          .append("  流量=")
          .append(currentUrlView.count)
          .append("\n")
      }

      result
        .append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }

  class CountAgg extends AggregateFunction[ApacheLog, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLog, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class WindowResult extends ProcessWindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key, context.window.getEnd, elements.head))
    }
  }
}