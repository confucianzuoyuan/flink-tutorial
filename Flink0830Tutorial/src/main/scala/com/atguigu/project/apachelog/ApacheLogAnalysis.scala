package com.atguigu.project.apachelog

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

  // 相当于热门商品中的 UserBehavior 样例类
  case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

  // 相当于热门商品中的 ItemViewCount 样例类
  case class UrlViewCount(url: String, windowEnd: Long, count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      // 文件的绝对路径
      .readTextFile("/home/parallels/flink-tutorial/Flink0830Tutorial/src/main/resources/apachelog.txt")
      .map(line => {
        val linearray = line.split(" ")
        // 把时间戳ETL成毫秒
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(linearray(3)).getTime
        ApacheLogEvent(linearray(0), linearray(2), timestamp, linearray(5), linearray(6))
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1000)) {
          override def extractTimestamp(t: ApacheLogEvent): Long = {
            t.eventTime
          }
        }
      )
      .keyBy(_.url)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
      .print()

    env.execute("Traffic Analysis Job")
  }

  class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L
    override def add(apacheLogEvent: ApacheLogEvent, acc: Long): Long = acc + 1
    override def getResult(acc: Long): Long = acc
    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
  }

  class WindowResultFunction extends ProcessWindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key, context.window.getEnd, elements.iterator.next()))
    }
  }

  class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

    lazy val urlState = getRuntimeContext.getListState(
      new ListStateDescriptor[UrlViewCount]("urlState-state", Types.of[UrlViewCount])
    )

    override def processElement(input: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
      // 每条数据都保存到状态中
      urlState.add(input)
      context.timerService.registerEventTimeTimer(input.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 获取收到的所有URL访问量
      val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (urlView <- urlState.get) {
        allUrlViews += urlView
      }
      // 提前清除状态中的数据，释放空间
      urlState.clear()
      // 按照访问量从大到小排序
      val sortedUrlViews = allUrlViews.sortBy(_.count)(Ordering.Long.reverse)
        .take(topSize)
      // 将排名信息格式化成 String, 便于打印
      var result: StringBuilder = new StringBuilder
      result
        .append("====================================\n")
        .append("时间: ")
        .append(new Timestamp(timestamp - 1))
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
}
